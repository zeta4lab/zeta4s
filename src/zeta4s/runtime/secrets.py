"""Zeta4s encrypted secret store primitives."""

from __future__ import annotations

import base64
import binascii
import json
import os
import stat
from pathlib import Path
from typing import Any, NamedTuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from zeta4s.metastore.factory import metastore_adapter_factory

SECRET_MASTER_KEY_FILE_ENV = "ZETA4S_SECRET_MASTER_KEY_FILE"
# runtime state volume 밖이다. 그 volume 은 Prefect worker 와 공유되므로 경로가 곧
# 노출 경계가 된다. Kubernetes 에서는 Secret mount 지점이 여기다.
DEFAULT_SECRET_MASTER_KEY_FILE = Path("/var/lib/zeta4s-keyring/master.json")
SECRET_ALGORITHM = "AESGCM256"
KEYRING_SCHEMA_VERSION = 1


class MasterKeyring(NamedTuple):
    """복호화 가능한 master key 세대 모음.

    활성 세대는 정확히 하나이며 새 암호화는 그것만 쓴다. 나머지 세대는 회전이
    끝나기 전의 ciphertext 를 읽기 위해 남는다.
    """

    active_key_id: str
    keys: dict[str, bytes]

    def active_key(self) -> bytes:
        return self.keys[self.active_key_id]

    def key_for(self, key_id: str) -> bytes:
        try:
            return self.keys[key_id]
        except KeyError as e:
            raise ValueError(f"secret master keyring has no generation: {key_id}") from e


def generate_master_key() -> str:
    return base64.b64encode(os.urandom(32)).decode("ascii")


def generate_key_id() -> str:
    return "k" + base64.b32encode(os.urandom(10)).decode("ascii").rstrip("=").lower()


def build_keyring_document(entries: list[tuple[str, str]], active_key_id: str) -> str:
    """keyring 파일 본문을 만든다. entries 는 (key_id, base64 key) 목록이다."""
    if not entries:
        raise ValueError("secret master keyring requires at least one generation")
    key_ids = [key_id for key_id, _ in entries]
    if len(set(key_ids)) != len(key_ids):
        raise ValueError("secret master keyring generation ids must be unique")
    if active_key_id not in key_ids:
        raise ValueError(f"active generation is not in the keyring: {active_key_id}")
    return (
        json.dumps(
            {
                "schema_version": KEYRING_SCHEMA_VERSION,
                "active_key_id": active_key_id,
                "keys": [{"key_id": key_id, "key": key} for key_id, key in entries],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def init_master_key_file(path: Path, *, force: bool = False) -> Path:
    """운영자 절차용 keyring 생성. `zeta4s-api` 는 이 함수를 쓰지 않는다.

    `write_text` 로 만들면 umask(보통 022)가 적용돼 0644 로 생성된 뒤 chmod 되므로
    그 사이 창에서 key 가 world-readable 이다. 처음부터 0400 으로 연다.
    """
    target = path.expanduser()
    if target.exists() and not force:
        raise FileExistsError(f"secret master keyring file already exists: {target}")
    # 디렉터리도 umask 를 타므로 만든 뒤 소유자 전용으로 좁힌다.
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        target.parent.chmod(0o700)
    except OSError:
        pass
    key_id = generate_key_id()
    document = build_keyring_document([(key_id, generate_master_key())], key_id)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if not force:
        flags |= os.O_EXCL
    fd = os.open(target, flags, 0o400)
    try:
        os.write(fd, document.encode("ascii"))
    finally:
        os.close(fd)
    try:
        target.chmod(0o400)  # force 로 덮어쓴 경우 기존 mode 를 되돌린다
    except OSError:
        pass
    return target


def _decode_master_key(raw: str) -> bytes:
    try:
        key = base64.b64decode(raw.strip().encode("ascii"), validate=True)
    except Exception as e:
        raise ValueError("secret master key must be base64 encoded") from e
    if len(key) != 32:
        raise ValueError("secret master key must decode to 32 bytes")
    return key


def _parse_keyring(raw: str) -> MasterKeyring:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError("secret master keyring must be a JSON document") from e
    if not isinstance(data, dict):
        raise ValueError("secret master keyring must be a mapping")
    if int(data.get("schema_version") or 0) != KEYRING_SCHEMA_VERSION:
        raise ValueError("unsupported secret master keyring schema version")
    active_key_id = str(data.get("active_key_id") or "").strip()
    if not active_key_id:
        raise ValueError("secret master keyring requires active_key_id")
    entries = data.get("keys")
    if not isinstance(entries, list) or not entries:
        raise ValueError("secret master keyring requires at least one generation")
    keys: dict[str, bytes] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("secret master keyring generation must be a mapping")
        key_id = str(entry.get("key_id") or "").strip()
        if not key_id:
            raise ValueError("secret master keyring generation requires key_id")
        if key_id in keys:
            raise ValueError(f"duplicate secret master keyring generation: {key_id}")
        keys[key_id] = _decode_master_key(str(entry.get("key") or ""))
    if active_key_id not in keys:
        raise ValueError("secret master keyring active generation is missing")
    return MasterKeyring(active_key_id=active_key_id, keys=keys)


def _process_group_ids() -> set[int]:
    ids = {os.getgid(), os.getegid()}
    try:
        ids.update(os.getgroups())
    except OSError:  # pragma: no cover - 플랫폼에 따라 없을 수 있다
        pass
    return ids


def master_key_file_path(path: Path | None = None) -> Path:
    if path is not None:
        return path.expanduser()
    configured = os.environ.get(SECRET_MASTER_KEY_FILE_ENV)
    return Path(configured).expanduser() if configured else DEFAULT_SECRET_MASTER_KEY_FILE


def check_master_key_file(path: Path | None = None) -> dict[str, Any]:
    """keyring 파일의 안전성과 형식을 함께 본다.

    검사는 symlink 를 따라간 최종 대상 기준이다. Kubernetes 는 Secret 을
    `master.json -> ..data/master.json` symlink 로 mount 하므로 symlink 자체를
    거절하면 그 배포 형태에서 keyring 을 읽을 수 없다.

    권한 기준도 두 배포 형태를 함께 만족해야 한다.

    - compose: 파일을 실행 UID 소유 0400 으로 둔다.
    - Kubernetes: kubelet 이 Secret 을 root 소유로 만들고 fsGroup 으로 group 접근을
      준다. group 을 통째로 막으면 Pod 가 자기 Secret 을 못 읽는다.

    그래서 group **read** 만 허용하고 group write/exec 과 world 접근은 막는다.
    read 만으로도 키가 새면 끝이지만, write 는 공격자가 자기 keyring 을 주입해
    이후 secret 을 자기 키로 암호화하게 만들 수 있다.

    group read 를 허용하는 이상 **그 group 이 누구인지** 확인해야 의미가 있다.
    group bit 가 켜져 있으면 파일의 gid 가 이 process 의 group 집합에 있어야 한다.
    소유자는 실행 UID 이거나 root 여야 한다 — root 는 어차피 무엇이든 할 수 있고,
    제3의 사용자가 소유한 파일은 내용을 바꿀 수 있다.
    """
    target = master_key_file_path(path)
    result: dict[str, Any] = {
        "path": str(target),
        "exists": target.exists(),
        "readable": False,
        "valid": False,
        "mode": None,
        "secure_permissions": False,
        "regular_file": False,
        "owner_trusted": False,
        "group_trusted": False,
        "active_key_id": None,
        "generation_count": 0,
    }
    if not target.exists():
        return result
    try:
        stat_result = target.stat()  # symlink 를 따라간 최종 대상
        result["regular_file"] = stat.S_ISREG(stat_result.st_mode)
        mode = stat_result.st_mode & 0o777
        result["mode"] = oct(mode)
        # group write(0o020), group exec(0o010), world 전체(0o007) 를 막는다.
        result["secure_permissions"] = (mode & 0o037) == 0
        result["owner_trusted"] = stat_result.st_uid in (os.getuid(), 0)
        if mode & 0o040:
            result["group_trusted"] = stat_result.st_gid in _process_group_ids()
        else:
            result["group_trusted"] = True
        if not result["regular_file"]:
            return result
        raw = target.read_text(encoding="ascii")
        result["readable"] = True
        keyring = _parse_keyring(raw)
        result["active_key_id"] = keyring.active_key_id
        result["generation_count"] = len(keyring.keys)
        result["valid"] = bool(result["secure_permissions"] and result["owner_trusted"] and result["group_trusted"])
    except (OSError, UnicodeError, ValueError):
        result["valid"] = False
    return result


def load_master_keyring(path: Path | None = None) -> MasterKeyring:
    target = master_key_file_path(path)
    status = check_master_key_file(target)
    if not status["valid"]:
        raise ValueError(f"secret master keyring file is not valid: {target}")
    return _parse_keyring(target.read_text(encoding="ascii"))


def _secret_aad(secret_key: str, version: int, key_id: str) -> bytes:
    return f"{secret_key}:{version}:{key_id}".encode("utf-8")


def encrypt_secret_value(secret_key: str, version: int, plaintext: str, *, keyring: MasterKeyring) -> str:
    nonce = os.urandom(12)
    ciphertext = AESGCM(keyring.active_key()).encrypt(
        nonce,
        plaintext.encode("utf-8"),
        _secret_aad(secret_key, version, keyring.active_key_id),
    )
    return json.dumps(
        {
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        },
        sort_keys=True,
    )


def decrypt_secret_value(
    secret_key: str,
    version: int,
    envelope: str,
    *,
    keyring: MasterKeyring,
    key_id: str,
) -> str:
    data = json.loads(envelope)
    if not isinstance(data, dict):
        raise ValueError("secret ciphertext envelope must be a mapping")
    try:
        nonce_raw = data["nonce"]
        ciphertext_raw = data["ciphertext"]
    except KeyError as e:
        raise ValueError("secret ciphertext envelope is missing required fields") from e
    try:
        nonce = base64.b64decode(str(nonce_raw).encode("ascii"), validate=True)
        ciphertext = base64.b64decode(str(ciphertext_raw).encode("ascii"), validate=True)
    except (binascii.Error, UnicodeError) as e:
        raise ValueError("secret ciphertext envelope contains invalid base64") from e
    plaintext = AESGCM(keyring.key_for(key_id)).decrypt(nonce, ciphertext, _secret_aad(secret_key, version, key_id))
    return plaintext.decode("utf-8")


class EncryptedSecretStore:
    def __init__(self, *, master_key_file: Path | None = None):
        self.master_key_file = master_key_file

    def _repository(self):
        return metastore_adapter_factory().secret_repository

    def _keyring(self) -> MasterKeyring:
        return load_master_keyring(self.master_key_file)

    def set_secret(self, secret_key: str, value: str) -> dict[str, Any]:
        key = _validate_secret_key(secret_key)
        repository = self._repository()
        # 같은 secret 에 대한 동시 쓰기는 next_version 계산을 겹치게 한다.
        # repository 가 제공하는 직렬화 구간 안에서 읽고 쓴다.
        with repository.secret_write_lock(key):
            # keyring 은 반드시 lock 안에서 읽는다. 밖에서 읽으면 운영자가 세대를
            # 교체하는 동안 지연된 호출이 옛 세대로 기록할 수 있고, 회전이 이미
            # "옛 세대 없음" 을 보고한 뒤라면 그 secret 은 복호화 불가가 된다.
            keyring = self._keyring()
            current_versions = [
                item
                for item in self._collapsed_metadata(repository.list_secret_metadata())
                if item.get("secret_key") == key
            ]
            active_versions = [int(item["version"]) for item in current_versions if item.get("status") == "active"]
            next_version = max([int(item["version"]) for item in current_versions] or [0]) + 1
            ciphertext = encrypt_secret_value(key, next_version, value, keyring=keyring)
            repository.put_secret_version(
                secret_key=key,
                version=next_version,
                ciphertext=ciphertext,
                algorithm=SECRET_ALGORITHM,
                key_id=keyring.active_key_id,
                status="active",
            )
            for version in active_versions:
                repository.put_secret_version(
                    secret_key=key,
                    version=version,
                    ciphertext="",
                    algorithm=SECRET_ALGORITHM,
                    # 빈 ciphertext 는 어떤 세대로도 만들어지지 않았다. 세대를
                    # 기록하면 "이 세대가 아직 필요하다" 는 거짓 신호가 된다.
                    key_id=None,
                    status="rotated",
                )
        return {"secret_key": key, "version": next_version, "status": "active"}

    def resolve_secret(self, secret_key: str) -> str:
        key = _validate_secret_key(secret_key)
        envelope = self._repository().get_active_secret(key)
        if envelope is None:
            raise KeyError(f"secret is not active: {key}")
        if envelope.get("algorithm") != SECRET_ALGORITHM:
            raise ValueError(f"unsupported secret algorithm: {envelope.get('algorithm')}")
        key_id = envelope.get("key_id")
        if not key_id:
            # 세대를 모르는 ciphertext 는 어떤 키로 만들었는지 알 수 없다.
            # 추측해서 읽지 않는다.
            raise ValueError(f"secret ciphertext has no master key generation: {key}")
        return decrypt_secret_value(
            key,
            int(envelope["version"]),
            str(envelope["ciphertext"]),
            keyring=self._keyring(),
            key_id=str(key_id),
        )

    def rotate_to_active_generation(self) -> dict[str, Any]:
        """활성 세대가 아닌 active ciphertext 를 활성 세대로 다시 암호화한다.

        keyring 파일은 운영자가 갱신한다. 이 연산은 파일을 쓰지 않고 저장된
        ciphertext 만 옮긴다. 각 row 는 compare-and-set 으로 갱신하므로 동시에
        들어온 `set_secret` 의 새 version 을 덮지 않는다.
        """
        repository = self._repository()
        keyring = self._keyring()
        active_key_id = keyring.active_key_id

        reencrypted = 0
        skipped = 0
        # 세대를 모르는 row 는 어떤 키로 만들었는지 알 수 없어 재암호화 대상이 아니다.
        # 이런 row 하나 때문에 나머지 secret 의 회전을 막지 않는다 — 회전을 멈추면
        # 운영자가 옛 세대를 영영 정리하지 못한다. 대신 수를 보고해 드러낸다.
        without_generation: list[str] = []
        remaining: dict[str, int] = {}
        for item in self._collapsed_metadata(repository.list_secret_metadata()):
            if item.get("status") != "active":
                continue
            key_id = item.get("key_id")
            if not key_id:
                without_generation.append(str(item.get("secret_key")))
                continue
            if str(key_id) == active_key_id:
                continue
            secret_key = str(item["secret_key"])
            version = int(item["version"])
            # 같은 secret 의 write lock 을 잡는다. 그래야 진행 중인 set_secret 이
            # 옛 세대로 기록하는 것과 이 재암호화가 겹치지 않는다.
            with repository.secret_write_lock(secret_key):
                envelope = repository.get_active_secret(secret_key)
                if envelope is None or int(envelope["version"]) != version or not envelope.get("key_id"):
                    skipped += 1
                    continue
                if str(envelope["key_id"]) == active_key_id:
                    continue
                plaintext = decrypt_secret_value(
                    secret_key,
                    version,
                    str(envelope["ciphertext"]),
                    keyring=keyring,
                    key_id=str(envelope["key_id"]),
                )
                replaced = repository.reencrypt_secret_version(
                    secret_key=secret_key,
                    version=version,
                    expected_key_id=str(envelope["key_id"]),
                    ciphertext=encrypt_secret_value(secret_key, version, plaintext, keyring=keyring),
                    key_id=active_key_id,
                )
            if replaced:
                reencrypted += 1
            else:
                skipped += 1

        for item in self._collapsed_metadata(repository.list_secret_metadata()):
            if item.get("status") != "active":
                continue
            key_id = str(item.get("key_id") or "")
            if key_id and key_id != active_key_id:
                remaining[key_id] = remaining.get(key_id, 0) + 1

        return {
            "active_key_id": active_key_id,
            "reencrypted": reencrypted,
            "skipped": skipped,
            "remaining_by_key_id": remaining,
            "without_generation": sorted(without_generation),
        }

    def list_metadata(self) -> list[dict[str, Any]]:
        return self._collapsed_metadata(self._repository().list_secret_metadata())

    def check_secret(self, secret_key: str) -> dict[str, Any]:
        key = _validate_secret_key(secret_key)
        try:
            self.resolve_secret(key)
            return {"secret_key": key, "active": True, "decryptable": True}
        except KeyError:
            return {"secret_key": key, "active": False, "decryptable": False}
        except (InvalidTag, ValueError):
            return {"secret_key": key, "active": True, "decryptable": False}

    @staticmethod
    def _collapsed_metadata(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        latest: dict[tuple[str, int], tuple[int, dict[str, Any]]] = {}
        for index, row in enumerate(rows):
            secret_key = str(row.get("secret_key") or "")
            version_raw = row.get("version")
            if not secret_key or version_raw is None:
                continue
            version = int(version_raw)
            candidate = dict(row)
            candidate["secret_key"] = secret_key
            candidate["version"] = version
            key = (secret_key, version)
            rank = _metadata_revision_rank(candidate, index)
            current = latest.get(key)
            if current is None or rank >= current[0]:
                latest[key] = (rank, candidate)
        return [
            item
            for _, item in sorted(
                latest.values(),
                key=lambda value: (str(value[1].get("secret_key")), -int(value[1].get("version") or 0)),
            )
        ]


def _validate_secret_key(secret_key: str) -> str:
    key = str(secret_key).strip()
    if not key:
        raise ValueError("secret key is required")
    if key.startswith("/") or ".." in Path(key).parts:
        raise ValueError(f"invalid secret key: {secret_key}")
    return key


def _metadata_revision_rank(row: dict[str, Any], fallback: int) -> int:
    revision = row.get("revision")
    if revision is not None:
        try:
            return int(revision)
        except (TypeError, ValueError):
            pass
    return fallback
