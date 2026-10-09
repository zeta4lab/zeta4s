"""Immutable zeta4s artifact storage."""

from __future__ import annotations

import base64
import hashlib
import io
import os
import shutil
import tarfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from zeta4s.metastore.factory import metastore_adapter_factory
from zeta4s.api.services.locks import artifact_operation_lock
from zeta4s.project.paths import normalize_relative_ref

ZETA4S_API_HOME = Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))
DEFAULT_AIRFLOW_UID = 50000
ARTIFACT_COMPLETE_MARKER = ".zeta4s-artifact-complete"


def _airflow_uid() -> int:
    raw_value = os.environ.get("AIRFLOW_UID")
    if not raw_value:
        return DEFAULT_AIRFLOW_UID
    try:
        return int(raw_value)
    except ValueError as e:
        raise ValueError(f"invalid AIRFLOW_UID: {raw_value}") from e


def artifact_dir_name(artifact_id: str) -> str:
    return artifact_id.replace(":", "-", 1)


def artifact_root(home: Path, artifact_id: str) -> Path:
    return home / "artifacts" / artifact_dir_name(artifact_id)


def decode_bundle(bundle_base64: str) -> tuple[bytes, str]:
    bundle = base64.b64decode(bundle_base64.encode("ascii"), validate=True)
    return bundle, "sha256:" + hashlib.sha256(bundle).hexdigest()


def _safe_member_path(root: Path, member_name: str) -> Path:
    target = (root / normalize_relative_ref(member_name)).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError as e:
        raise ValueError(f"unsafe artifact path: {member_name}") from e
    return target


def _safe_member_target(root: Path, member: tarfile.TarInfo) -> Path:
    if member.issym() or member.islnk():
        raise ValueError(f"artifact links are not allowed: {member.name}")
    if not (member.isfile() or member.isdir()):
        raise ValueError(f"unsupported artifact member type: {member.name}")
    return _safe_member_path(root, member.name)


def _extract_member(archive: tarfile.TarFile, member: tarfile.TarInfo, target: Path) -> None:
    if member.isdir():
        target.mkdir(parents=True, exist_ok=True)
        return
    source = archive.extractfile(member)
    if source is None:
        raise ValueError(f"artifact member has no file content: {member.name}")
    target.parent.mkdir(parents=True, exist_ok=True)
    with source, target.open("wb") as output:
        while chunk := source.read(1024 * 1024):
            output.write(chunk)


def _make_airflow_writable(path: Path) -> None:
    if os.geteuid() == 0:
        os.chown(path, _airflow_uid(), 0)
    if path.is_dir():
        path.chmod(0o775)
    else:
        path.chmod(0o664)


def _set_artifact_permissions(root: Path) -> None:
    _make_airflow_writable(root)
    for path in root.rglob("*"):
        _make_airflow_writable(path)


def ensure_artifact_runtime_permissions(artifact_id: str, *, home: Path = ZETA4S_API_HOME) -> Path:
    with artifact_operation_lock(artifact_id, home=home):
        root = artifact_root(home, artifact_id)
        _ensure_artifact_root(home, root)
        _set_artifact_permissions(root)
        return root


def delete_artifact(artifact_id: str, *, home: Path = ZETA4S_API_HOME) -> Path | None:
    with artifact_operation_lock(artifact_id, home=home):
        root = artifact_root(home, artifact_id)
        home_artifacts = (home / "artifacts").resolve()
        root_resolved = root.resolve()
        try:
            root_resolved.relative_to(home_artifacts)
        except ValueError as e:
            raise ValueError(f"unsafe artifact root: {root}") from e
        if not root_resolved.exists():
            return None
        shutil.rmtree(root_resolved)
        return root_resolved


def _ensure_artifact_root(home: Path, root: Path) -> None:
    home_artifacts = (home / "artifacts").resolve()
    root_resolved = root.resolve()
    try:
        root_resolved.relative_to(home_artifacts)
    except ValueError as e:
        raise ValueError(f"unsafe artifact root: {root}") from e
    root_resolved.mkdir(parents=True, exist_ok=True)


def _artifact_is_complete(root: Path) -> bool:
    return (root / ARTIFACT_COMPLETE_MARKER).is_file()


def extract_bundle(bundle: bytes, artifact_id: str, *, home: Path = ZETA4S_API_HOME) -> Path:
    with artifact_operation_lock(artifact_id, home=home):
        root = artifact_root(home, artifact_id)
        home_artifacts = home / "artifacts"
        home_artifacts.mkdir(parents=True, exist_ok=True)
        if root.exists() and _artifact_is_complete(root):
            return root

        tmp_root = home_artifacts / f".{artifact_dir_name(artifact_id)}.{uuid4().hex}.tmp"
        _ensure_artifact_root(home, tmp_root)
        try:
            with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as archive:
                members = archive.getmembers()
                targets = [(member, _safe_member_target(tmp_root, member)) for member in members]
                for member, target in targets:
                    _extract_member(archive, member, target)
            (tmp_root / ARTIFACT_COMPLETE_MARKER).write_text("ok\n", encoding="ascii")
            _set_artifact_permissions(tmp_root)
            if root.exists():
                shutil.rmtree(root)
            os.replace(tmp_root, root)
            return root
        finally:
            if tmp_root.exists():
                shutil.rmtree(tmp_root)


def record_artifact_metadata(
    *,
    artifact_id: str,
    project_id: str,
    dags: list[dict[str, Any]],
    runtime_connections: list[dict[str, Any]] | None = None,
    home: Path = ZETA4S_API_HOME,
) -> None:
    root = artifact_root(home, artifact_id)
    metastore_adapter_factory().artifact_repository.upsert_artifact(
        artifact_id=artifact_id,
        project_id=project_id,
        storage_uri=str(root),
        runtime_connections=runtime_connections or [],
        dags=dags,
    )
