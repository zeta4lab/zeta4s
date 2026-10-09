from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from zeta4s.api.app import (
    DEPLOY_PROGRESS_STEPS,
    REDEPLOY_PROGRESS_STEPS,
    UNDEPLOY_PROGRESS_STEPS,
    DeployRequest,
    ProfileCheckRequest,
    SecretCheckRequest,
    SecretSetRequest,
    create_app,
    _runtime_connection_policy_by_conn_id,
)
from zeta4s.cli.main import cli, main
from zeta4s.runtime.secrets import (
    EncryptedSecretStore,
    build_keyring_document,
    check_master_key_file,
    generate_key_id,
    generate_master_key,
    init_master_key_file,
    load_master_keyring,
)


def _b64(raw: bytes) -> str:
    import base64

    return base64.b64encode(raw).decode("ascii")


def _write_keyring(path: Path, entries: list[tuple[str, str]], active_key_id: str) -> None:
    """운영자가 keyring 파일을 갱신하는 절차를 흉내낸다. API 는 이 파일을 쓰지 않는다."""
    path.chmod(0o600)
    path.write_text(build_keyring_document(entries, active_key_id), encoding="ascii")
    path.chmod(0o400)


class SecretStoreContractTest(unittest.TestCase):
    def test_master_key_file_init_and_check(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master.json"

            init_master_key_file(path)
            status = check_master_key_file(path)

            self.assertTrue(status["exists"])
            self.assertTrue(status["readable"])
            self.assertTrue(status["valid"])
            self.assertEqual(status["mode"], "0o400")

    def test_master_key_file_rejects_world_readable_mode(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master.json"
            init_master_key_file(path)
            path.chmod(0o444)

            status = check_master_key_file(path)

            self.assertFalse(status["secure_permissions"])
            self.assertFalse(status["valid"])

    def test_master_key_file_allows_group_readable_secret_mount_shape(self) -> None:
        """Kubernetes 는 Secret 을 group-readable 로 mount 한다.

        group read 를 막으면 Pod 가 자기 Secret 을 못 읽는다.
        """
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master.json"
            init_master_key_file(path)
            path.chmod(0o440)

            status = check_master_key_file(path)

            self.assertTrue(status["secure_permissions"])
            self.assertTrue(status["group_trusted"])
            self.assertTrue(status["valid"])

    def test_master_key_file_rejects_group_writable_mode(self) -> None:
        """group read 는 허용해도 write 는 안 된다.

        같은 group 의 프로세스가 keyring 을 자기 것으로 바꾸면 이후 secret 이
        공격자 키로 암호화된다.
        """
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master.json"
            init_master_key_file(path)

            for mode in (0o660, 0o670, 0o770):
                path.chmod(mode)
                status = check_master_key_file(path)
                self.assertFalse(status["secure_permissions"], oct(mode))
                self.assertFalse(status["valid"], oct(mode))

    def test_master_key_file_follows_symlink_like_a_secret_mount(self) -> None:
        """Kubernetes Secret 은 master.json -> ..data/master.json symlink 다.

        symlink 자체를 거절하면 그 배포 형태에서 keyring 을 읽을 수 없다.
        검사는 최종 대상 기준으로 한다.
        """
        with TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "..data"
            data_dir.mkdir()
            real = data_dir / "master.json"
            init_master_key_file(real)
            link = Path(tmp) / "master.json"
            link.symlink_to(real)

            status = check_master_key_file(link)

            self.assertTrue(status["regular_file"])
            self.assertTrue(status["valid"])

    def test_master_key_file_reports_generation_summary(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "master.json"
            init_master_key_file(path)
            keyring = load_master_keyring(path)

            status = check_master_key_file(path)

            self.assertTrue(status["owner_trusted"])
            self.assertTrue(status["regular_file"])
            self.assertEqual(status["active_key_id"], keyring.active_key_id)
            self.assertEqual(status["generation_count"], 1)

    def test_secret_store_writes_ciphertext_and_resolves_plaintext(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                metadata = store.set_secret("prod.analytics_clickhouse.password", "plain-password")
                resolved = store.resolve_secret("prod.analytics_clickhouse.password")

            self.assertEqual(
                metadata,
                {
                    "secret_key": "prod.analytics_clickhouse.password",
                    "version": 1,
                    "status": "active",
                },
            )
            self.assertEqual(resolved, "plain-password")
            self.assertEqual(repository.rows[0]["secret_key"], "prod.analytics_clickhouse.password")
            self.assertEqual(repository.rows[0]["algorithm"], "AESGCM256")
            self.assertNotIn("plain-password", repository.rows[0]["ciphertext"])

    def test_secret_store_rotation_marks_previous_active_without_exposing_plaintext(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "old-password")
                store.set_secret("prod.analytics_clickhouse.password", "new-password")
                resolved = store.resolve_secret("prod.analytics_clickhouse.password")

            self.assertEqual(resolved, "new-password")
            # (secret_key, version) 은 유일하다. v1 은 새로 쌓이지 않고 rotated 로 갱신된다.
            self.assertEqual(
                sorted((row["version"], row["status"]) for row in repository.rows),
                [
                    (1, "rotated"),
                    (2, "active"),
                ],
            )
            serialized_rows = repr(repository.rows)
            self.assertNotIn("old-password", serialized_rows)
            self.assertNotIn("new-password", serialized_rows)

    def test_secret_store_list_collapses_stale_version_rows(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "old-password")
                store.set_secret("prod.analytics_clickhouse.password", "new-password")
                metadata = store.list_metadata()
                store.set_secret("prod.analytics_clickhouse.password", "next-password")

            self.assertEqual(
                [(row["version"], row["status"]) for row in metadata],
                [
                    (2, "active"),
                    (1, "rotated"),
                ],
            )
            self.assertEqual(
                sorted((row["version"], row["status"]) for row in repository.rows),
                [
                    (1, "rotated"),
                    (2, "rotated"),
                    (3, "active"),
                ],
            )

    def test_secret_store_check_reports_key_mismatch_as_undecryptable(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            wrong_key_path = Path(tmp) / "wrong-master.json"
            init_master_key_file(key_path)
            init_master_key_file(wrong_key_path)
            store = EncryptedSecretStore(master_key_file=key_path)
            wrong_key_store = EncryptedSecretStore(master_key_file=wrong_key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "plain-password")
                status = wrong_key_store.check_secret("prod.analytics_clickhouse.password")

            self.assertEqual(
                status,
                {
                    "secret_key": "prod.analytics_clickhouse.password",
                    "active": True,
                    "decryptable": False,
                },
            )

    def test_secret_ciphertext_records_active_master_key_generation(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            keyring = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "plain-password")

            self.assertEqual(repository.rows[0]["key_id"], keyring.active_key_id)

    def test_secret_without_key_generation_is_rejected(self) -> None:
        # 세대를 모르는 ciphertext 는 어떤 키로 만들었는지 알 수 없다. 추측해서 읽지 않는다.
        repository = _FakeSecretRepository()
        repository.rows.append(
            {
                "secret_key": "prod.legacy.password",
                "version": 1,
                "ciphertext": "{}",
                "algorithm": "AESGCM256",
                "key_id": None,
                "status": "active",
            }
        )
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                with self.assertRaises(ValueError):
                    store.resolve_secret("prod.legacy.password")

    def test_rotation_reencrypts_to_active_generation_and_keeps_plaintext(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            first = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "plain-password")

                # 운영자가 새 세대를 추가하고 활성으로 바꾼다. API 는 파일을 쓰지 않는다.
                second_key_id = generate_key_id()
                _write_keyring(
                    key_path,
                    [
                        (first.active_key_id, _b64(first.active_key())),
                        (second_key_id, generate_master_key()),
                    ],
                    second_key_id,
                )

                # 회전 전에도 옛 세대 ciphertext 를 읽을 수 있다.
                self.assertEqual(store.resolve_secret("prod.analytics_clickhouse.password"), "plain-password")

                report = store.rotate_to_active_generation()
                resolved = store.resolve_secret("prod.analytics_clickhouse.password")

            self.assertEqual(report["active_key_id"], second_key_id)
            self.assertEqual(report["reencrypted"], 1)
            self.assertEqual(report["remaining_by_key_id"], {})
            self.assertEqual(resolved, "plain-password")
            self.assertEqual(repository.rows[0]["key_id"], second_key_id)
            self.assertNotIn("plain-password", repr(repository.rows))

    def test_rotation_reports_rows_without_generation_instead_of_aborting(self) -> None:
        """세대를 모르는 row 하나가 나머지 secret 의 회전을 막으면 안 된다.

        그런 row 는 resolve 도 거절되는 죽은 데이터다. 회전이 멈추면 운영자가 옛
        세대를 영영 정리하지 못한다.
        """
        repository = _FakeSecretRepository()
        repository.rows.append(
            {
                "secret_key": "prod.legacy.password",
                "version": 1,
                "ciphertext": "{}",
                "algorithm": "AESGCM256",
                "key_id": None,
                "status": "active",
            }
        )
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            first = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.live.password", "live-value")

                second_key_id = generate_key_id()
                _write_keyring(
                    key_path,
                    [
                        (first.active_key_id, _b64(first.active_key())),
                        (second_key_id, generate_master_key()),
                    ],
                    second_key_id,
                )

                report = store.rotate_to_active_generation()
                resolved = store.resolve_secret("prod.live.password")

            self.assertEqual(report["reencrypted"], 1)
            self.assertEqual(report["without_generation"], ["prod.legacy.password"])
            self.assertEqual(report["remaining_by_key_id"], {})
            self.assertEqual(resolved, "live-value")

    def test_rotation_skips_rows_a_concurrent_write_moved_on(self) -> None:
        # 재암호화 중 사용자가 새 값을 쓰면 CAS 가 실패해야 한다. 옛 평문으로 되돌리면 안 된다.
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            first = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics_clickhouse.password", "old-password")

                second_key_id = generate_key_id()
                _write_keyring(
                    key_path,
                    [
                        (first.active_key_id, _b64(first.active_key())),
                        (second_key_id, generate_master_key()),
                    ],
                    second_key_id,
                )

                # 새 세대로 쓰인 새 version 이 이미 있는 상태를 만든다.
                store.set_secret("prod.analytics_clickhouse.password", "new-password")

                report = store.rotate_to_active_generation()
                resolved = store.resolve_secret("prod.analytics_clickhouse.password")

            self.assertEqual(report["reencrypted"], 0)
            self.assertEqual(report["remaining_by_key_id"], {})
            self.assertEqual(resolved, "new-password")

    def test_set_secret_takes_the_write_lock(self) -> None:
        """직렬화가 빠지면 동시 호출이 같은 next_version 을 계산해 서로를 덮는다."""
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics.password", "value")

            self.assertEqual(repository.lock_calls, ["prod.analytics.password"])

    def test_rotation_cas_failure_leaves_the_newer_write_intact(self) -> None:
        """재암호화 직전에 값이 바뀌면 CAS 가 실패해야 한다.

        실패하지 않으면 옛 평문이 새 값을 덮어쓴다.
        """
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            first = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.analytics.password", "old-password")

                second_key_id = generate_key_id()
                _write_keyring(
                    key_path,
                    [
                        (first.active_key_id, _b64(first.active_key())),
                        (second_key_id, generate_master_key()),
                    ],
                    second_key_id,
                )

                # CAS 직전에 다른 writer 가 같은 row 를 옮긴 상황을 만든다.
                original = repository.reencrypt_secret_version

                def move_row_then_cas(**kwargs):
                    for row in repository.rows:
                        if row["version"] == kwargs["version"] and row["status"] == "active":
                            row["key_id"] = "someone-else"
                    return original(**kwargs)

                repository.reencrypt_secret_version = move_row_then_cas
                report = store.rotate_to_active_generation()

            self.assertEqual(report["reencrypted"], 0)
            self.assertEqual(report["skipped"], 1)

    def test_rotation_resumes_after_interruption(self) -> None:
        repository = _FakeSecretRepository()
        with TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "master.json"
            init_master_key_file(key_path)
            first = load_master_keyring(key_path)
            store = EncryptedSecretStore(master_key_file=key_path)

            with patch("zeta4s.runtime.secrets.metastore_adapter_factory", return_value=_FakeAdapter(repository)):
                store.set_secret("prod.a.password", "a-value")
                store.set_secret("prod.b.password", "b-value")

                second_key_id = generate_key_id()
                _write_keyring(
                    key_path,
                    [
                        (first.active_key_id, _b64(first.active_key())),
                        (second_key_id, generate_master_key()),
                    ],
                    second_key_id,
                )

                # 첫 시도가 하나만 옮기고 끊긴 상황을 만든다.
                original = repository.reencrypt_secret_version
                calls = {"n": 0}

                def failing_once(**kwargs):
                    calls["n"] += 1
                    if calls["n"] == 2:
                        raise RuntimeError("interrupted")
                    return original(**kwargs)

                repository.reencrypt_secret_version = failing_once
                with self.assertRaises(RuntimeError):
                    store.rotate_to_active_generation()

                repository.reencrypt_secret_version = original
                report = store.rotate_to_active_generation()

                self.assertEqual(store.resolve_secret("prod.a.password"), "a-value")
                self.assertEqual(store.resolve_secret("prod.b.password"), "b-value")

            self.assertEqual(report["reencrypted"], 1)
            self.assertEqual(report["remaining_by_key_id"], {})

    def test_api_secret_set_returns_metadata_without_secret_value(self) -> None:
        store = _FakeEncryptedSecretStore()
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.api.app.EncryptedSecretStore", return_value=store),
            patch(
                "zeta4s.api.app.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/secrets", "POST")
            response = endpoint(
                SecretSetRequest(secret_key="prod.analytics_clickhouse.password", value="plain-password"),
                authorization=None,
            )

        self.assertEqual(
            response,
            {
                "secret_key": "prod.analytics_clickhouse.password",
                "version": 1,
                "status": "active",
            },
        )
        self.assertEqual(store.set_calls, [("prod.analytics_clickhouse.password", "plain-password")])
        self.assertNotIn("plain-password", repr(response))

    def test_api_profile_check_reports_secret_and_connection_status(self) -> None:
        store = _FakeEncryptedSecretStore()
        checks = [
            _FakeProfileConnectionCheck(
                conn_id="analytics_clickhouse",
                kind="clickhouse",
                ok=True,
                detail="SELECT 1",
            )
        ]
        profile = {
            "connections": {
                "analytics_clickhouse": {
                    "type": "clickhouse",
                    "host": "clickhouse",
                    "port": 8123,
                    "username": "metastore",
                    "password_ref": "prod.analytics_clickhouse.password",
                    "database": "analytics",
                }
            }
        }

        with (
            patch("zeta4s.api.app.EncryptedSecretStore", return_value=store),
            patch(
                "zeta4s.api.app._metastore_not_ready_report",
                return_value=None,
            ),
            patch("zeta4s.api.app._emit_operation_report", side_effect=lambda report: report),
            patch(
                "zeta4s.airflow.runtime_check.check_profile_api",
                return_value=checks,
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/profiles/check", "POST")
            response = endpoint(ProfileCheckRequest(profile=profile), authorization=None)

        self.assertEqual(response["status"], "passed")
        self.assertEqual(response["command"], "z4s profile check")
        self.assertNotIn("project", response)
        self.assertNotIn("project_id", response)
        self.assertNotIn("profile_id", response["summary"])
        self.assertEqual(response["summary"]["connection_count"], 1)
        self.assertEqual(response["summary"]["secret_ref_count"], 1)
        self.assertEqual(store.check_calls, ["prod.analytics_clickhouse.password"])
        connection_step = next(step for step in response["steps"] if step["name"] == "connections_check")
        self.assertEqual(connection_step["summary"]["checks"][0]["detail"], "SELECT 1")

    def test_runtime_connection_policy_uses_metastore_artifact_metadata(self) -> None:
        adapter = _FakeMetastoreAdapter(
            deployment_repository=_FakeDeploymentRepository(
                [
                    {
                        "project_id": "retail",
                        "artifact_id": "sha256:retail",
                        "scheduler_backend": "airflow",
                        "dags": [],
                    },
                ]
            ),
            artifact_repository=_FakeArtifactRepository(
                {
                    "sha256:retail": {
                        "artifact_id": "sha256:retail",
                        "project_id": "retail",
                        "storage_uri": "/var/lib/zeta4s/artifacts/sha256-retail",
                        "runtime_connections": [
                            {
                                "conn_id": "analytics_clickhouse",
                                "type": "clickhouse",
                                "password_ref": "prod.analytics_clickhouse.password",
                            }
                        ],
                        "dags": [],
                        "created_at": "2026-07-09T00:00:00+00:00",
                    }
                }
            ),
        )

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            policy = _runtime_connection_policy_by_conn_id("analytics_clickhouse")

        self.assertEqual(policy["conn_id"], "analytics_clickhouse")
        self.assertEqual(policy["password_ref"], "prod.analytics_clickhouse.password")

    def test_api_secret_operations_require_explicit_metastore_bootstrap(self) -> None:
        adapter = _FakeMissingMetastoreAdapter()
        store = _FakeEncryptedSecretStore()
        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.app.EncryptedSecretStore",
                return_value=store,
            ),
        ):
            app = create_app()
            set_response = _route_endpoint(app, "/api/v1/secrets", "POST")(
                SecretSetRequest(secret_key="prod.analytics_clickhouse.password", value="plain-password"),
                authorization=None,
            )
            list_response = _route_endpoint(app, "/api/v1/secrets", "GET")(authorization=None)
            check_response = _route_endpoint(app, "/api/v1/secrets/check", "POST")(
                SecretCheckRequest(secret_key="prod.analytics_clickhouse.password"),
                authorization=None,
            )

        self.assertFalse(adapter.bootstrapped)
        self.assertEqual(store.set_calls, [])
        for command, response in [
            ("z4s api secret set", set_response),
            ("z4s api secret list", list_response),
            ("z4s api secret check", check_response),
        ]:
            self.assertEqual(response["command"], command)
            self.assertEqual(response["status"], "failed")
            self.assertEqual(response["summary"]["bootstrap_status"], "not_ready")
            self.assertEqual(response["issues"][0]["code"], "Z4S_METASTORE_NOT_BOOTSTRAPPED")
            self.assertIn("z4s api bootstrap", response["issues"][0]["message"])

    def test_api_platform_bootstrap_runs_metastore_adapter(self) -> None:
        adapter = _FakeMetastoreAdapter()
        with (
            TemporaryDirectory() as tmp,
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.app._inspect_iceberg_rowset_store",
                return_value={"catalog_uri": "http://catalog", "warehouse": "warehouse", "status": "ok"},
            ),
            patch.dict(
                "os.environ",
                {"ZETA4S_SECRET_MASTER_KEY_FILE": str(Path(tmp) / "master.json")},
                clear=False,
            ),
        ):
            # keyring 은 운영자가 미리 둔다. bootstrap 은 만들지 않고 상태만 본다.
            init_master_key_file(Path(tmp) / "master.json")
            endpoint = _route_endpoint(create_app(), "/api/v1/platform/bootstrap", "POST")
            response = endpoint(authorization=None)

        self.assertTrue(adapter.bootstrapped)
        self.assertEqual(response["command"], "z4s api bootstrap")
        self.assertEqual(response["status"], "passed")
        self.assertEqual(response["summary"]["bootstrap"], "passed")
        self.assertIn("secret_master_key", [step["name"] for step in response["steps"]])
        iceberg = next(step for step in response["steps"] if step["name"] == "iceberg_rowset_store")
        self.assertEqual(
            iceberg["summary"], {"catalog_uri": "http://catalog", "warehouse": "warehouse", "status": "ok"}
        )

    def test_api_platform_bootstrap_does_not_create_the_keyring(self) -> None:
        """keyring 은 운영자 소유 입력이다.

        Kubernetes 는 Secret 을 read-only 로 mount 하므로 API 가 생성하려 들면 그
        배포 형태에서 실패한다. 없으면 만들지 말고 실패로 보고해야 한다.
        """
        adapter = _FakeMetastoreAdapter()
        with (
            TemporaryDirectory() as tmp,
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.app._inspect_iceberg_rowset_store",
                return_value={"catalog_uri": "http://catalog", "warehouse": "warehouse", "status": "ok"},
            ),
            patch.dict(
                "os.environ",
                {"ZETA4S_SECRET_MASTER_KEY_FILE": str(Path(tmp) / "master.json")},
                clear=False,
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/platform/bootstrap", "POST")
            response = endpoint(authorization=None)

            self.assertFalse((Path(tmp) / "master.json").exists())

        self.assertEqual(response["status"], "failed")
        step = next(item for item in response["steps"] if item["name"] == "secret_master_key")
        self.assertEqual(step["status"], "failed")

    def test_api_platform_status_reports_ready_schema(self) -> None:
        adapter = _FakeMetastoreAdapter()
        with patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter):
            endpoint = _route_endpoint(create_app(), "/api/v1/platform/status", "GET")
            response = endpoint(authorization=None)

        self.assertFalse(adapter.bootstrapped)
        self.assertEqual(response["bootstrap_status"], "ready")
        self.assertEqual(response["schema_status"], "ok")
        self.assertEqual(response["schema"]["status"], "ok")
        self.assertEqual(response["issues"], [])

    def test_api_deploy_requires_explicit_metastore_bootstrap(self) -> None:
        adapter = _FakeMissingMetastoreAdapter()
        request = DeployRequest(project_id="retail", bundle_base64="not-used", profile_id="local", profile={})
        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.app.decode_bundle",
                side_effect=AssertionError("deploy must stop before bundle decode"),
            ),
            patch(
                "zeta4s.api.app.project_operation_lock",
                return_value=nullcontext(),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/deploy", "POST")
            response = endpoint(request, authorization=None)

        self.assertFalse(adapter.bootstrapped)
        self.assertEqual(response["command"], "z4s api deploy")
        self.assertEqual(response["status"], "failed")
        self.assertEqual(response["summary"]["bootstrap_status"], "not_ready")
        self.assertEqual(response["issues"][0]["code"], "Z4S_METASTORE_NOT_BOOTSTRAPPED")
        self.assertIn("z4s api bootstrap", response["issues"][0]["message"])

    def test_api_deploy_reports_project_profile_dbt_validation_steps(self) -> None:
        backend_registry_repository = _FakeBackendRegistryRepository(
            [
                {
                    "project_id": "retail",
                    "backend_id": "legacy",
                    "backend_type": "clickhouse",
                    "backend": {"conn_id": "legacy", "type": "clickhouse"},
                    "status": "active",
                }
            ]
        )
        adapter = _FakeMetastoreAdapter(backend_registry_repository=backend_registry_repository)
        with TemporaryDirectory() as tmp:
            project_root = Path(tmp) / "retail"
            (project_root / "jobs").mkdir(parents=True)
            (project_root / "project.yml").write_text(
                "\n".join(
                    [
                        "project_id: retail",
                        "timezone: Asia/Seoul",
                        "paths:",
                        "  jobs: jobs",
                        "  dbt: dbt",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (project_root / "jobs" / "daily.yml").write_text(
                "\n".join(
                    [
                        "job_id: daily",
                        "steps:",
                        "  - step_id: start",
                        "    type: noop",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            request = DeployRequest(
                project_id="retail",
                bundle_base64="not-used",
                profile_id="local",
                profile={
                    "scheduler": "airflow",
                    "connections": {"analytics": {"type": "clickhouse", "host": "clickhouse"}},
                },
            )

            def fake_discover_and_unpause(project, expected_dag_ids, **kwargs):
                return {
                    "dag_discovery": {"status": "passed", "found": expected_dag_ids},
                    "dag_unpause": {"status": "passed", "requested_paused": False},
                }

            with (
                patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
                patch(
                    "zeta4s.api.app.decode_bundle",
                    return_value=(b"bundle", "sha256:deploy"),
                ),
                patch("zeta4s.api.app.extract_bundle"),
                patch(
                    "zeta4s.api.app._project_root_for_artifact",
                    return_value=project_root,
                ),
                patch("zeta4s.api.app.ensure_artifact_runtime_permissions"),
                patch(
                    "zeta4s.api.app.project_operation_lock",
                    return_value=nullcontext(),
                ),
                patch(
                    "zeta4s.api.app._airflow_register_prepare",
                    return_value={"connection_count": 0, "project_pool_count": 0, "project_pools": []},
                ),
                patch(
                    "zeta4s.airflow.runtime_check.check_profile_api",
                    return_value=[SimpleNamespace(conn_id="analytics", kind="clickhouse", ok=True, detail="SELECT 1")],
                ),
                patch(
                    "zeta4s.api.app._airflow_pause_and_terminate",
                    return_value={
                        "dag_ids": [],
                        "dag_pause": {"status": "passed"},
                        "active_run_terminate": {"status": "skipped"},
                    },
                ),
                patch(
                    "zeta4s.api.app._airflow_discover_and_unpause",
                    side_effect=fake_discover_and_unpause,
                ) as discover_and_unpause,
                patch("zeta4s.api.app.record_artifact_metadata"),
                patch(
                    "zeta4s.api.app.upsert_project_registration",
                    return_value=Path(tmp) / "registration.yml",
                ) as upsert_registration,
            ):
                endpoint = _route_endpoint(create_app(), "/api/v1/deploy", "POST")
                response = endpoint(request, authorization=None)

        step_names = [step["name"] for step in response["steps"]]
        self.assertEqual(response["status"], "passed")
        self.assertEqual(
            step_names,
            [
                "bundle_build",
                "project_check",
                "profile_check",
                "dag_pause",
                "active_run_terminate",
                "airflow_register_prepare",
                "connections_apply",
                "project_pools_apply",
                "dbt_validate",
                "artifact_register",
                "scheduler_deploy",
                "dag_discovery",
                "dag_unpause",
            ],
        )
        self.assertEqual(upsert_registration.call_args.kwargs["scheduler_backend"], "airflow")
        self.assertEqual(discover_and_unpause.call_args.kwargs["expected_artifact_id"], "sha256:deploy")
        self.assertEqual(response["checks"]["dbt_validate"]["status"], "skipped")
        self.assertEqual(
            [(item["backend_id"], item["status"]) for item in backend_registry_repository.upserts],
            [("analytics", "active"), ("legacy", "removed")],
        )
        artifact_step = next(step for step in response["steps"] if step["name"] == "artifact_register")
        self.assertEqual(artifact_step["summary"]["active_backend_count"], 1)
        self.assertEqual(artifact_step["summary"]["removed_backend_count"], 1)

    def test_api_deploy_project_check_validates_step_graph_file_refs(self) -> None:
        adapter = _FakeMetastoreAdapter()
        with TemporaryDirectory() as tmp:
            project_root = Path(tmp) / "retail"
            (project_root / "jobs").mkdir(parents=True)
            (project_root / "project.yml").write_text(
                "\n".join(
                    [
                        "project_id: retail",
                        "timezone: Asia/Seoul",
                        "paths:",
                        "  jobs: jobs",
                        "  dbt: dbt",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (project_root / "jobs" / "daily.yml").write_text(
                "\n".join(
                    [
                        "job_id: daily",
                        "steps:",
                        "  - step_id: run_query",
                        "    type: clickhouse.sql",
                        "    conn: analytics",
                        "    query: sql/missing.sql",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            request = DeployRequest(
                project_id="retail",
                bundle_base64="not-used",
                profile_id="local",
                profile={"connections": {"analytics": {"type": "clickhouse", "host": "clickhouse"}}},
            )

            with (
                patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
                patch(
                    "zeta4s.api.app.decode_bundle",
                    return_value=(b"bundle", "sha256:deploy"),
                ),
                patch("zeta4s.api.app.extract_bundle"),
                patch(
                    "zeta4s.api.app._project_root_for_artifact",
                    return_value=project_root,
                ),
                patch(
                    "zeta4s.api.app.project_operation_lock",
                    return_value=nullcontext(),
                ),
            ):
                endpoint = _route_endpoint(create_app(), "/api/v1/deploy", "POST")
                response = endpoint(request, authorization=None)

        self.assertEqual(response["status"], "failed")
        self.assertEqual([step["name"] for step in response["steps"]], DEPLOY_PROGRESS_STEPS)
        self.assertEqual(response["steps"][1]["name"], "project_check")
        self.assertEqual(response["steps"][1]["status"], "failed")
        self.assertIn("Z4S_PROJECT_CHECK_STEP_GRAPH_SCHEMA_FAILED", response["steps"][1]["issue_codes"])
        self.assertIn("sql/missing.sql", response["issues"][0]["message"])

    def test_api_undeploy_requires_registered_project(self) -> None:
        adapter = _FakeMetastoreAdapter()

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
            patch(
                "zeta4s.api.app._airflow_pause_and_terminate",
                side_effect=AssertionError("undeploy must not query stale Airflow resources without registration"),
            ),
            patch(
                "zeta4s.api.app.project_operation_lock",
                return_value=nullcontext(),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "missing"}, authorization=None)

        self.assertEqual(response["status"], "failed")
        self.assertEqual([step["name"] for step in response["steps"]], UNDEPLOY_PROGRESS_STEPS)
        self.assertEqual(response["steps"][0]["status"], "failed")
        self.assertEqual(response["issues"][0]["code"], "Z4S_PROJECT_NOT_REGISTERED")

    def test_api_undeploy_does_not_cleanup_stale_airflow_resources_without_registration(self) -> None:
        adapter = _FakeMetastoreAdapter()

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
            patch(
                "zeta4s.api.app._airflow_pause_and_terminate",
                side_effect=AssertionError("undeploy must not query stale Airflow resources without registration"),
            ),
            patch(
                "zeta4s.api.app.project_operation_lock",
                return_value=nullcontext(),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "retail"}, authorization=None)

        self.assertEqual(response["status"], "failed")
        self.assertEqual([step["name"] for step in response["steps"]], UNDEPLOY_PROGRESS_STEPS)
        self.assertEqual(response["steps"][0]["status"], "failed")
        self.assertEqual(response["issues"][0]["code"], "Z4S_PROJECT_NOT_REGISTERED")

    def test_api_undeploy_failure_report_includes_all_steps(self) -> None:
        adapter = _FakeMetastoreAdapter(
            deployment_repository=_FakeDeploymentRepository(
                [
                    {
                        "project_id": "retail",
                        "artifact_id": "sha256:retail",
                        "scheduler_backend": "airflow",
                        "dags": [],
                    },
                ]
            )
        )

        with (
            patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
            patch(
                "zeta4s.api.app._airflow_pause_and_terminate",
                return_value={
                    "dag_ids": ["retail__daily"],
                    "dag_pause": {"status": "failed", "not_converged_count": 1},
                    "active_run_terminate": {"status": "skipped", "remaining_runs": [], "remaining_task_instances": []},
                },
            ),
            patch(
                "zeta4s.api.app.project_operation_lock",
                return_value=nullcontext(),
            ),
        ):
            endpoint = _route_endpoint(create_app(), "/api/v1/undeploy", "POST")
            response = endpoint({"project_id": "retail"}, authorization=None)

        self.assertEqual(response["status"], "failed")
        self.assertEqual([step["name"] for step in response["steps"]], UNDEPLOY_PROGRESS_STEPS)
        self.assertEqual(response["steps"][0]["status"], "passed")
        self.assertEqual(response["steps"][1]["status"], "failed")
        self.assertEqual(response["steps"][3]["name"], "scheduler_cleanup")
        self.assertEqual(response["steps"][3]["status"], "skipped")

    def test_api_redeploy_progress_is_aggregate_two_steps(self) -> None:
        self.assertEqual(REDEPLOY_PROGRESS_STEPS, ["undeploy", "deploy"])

    def test_cli_api_bootstrap_calls_platform_endpoint(self) -> None:
        captured = {}

        def fake_post_json(api_alias, endpoint, payload):
            captured.update({"api_alias": api_alias, "endpoint": endpoint, "payload": payload})
            return {
                "operation_id": "op-test",
                "command": "z4s api bootstrap",
                "status": "passed",
                "summary": {"bootstrap": "passed"},
                "steps": [],
                "issues": [],
            }

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
                side_effect=fake_post_json,
            ),
        ):
            result = CliRunner().invoke(cli, ["api", "bootstrap", "--api", "local"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(
            captured,
            {
                "api_alias": "local",
                "endpoint": "/api/v1/platform/bootstrap",
                "payload": {},
            },
        )

    def test_cli_api_status_calls_platform_endpoint(self) -> None:
        captured = {}

        def fake_get_json(api_alias, endpoint):
            captured.update({"api_alias": api_alias, "endpoint": endpoint})
            return {
                "api_version": "0.1.0",
                "metastore": {"type": "clickhouse", "database": "zeta4s_metastore"},
                "bootstrap_status": "ready",
                "schema_status": "ok",
                "scheduler_snapshot_status": "unknown",
                "issues": [],
            }

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._get_json",
                side_effect=fake_get_json,
            ),
        ):
            result = CliRunner().invoke(cli, ["api", "status", "--api", "local"])

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(
            captured,
            {
                "api_alias": "local",
                "endpoint": "/api/v1/platform/status",
            },
        )

    def test_cli_api_secret_commands_fail_on_failed_operation_report(self) -> None:
        failed_report = {
            "command": "z4s api secret",
            "status": "failed",
            "summary": {"bootstrap_status": "not_ready"},
            "issues": [{"code": "Z4S_METASTORE_NOT_BOOTSTRAPPED", "message": "Run z4s api bootstrap"}],
        }

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
                return_value=failed_report,
            ),
            patch("zeta4s.cli.main._stdin_is_interactive", return_value=False),
        ):
            set_result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password"],
                input="plain-password",
            )
            check_result = CliRunner().invoke(
                cli,
                ["api", "secret", "check", "prod.analytics_clickhouse.password"],
            )

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._get_json",
                return_value=failed_report,
            ),
        ):
            list_result = CliRunner().invoke(cli, ["api", "secret", "list"])

        self.assertEqual(set_result.exit_code, 1, set_result.output)
        self.assertEqual(list_result.exit_code, 1, list_result.output)
        self.assertEqual(check_result.exit_code, 1, check_result.output)
        self.assertIn("Z4S_METASTORE_NOT_BOOTSTRAPPED", set_result.output)
        self.assertIn("Z4S_METASTORE_NOT_BOOTSTRAPPED", list_result.output)
        self.assertIn("Z4S_METASTORE_NOT_BOOTSTRAPPED", check_result.output)

    def test_cli_entrypoint_returns_secret_failed_report_exit_code(self) -> None:
        failed_report = {
            "command": "z4s api secret list",
            "status": "failed",
            "summary": {"bootstrap_status": "not_ready"},
            "issues": [{"code": "Z4S_METASTORE_NOT_BOOTSTRAPPED", "message": "Run z4s api bootstrap"}],
        }

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._get_json",
                return_value=failed_report,
            ),
        ):
            exit_code = main(["api", "secret", "list"])

        self.assertEqual(exit_code, 1)

    def test_cli_api_secret_set_prompts_without_echoing_secret_value(self) -> None:
        captured = {}

        def fake_post_json(api_alias, endpoint, payload):
            captured.update({"api_alias": api_alias, "endpoint": endpoint, "payload": payload})
            return {"secret_key": payload["secret_key"], "version": 1, "status": "active"}

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
                side_effect=fake_post_json,
            ),
            patch("zeta4s.cli.main._stdin_is_interactive", return_value=True),
        ):
            result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password"],
                input="plain-password\nplain-password\n",
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(captured["endpoint"], "/api/v1/secrets")
        self.assertEqual(
            captured["payload"],
            {
                "secret_key": "prod.analytics_clickhouse.password",
                "value": "plain-password",
            },
        )
        self.assertNotIn("plain-password", result.output)

    def test_cli_api_secret_set_can_read_piped_value_without_echoing_it(self) -> None:
        captured = {}

        def fake_post_json(api_alias, endpoint, payload):
            captured.update({"api_alias": api_alias, "endpoint": endpoint, "payload": payload})
            return {"secret_key": payload["secret_key"], "version": 1, "status": "active"}

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
                side_effect=fake_post_json,
            ),
            patch("zeta4s.cli.main._stdin_is_interactive", return_value=False),
        ):
            result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password"],
                input="plain-password\n",
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(captured["endpoint"], "/api/v1/secrets")
        self.assertEqual(
            captured["payload"],
            {
                "secret_key": "prod.analytics_clickhouse.password",
                "value": "plain-password\n",
            },
        )
        self.assertNotIn("plain-password", result.output)

    def test_cli_api_secret_set_preserves_piped_trailing_newlines(self) -> None:
        captured = {}

        def fake_post_json(api_alias, endpoint, payload):
            captured.update({"api_alias": api_alias, "endpoint": endpoint, "payload": payload})
            return {"secret_key": payload["secret_key"], "version": 1, "status": "active"}

        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
                side_effect=fake_post_json,
            ),
            patch("zeta4s.cli.main._stdin_is_interactive", return_value=False),
        ):
            result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password"],
                input="plain-password\n\n",
            )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(captured["payload"]["value"], "plain-password\n\n")
        self.assertNotIn("plain-password", result.output)

    def test_cli_api_secret_set_rejects_empty_stdin_value(self) -> None:
        with (
            TemporaryDirectory() as home_tmp,
            patch.object(Path, "home", return_value=Path(home_tmp)),
            patch(
                "zeta4s.cli.main._post_json",
            ) as post_json,
            patch("zeta4s.cli.main._stdin_is_interactive", return_value=False),
        ):
            result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password"],
                input="",
            )

        self.assertIn("secret value from stdin must not be empty", result.output)
        post_json.assert_not_called()

    def test_cli_api_secret_set_rejects_secret_value_argument(self) -> None:
        with TemporaryDirectory() as home_tmp, patch.object(Path, "home", return_value=Path(home_tmp)):
            result = CliRunner().invoke(
                cli,
                ["api", "secret", "set", "prod.analytics_clickhouse.password", "plain-password"],
            )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Got unexpected extra argument", result.output)


class _FakeAdapter:
    def __init__(self, repository):
        self.secret_repository = repository


class _FakeDeploymentRegistration:
    def __init__(self, item):
        self.item = dict(item)

    def as_scheduler_item(self):
        return dict(self.item)


class _FakeDeploymentRepository:
    def __init__(self, items=None):
        self.items = {str(item["project_id"]): _FakeDeploymentRegistration(item) for item in (items or [])}

    def list_active(self):
        return [self.items[key] for key in sorted(self.items)]

    def remove_active(self, project_id):
        return self.items.pop(project_id, None)

    def artifact_is_active(self, artifact_id):
        return any(item.as_scheduler_item().get("artifact_id") == artifact_id for item in self.items.values())


class _FakeOperationReportRepository:
    def __init__(self):
        self.reports = []

    def save_report(self, report):
        self.reports.append(report)


class _FakeArtifactMetadata:
    def __init__(self, item):
        self.artifact_id = item["artifact_id"]
        self.project_id = item["project_id"]
        self.storage_uri = item["storage_uri"]
        self.runtime_connections = item.get("runtime_connections") or []
        self.dags = item.get("dags") or []
        self.created_at = item["created_at"]


class _FakeArtifactRepository:
    def __init__(self, items=None):
        self.items = {str(artifact_id): _FakeArtifactMetadata(item) for artifact_id, item in (items or {}).items()}

    def upsert_artifact(self, **kwargs):
        item = _FakeArtifactMetadata(
            {
                "artifact_id": kwargs["artifact_id"],
                "project_id": kwargs["project_id"],
                "storage_uri": kwargs["storage_uri"],
                "runtime_connections": kwargs.get("runtime_connections") or [],
                "dags": kwargs.get("dags") or [],
                "created_at": "2026-07-09T00:00:00+00:00",
            }
        )
        self.items[item.artifact_id] = item
        return item

    def get_artifact(self, artifact_id):
        return self.items.get(artifact_id)


class _FakeBackendRegistryRepository:
    def __init__(self, items=None):
        self.items = list(items or [])
        self.upserts = []

    def upsert_backend(self, **kwargs):
        self.upserts.append(kwargs)
        self.items.append(kwargs)

    def get_backend(self, **kwargs):
        for item in reversed(self.items):
            if all(item.get(key) == value for key, value in kwargs.items()):
                return item
        return None

    def list_backends(self, *, project_id, status=None):
        items = [item for item in self.items if item.get("project_id") == project_id]
        if status is not None:
            items = [item for item in items if item.get("status") == status]
        return items


class _FakeMetastoreAdapter:
    database = "zeta4s_metastore"

    def __init__(self, deployment_repository=None, artifact_repository=None, backend_registry_repository=None):
        self.bootstrapped = False
        self.deployment_repository = deployment_repository or _FakeDeploymentRepository()
        self.artifact_repository = artifact_repository or _FakeArtifactRepository()
        self.backend_registry_repository = backend_registry_repository or _FakeBackendRegistryRepository()
        self.operation_report_repository = _FakeOperationReportRepository()

    def bootstrap(self) -> None:
        self.bootstrapped = True

    def inspect_schema(self):
        return {"database": self.database, "status": "ok", "missing_tables": []}


class _FakeMissingMetastoreAdapter(_FakeMetastoreAdapter):
    def inspect_schema(self):
        return {"database": self.database, "status": "missing", "missing_tables": ["deploy_registration"]}


class _FakeSecretRepository:
    def __init__(self):
        self.rows: list[dict] = []
        self.lock_calls: list[str] = []

    def put_secret_version(self, **kwargs) -> None:
        # 실제 postgres 는 PRIMARY KEY (secret_key, version) + ON CONFLICT DO UPDATE 다.
        # append 로그로 흉내내면 낡은 row 가 남아 CAS 가 실제와 다르게 동작한다.
        key = (kwargs["secret_key"], kwargs["version"])
        for row in self.rows:
            if (row["secret_key"], row["version"]) == key:
                row.update(kwargs)
                return
        self.rows.append(dict(kwargs))

    @contextmanager
    def secret_write_lock(self, secret_key: str):
        self.lock_calls.append(secret_key)
        yield

    def reencrypt_secret_version(
        self,
        *,
        secret_key: str,
        version: int,
        expected_key_id: str,
        ciphertext: str,
        key_id: str,
    ) -> bool:
        for row in self.rows:
            if (
                row["secret_key"] == secret_key
                and row["version"] == version
                and row["status"] == "active"
                and row["key_id"] == expected_key_id
            ):
                row["ciphertext"] = ciphertext
                row["key_id"] = key_id
                return True
        return False

    def get_active_secret(self, secret_key: str):
        active = [row for row in self.rows if row["secret_key"] == secret_key and row["status"] == "active"]
        if not active:
            return None
        return max(active, key=lambda row: row["version"])

    def list_secret_metadata(self):
        return [
            {
                "secret_key": row["secret_key"],
                "version": row["version"],
                "algorithm": row["algorithm"],
                "key_id": row["key_id"],
                "status": row["status"],
            }
            for row in self.rows
        ]


class _FakeEncryptedSecretStore:
    def __init__(self):
        self.set_calls = []
        self.check_calls = []

    def set_secret(self, secret_key, value):
        self.set_calls.append((secret_key, value))
        return {"secret_key": secret_key, "version": 1, "status": "active"}

    def list_metadata(self):
        return []

    def check_secret(self, secret_key):
        self.check_calls.append(secret_key)
        return {"secret_key": secret_key, "active": True, "decryptable": True}


class _FakeProfileConnectionCheck:
    def __init__(self, *, conn_id, kind, ok, detail):
        self.conn_id = conn_id
        self.kind = kind
        self.ok = ok
        self.detail = detail


def _route_endpoint(app, path: str, method: str):
    for route in app.routes:
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
            return route.endpoint
    raise AssertionError(f"route not found: {method} {path}")


if __name__ == "__main__":
    unittest.main()
