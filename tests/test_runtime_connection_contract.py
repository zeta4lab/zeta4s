from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from zeta4s.api.app import (
    RuntimeStepExecuteRequest,
    _active_runtime_execution,
    create_app,
)
from zeta4s.airflow.runtime_check import _resolve_connection
from zeta4s.metastore.contracts import ArtifactMetadata
from zeta4s.runtime.connection_policy import runtime_connection_policy_from_profile


class RuntimeConnectionContractTest(unittest.TestCase):
    def test_internal_step_endpoint_requires_token(self) -> None:
        endpoint = _route_endpoint(create_app(), "/internal/v1/runtime/steps/execute", "POST")
        request = RuntimeStepExecuteRequest(
            project_id="retail",
            artifact_id="sha256:abc",
            profile_id="prod",
            job_id="daily",
            step_id="extract",
            run_id="run-1",
        )
        with patch.dict("os.environ", {"ZETA4S_RUNTIME_INTERNAL_TOKEN": "runtime-token"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                endpoint(request, authorization="Bearer wrong")
        self.assertEqual(raised.exception.status_code, 401)

    def test_internal_step_endpoint_dispatches_authenticated_request(self) -> None:
        endpoint = _route_endpoint(create_app(), "/internal/v1/runtime/steps/execute", "POST")
        request = RuntimeStepExecuteRequest(
            project_id="retail",
            artifact_id="sha256:abc",
            profile_id="prod",
            job_id="daily",
            step_id="extract",
            run_id="run-1",
        )
        with (
            patch.dict("os.environ", {"ZETA4S_RUNTIME_INTERNAL_TOKEN": "runtime-token"}, clear=False),
            patch("zeta4s.api.app._execute_runtime_step", return_value={"status": "succeeded"}) as execute,
        ):
            response = endpoint(request, authorization="Bearer runtime-token")
        self.assertEqual(response, {"status": "succeeded"})
        execute.assert_called_once_with(request)

    def test_internal_execution_rejects_inactive_deployment(self) -> None:
        with patch("zeta4s.api.app._active_project_registration", return_value=None):
            with self.assertRaises(HTTPException) as raised:
                _active_runtime_execution(
                    project_id="retail",
                    artifact_id="sha256:abc",
                    profile_id="prod",
                    job_id="daily",
                )
        self.assertEqual(raised.exception.status_code, 404)

    def test_runtime_connection_policy_keeps_password_ref_without_plaintext(self) -> None:
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

        policies = runtime_connection_policy_from_profile(profile)

        self.assertEqual(
            policies,
            [
                {
                    "conn_id": "analytics_clickhouse",
                    "conn_type": "clickhouse",
                    "host": "clickhouse",
                    "port": 8123,
                    "login": "metastore",
                    "schema": "analytics",
                    "extra": {
                        "database": "analytics",
                        "password_ref": "prod.analytics_clickhouse.password",
                    },
                }
            ],
        )
        self.assertNotIn("password", policies[0])
        self.assertNotIn("plain-password", repr(policies))

    def test_internal_connection_endpoint_resolves_active_profile_secret(self) -> None:
        with TemporaryDirectory() as tmp:
            home = Path(tmp)
            metastore = _FakeMetastoreAdapter(
                ArtifactMetadata(
                    artifact_id="sha256:abc",
                    project_id="retail",
                    storage_uri=str(home / "artifacts" / "sha256-abc"),
                    dags=[],
                    created_at="2026-07-09T00:00:00+00:00",
                    runtime_connections=[
                        {
                            "conn_id": "analytics_clickhouse",
                            "conn_type": "clickhouse",
                            "host": "clickhouse",
                            "port": 8123,
                            "login": "metastore",
                            "schema": "analytics",
                            "extra": {
                                "database": "analytics",
                                "password_ref": "prod.analytics_clickhouse.password",
                            },
                        }
                    ],
                )
            )
            store = _FakeSecretStore({"prod.analytics_clickhouse.password": "plain-password"})

            with (
                patch.dict("os.environ", {"ZETA4S_RUNTIME_INTERNAL_TOKEN": "runtime-token"}),
                patch(
                    "zeta4s.api.services.registration_store.load_registrations",
                    return_value={
                        "registrations": [
                            {
                                "project_id": "retail",
                                "artifact_id": "sha256:abc",
                                "scheduler_backend": "airflow",
                            }
                        ]
                    },
                ),
                patch(
                    "zeta4s.api.app.metastore_adapter_factory",
                    return_value=metastore,
                ),
                patch("zeta4s.api.app.EncryptedSecretStore", return_value=store),
            ):
                endpoint = _route_endpoint(create_app(), "/internal/v1/runtime/connections/{conn_id}", "GET")
                response = endpoint("analytics_clickhouse", authorization="Bearer runtime-token")

        self.assertEqual(response["conn_id"], "analytics_clickhouse")
        self.assertEqual(response["password"], "plain-password")
        self.assertEqual(response["extra"]["password_ref"], "prod.analytics_clickhouse.password")

    def test_runtime_check_candidate_policy_resolves_secret_without_active_artifact(self) -> None:
        policy = {
            "conn_id": "analytics_clickhouse",
            "conn_type": "clickhouse",
            "host": "clickhouse",
            "port": 8123,
            "login": "metastore",
            "schema": "analytics",
            "extra": {
                "database": "analytics",
                "password_ref": "prod.analytics_clickhouse.password",
            },
        }
        store = _FakeSecretStore({"prod.analytics_clickhouse.password": "plain-password"})

        # profile 경로는 airflow 에 닿지 않는다. import 를 막아 두면 airflow 폴백이 생기는 순간
        # 이 테스트가 먼저 깨진다.
        with (
            patch.dict("sys.modules", {"airflow": None}),
            patch(
                "zeta4s.airflow.runtime_check.EncryptedSecretStore",
                return_value=store,
            ),
        ):
            connection = _resolve_connection("analytics_clickhouse", [policy])

        self.assertEqual(connection.conn_id, "analytics_clickhouse")
        self.assertEqual(connection.password, "plain-password")
        self.assertEqual(json.loads(connection.extra)["password_ref"], "prod.analytics_clickhouse.password")


class _FakeSecretStore:
    def __init__(self, values):
        self.values = values

    def resolve_secret(self, secret_key):
        return self.values[secret_key]


class _FakeMetastoreAdapter:
    def __init__(self, artifact: ArtifactMetadata):
        self.artifact_repository = _FakeArtifactRepository(artifact)


class _FakeArtifactRepository:
    def __init__(self, artifact: ArtifactMetadata):
        self.artifact = artifact

    def get_artifact(self, artifact_id: str):
        if artifact_id == self.artifact.artifact_id:
            return self.artifact
        return None


class _FakeHttpResponse:
    def __init__(self, body: bytes):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return self.body


def _fake_connection_class():
    class Connection:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    return Connection


def _route_endpoint(app, path: str, method: str):
    for route in app.routes:
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
            return route.endpoint
    raise AssertionError(f"route not found: {method} {path}")


if __name__ == "__main__":
    unittest.main()
