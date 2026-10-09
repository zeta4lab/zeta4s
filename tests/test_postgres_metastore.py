from __future__ import annotations

import os
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from zeta4s.metastore.backends.postgres import (
    DEFAULT_POSTGRES_DSN,
    REQUIRED_TABLES,
    PostgresMetastoreAdapter,
    postgres_dsn,
)
from zeta4s.metastore.backends.clickhouse import ClickHouseMetastoreAdapter
from zeta4s.metastore.factory import metastore_adapter_factory


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        # compare-and-set 판정이 rowcount 를 본다. 갱신된 행 수를 흉내낸다.
        self.rowcount = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def execute(self, sql: str, parameters=None) -> None:
        self.connection.statements.append((sql, parameters))
        self.connection.current_rows = self.connection.results.pop(0) if self.connection.results else []
        self.rowcount = self.connection.next_rowcount

    def fetchall(self) -> list[dict[str, object]]:
        if self.connection.current_rows:
            return self.connection.current_rows
        return [{"table_name": name} for name in self.connection.existing_tables]

    def fetchone(self) -> dict[str, object] | None:
        return self.connection.current_rows[0] if self.connection.current_rows else None


class FakeConnection:
    def __init__(
        self,
        *,
        existing_tables: tuple[str, ...] = (),
        results: list[list[dict[str, object]]] | None = None,
    ) -> None:
        self.existing_tables = existing_tables
        self.statements: list[tuple[str, object]] = []
        self.results = list(results or [])
        self.current_rows: list[dict[str, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.next_rowcount = 1

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


class FakeConnect:
    def __init__(
        self,
        *,
        existing_tables: tuple[str, ...] = (),
        results: list[list[dict[str, object]]] | None = None,
    ) -> None:
        self.connection = FakeConnection(existing_tables=existing_tables, results=results)
        self.calls: list[tuple[str, object]] = []

    def __call__(self, dsn: str, *, row_factory=None) -> FakeConnection:
        self.calls.append((dsn, row_factory))
        return self.connection


class PostgresMetastoreSchemaTest(unittest.TestCase):
    def test_postgres_dsn_uses_local_container_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(postgres_dsn(), DEFAULT_POSTGRES_DSN)

    def test_postgres_dsn_uses_explicit_environment_value(self) -> None:
        with patch.dict(
            os.environ,
            {"ZETA4S_METASTORE_DSN": "postgresql://runtime:secret@db/runtime"},
            clear=True,
        ):
            self.assertEqual(postgres_dsn(), "postgresql://runtime:secret@db/runtime")

    def test_adapter_exposes_database_identity_from_dsn(self) -> None:
        adapter = PostgresMetastoreAdapter(
            dsn="postgresql://runtime:secret@db/runtime_metastore",
            connect=FakeConnect(),
        )

        self.assertEqual(adapter.database, "runtime_metastore")

    def test_bootstrap_uses_postgres_native_types_and_keys(self) -> None:
        connect = FakeConnect()
        adapter = PostgresMetastoreAdapter(connect=connect)

        adapter.bootstrap()

        ddl = "\n".join(sql for sql, _ in connect.connection.statements)
        normalized_ddl = " ".join(ddl.split())
        self.assertEqual(len(connect.connection.statements), len(REQUIRED_TABLES) + 1)
        self.assertIn("JSONB", ddl)
        self.assertIn("TIMESTAMPTZ", ddl)
        self.assertIn("PRIMARY KEY (project_id)", ddl)
        self.assertIn("BIGSERIAL PRIMARY KEY", ddl)
        self.assertIn(
            "scheduler_backend TEXT NOT NULL CHECK (scheduler_backend IN ('airflow', 'prefect'))",
            ddl,
        )
        self.assertIn(
            "ADD COLUMN IF NOT EXISTS scheduler_backend TEXT NOT NULL "
            "CHECK (scheduler_backend IN ('airflow', 'prefect'))",
            normalized_ddl,
        )
        self.assertNotIn("scheduler_backend TEXT NOT NULL DEFAULT", ddl)
        self.assertNotIn("ReplacingMergeTree", ddl)
        for table in REQUIRED_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", ddl)

    def test_inspect_schema_reports_missing_tables(self) -> None:
        connect = FakeConnect(existing_tables=("artifact", "run_metadata"))

        result = PostgresMetastoreAdapter(connect=connect).inspect_schema()

        self.assertEqual(result["status"], "missing")
        self.assertEqual(result["existing_tables"], ["artifact", "run_metadata"])
        self.assertEqual(
            result["missing_tables"],
            sorted(set(REQUIRED_TABLES) - {"artifact", "run_metadata"}),
        )

    def test_inspect_schema_reports_complete_schema(self) -> None:
        connect = FakeConnect(existing_tables=REQUIRED_TABLES)

        result = PostgresMetastoreAdapter(connect=connect).inspect_schema()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["missing_tables"], [])
        sql, parameters = connect.connection.statements[0]
        self.assertIn("information_schema.tables", sql)
        self.assertEqual(parameters, {"required_tables": list(REQUIRED_TABLES)})


class PostgresControlPlaneRepositoryTest(unittest.TestCase):
    def test_deployment_upsert_uses_business_key_conflict(self) -> None:
        connect = FakeConnect()
        repository = PostgresMetastoreAdapter(connect=connect).deployment_repository

        registration = repository.upsert_active(
            project_id="analytics",
            artifact_id="sha256:abc",
            profile_id="default",
            scheduler_backend="prefect",
            dags=[{"dag_id": "daily"}],
        )

        sql, parameters = connect.connection.statements[0]
        self.assertIn("ON CONFLICT (project_id) DO UPDATE", sql)
        self.assertEqual(parameters["project_id"], "analytics")
        self.assertEqual(parameters["scheduler_backend"], "prefect")
        self.assertEqual(registration.project_id, "analytics")
        self.assertEqual(registration.scheduler_backend, "prefect")
        self.assertEqual(connect.connection.commits, 1)

    def test_secret_write_lock_takes_a_per_key_advisory_lock(self) -> None:
        """set_secret 은 현재 version 을 읽어 max+1 을 쓴다.

        동시 호출이 같은 값을 계산하면 서로를 덮으므로 secret 별로 직렬화한다.
        """
        connect = FakeConnect()
        repository = PostgresMetastoreAdapter(connect=connect).secret_repository

        with repository.secret_write_lock("prod.analytics.password"):
            pass

        sql, parameters = connect.connection.statements[0]
        self.assertIn("pg_advisory_xact_lock", sql)
        self.assertEqual(parameters["lock_key"], "zeta4s:secret:prod.analytics.password")

    def test_reencrypt_secret_version_is_compare_and_set(self) -> None:
        """회전 재암호화는 row 가 아직 옛 세대 active 일 때만 갱신한다.

        그 사이 사용자가 새 값을 썼다면 새 version 이 이미 활성 세대로 암호화되어
        있으므로 덮으면 안 된다.
        """
        connect = FakeConnect()
        repository = PostgresMetastoreAdapter(connect=connect).secret_repository

        repository.reencrypt_secret_version(
            secret_key="prod.analytics.password",
            version=3,
            expected_key_id="old-gen",
            ciphertext="{}",
            key_id="new-gen",
        )

        sql, parameters = connect.connection.statements[0]
        self.assertIn("UPDATE", sql)
        self.assertIn("status = 'active'", sql)
        self.assertIn("key_id = %(expected_key_id)s", sql)
        self.assertEqual(parameters["expected_key_id"], "old-gen")
        self.assertEqual(parameters["key_id"], "new-gen")
        self.assertEqual(parameters["version"], 3)

    def test_deployment_list_returns_contract_shape(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        connect = FakeConnect(
            results=[
                [
                    {
                        "project_id": "analytics",
                        "artifact_id": "sha256:abc",
                        "profile_id": "default",
                        "scheduler_backend": "airflow",
                        "registered_at": now,
                        "dags": [{"dag_id": "daily"}],
                        "status": "active",
                    }
                ]
            ]
        )

        registrations = PostgresMetastoreAdapter(connect=connect).deployment_repository.list_active()

        self.assertEqual(registrations[0].registered_at, "2026-07-11T00:00:00+00:00")
        self.assertEqual(registrations[0].scheduler_backend, "airflow")
        self.assertEqual(registrations[0].dags, [{"dag_id": "daily"}])

    def test_artifact_get_preserves_structured_json(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        connect = FakeConnect(
            results=[
                [
                    {
                        "artifact_id": "sha256:abc",
                        "project_id": "analytics",
                        "storage_uri": "file:///tmp/artifact.tar.gz",
                        "runtime_connections": [{"conn_id": "analytics"}],
                        "dags": [{"dag_id": "daily"}],
                        "created_at": now,
                    }
                ]
            ]
        )

        artifact = PostgresMetastoreAdapter(connect=connect).artifact_repository.get_artifact("sha256:abc")

        self.assertIsNotNone(artifact)
        self.assertEqual(artifact.runtime_connections, [{"conn_id": "analytics"}])
        self.assertEqual(artifact.dags, [{"dag_id": "daily"}])

    def test_operation_report_without_operation_id_is_not_written(self) -> None:
        connect = FakeConnect()

        PostgresMetastoreAdapter(connect=connect).operation_report_repository.save_report(
            {"command": "deploy", "status": "success"}
        )

        self.assertEqual(connect.connection.statements, [])
        self.assertEqual(connect.connection.commits, 0)

    def test_secret_activation_deactivates_previous_version_in_one_transaction(self) -> None:
        connect = FakeConnect()
        repository = PostgresMetastoreAdapter(connect=connect).secret_repository

        repository.put_secret_version(
            secret_key="warehouse/password",
            version=1,
            ciphertext="encrypted-value",
            algorithm="AES-256-GCM",
            key_id="local-test-key",
            status="active",
        )

        sql = "\n".join(statement for statement, _ in connect.connection.statements)
        parameters = [parameters for _, parameters in connect.connection.statements]
        self.assertIn("status = 'inactive'", sql)
        self.assertIn("ON CONFLICT (secret_key, version) DO UPDATE", sql)
        self.assertEqual(connect.connection.commits, 1)
        self.assertTrue(any(item.get("ciphertext") == "encrypted-value" for item in parameters))
        self.assertNotIn("plaintext", sql.lower())

    def test_secret_metadata_excludes_ciphertext(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        connect = FakeConnect(
            results=[
                [
                    {
                        "secret_key": "warehouse/password",
                        "version": 1,
                        "algorithm": "AES-256-GCM",
                        "key_id": "local-test-key",
                        "status": "active",
                        "created_at": now,
                        "rotated_at": None,
                    }
                ]
            ]
        )

        metadata = PostgresMetastoreAdapter(connect=connect).secret_repository.list_secret_metadata()

        self.assertNotIn("ciphertext", metadata[0])
        self.assertEqual(metadata[0]["created_at"], "2026-07-11T00:00:00+00:00")


class PostgresRuntimeRepositoryTest(unittest.TestCase):
    def test_factory_defaults_to_postgres(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsInstance(metastore_adapter_factory(), PostgresMetastoreAdapter)

    def test_factory_preserves_clickhouse_selection(self) -> None:
        with patch.dict(os.environ, {"ZETA4S_METASTORE_TYPE": "clickhouse"}, clear=True):
            self.assertIsInstance(metastore_adapter_factory(), ClickHouseMetastoreAdapter)

    def test_step_execution_upserts_same_attempt(self) -> None:
        connect = FakeConnect()
        repository = PostgresMetastoreAdapter(connect=connect).step_execution_repository
        execution = {
            "project_id": "analytics",
            "job_id": "daily",
            "run_id": "run-1",
            "step_id": "extract",
            "step_type": "oracle.sql",
            "attempt": 1,
            "task_id": "extract",
        }

        repository.record_execution(**execution, status="running")
        repository.record_execution(**execution, status="success")

        sql = "\n".join(statement for statement, _ in connect.connection.statements)
        self.assertIn(
            "ON CONFLICT (project_id, job_id, run_id, step_id, task_id, attempt)",
            sql,
        )
        self.assertEqual(connect.connection.commits, 2)

    def test_run_list_returns_structured_payloads(self) -> None:
        run = {
            "run_id": "run-1",
            "project_id": "analytics",
            "job_id": "daily",
            "status": "success",
        }
        connect = FakeConnect(results=[[{"run": run}]])

        runs = PostgresMetastoreAdapter(connect=connect).run_metadata_repository.list_runs(
            project_id="analytics", job_id="daily", limit=10
        )

        self.assertEqual(runs, [run])
        _, parameters = connect.connection.statements[0]
        self.assertEqual(parameters, {"project_id": "analytics", "job_id": "daily", "limit": 10})

    def test_state_get_preserves_json_value_and_contract_fields(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        row = {
            "project_id": "analytics",
            "job_id": "daily",
            "step_id": "extract",
            "state_key": "cursor",
            "state_type": "json",
            "state_value": {"id": 42},
            "run_id": "run-1",
            "revision": 7,
            "updated_at": now,
        }
        connect = FakeConnect(results=[[row]])

        state = PostgresMetastoreAdapter(connect=connect).step_state_repository.get_state(
            project_id="analytics", job_id="daily", step_id="extract", state_key="cursor"
        )

        self.assertEqual(state["state_value"], {"id": 42})
        self.assertEqual(state["revision"], 7)

    def test_event_list_orders_by_created_at_and_identity(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        row = {
            "project_id": "analytics",
            "job_id": "daily",
            "run_id": "run-1",
            "step_id": "extract",
            "task_id": "extract",
            "event_type": "progress",
            "status": "running",
            "event": {"rows": 10},
            "created_at": now,
            "revision": 8,
            "event_id": 3,
        }
        connect = FakeConnect(results=[[row]])

        events = PostgresMetastoreAdapter(connect=connect).step_event_repository.list_events(
            project_id="analytics", job_id="daily", event_type="progress", limit=900
        )

        sql, parameters = connect.connection.statements[0]
        self.assertIn("ORDER BY created_at DESC, event_id DESC", sql)
        self.assertEqual(parameters["limit"], 500)
        self.assertNotIn("event_id", events[0])
        self.assertEqual(events[0]["event"], {"rows": 10})

    def test_output_binding_get_returns_structured_binding(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        row = {
            "project_id": "analytics",
            "job_id": "daily",
            "run_id": "run-1",
            "step_id": "extract",
            "output_name": "rows",
            "output_kind": "table",
            "binding": {"table": "staging.events"},
            "revision": 9,
            "updated_at": now,
        }
        connect = FakeConnect(results=[[row]])

        binding = PostgresMetastoreAdapter(connect=connect).step_output_binding_repository.get_binding(
            project_id="analytics",
            job_id="daily",
            run_id="run-1",
            step_id="extract",
            output_name="rows",
        )

        self.assertEqual(binding["binding"], {"table": "staging.events"})
        self.assertEqual(binding["revision"], 9)

    def test_backend_list_filters_status_and_returns_structured_backend(self) -> None:
        now = datetime(2026, 7, 11, tzinfo=timezone.utc)
        row = {
            "project_id": "analytics",
            "backend_id": "warehouse",
            "backend_type": "postgres",
            "backend": {"database": "analytics"},
            "status": "active",
            "revision": 10,
            "updated_at": now,
        }
        connect = FakeConnect(results=[[row]])

        backends = PostgresMetastoreAdapter(connect=connect).backend_registry_repository.list_backends(
            project_id="analytics", status="active"
        )

        self.assertEqual(backends[0]["backend"], {"database": "analytics"})
        _, parameters = connect.connection.statements[0]
        self.assertEqual(parameters, {"project_id": "analytics", "status": "active"})


if __name__ == "__main__":
    unittest.main()
