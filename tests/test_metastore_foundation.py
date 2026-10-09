from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from fastapi import HTTPException
import yaml

from zeta4s.api.app import (
    _REDACTED,
    _emit_operation_report,
    _normalize_zeta4s_run,
    _sanitize_operation_report_for_persistence,
    _sync_run_metadata_from_airflow_summary,
)
from zeta4s.project.step_graph import ScheduleConfig
from zeta4s.api.services.artifact_store import artifact_root, record_artifact_metadata
from zeta4s.api.services import registration_store, run_store
from zeta4s.metastore.backends.clickhouse import (
    ClickHouseArtifactRepository,
    ClickHouseBackendRegistryRepository,
    ClickHouseDeploymentRepository,
    ClickHouseMetastoreAdapter,
    ClickHouseOperationReportRepository,
    ClickHouseRunMetadataRepository,
    ClickHouseSecretRepository,
    ClickHouseStepExecutionRepository,
    ClickHouseStepEventRepository,
    ClickHouseStepOutputBindingRepository,
    ClickHouseStepStateRepository,
)
from zeta4s.metastore.contracts import DeploymentRegistration
from zeta4s.metastore.scheduler_snapshot import (
    load_scheduler_snapshot,
    publish_scheduler_snapshot,
    scheduler_last_good_snapshot_path,
)
from zeta4s.core import ExecutionContext, RunResult, StepExecutionState, StepOutputBinding
from zeta4s.runtime.metastore_reporter import MetastoreRunReporter
from zeta4s.runtime.project_metadata import (
    ExtractHistoryEvent,
    record_extract_history,
)


class _FakeEmptyOperator:
    def __init__(self, *, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs


class _FakePythonOperator:
    def __init__(self, *, task_id: str, python_callable=None, op_kwargs=None, **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}
        self.kwargs = kwargs


class _FakeShortCircuitOperator(_FakePythonOperator):
    pass


class _FakeDAG:
    def __init__(self, *, dag_id: str, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def _install_airflow_stub() -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")
    empty = types.ModuleType("airflow.providers.standard.operators.empty")
    sdk = types.ModuleType("airflow.sdk")
    python.PythonOperator = _FakePythonOperator
    python.ShortCircuitOperator = _FakeShortCircuitOperator
    empty.EmptyOperator = _FakeEmptyOperator
    sdk.DAG = _FakeDAG
    sdk.get_current_context = lambda: {}
    sdk.get_parsing_context = lambda: types.SimpleNamespace(dag_id=None)
    sys.modules.setdefault("airflow", airflow)
    sys.modules.setdefault("airflow.sdk", sdk)
    sys.modules.setdefault("airflow.providers", providers)
    sys.modules.setdefault("airflow.providers.standard", standard)
    sys.modules.setdefault("airflow.providers.standard.operators", operators)
    sys.modules.setdefault("airflow.providers.standard.operators.python", python)
    sys.modules.setdefault("airflow.providers.standard.operators.empty", empty)


class MetastoreFoundationTest(unittest.TestCase):
    def test_clickhouse_bootstrap_creates_secret_table(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        fake_clickhouse_connect = types.SimpleNamespace(get_client=lambda **kwargs: client)

        with patch.dict(sys.modules, {"clickhouse_connect": fake_clickhouse_connect}):
            ClickHouseMetastoreAdapter(database="zeta4s_test").bootstrap()

        ddl = "\n".join(client.commands)
        self.assertIn("CREATE TABLE IF NOT EXISTS secret", ddl)
        self.assertIn("secret_key String", ddl)
        self.assertIn("version UInt64", ddl)
        self.assertIn("ciphertext String", ddl)
        self.assertIn("algorithm String", ddl)
        self.assertIn("key_id Nullable(String)", ddl)
        self.assertIn("ORDER BY (secret_key, version)", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS step_output_binding", ddl)
        self.assertIn("ORDER BY (project_id, job_id, run_id, step_id, output_name)", ddl)
        self.assertIn("CREATE TABLE IF NOT EXISTS backend_registry", ddl)
        self.assertIn("ORDER BY (project_id, backend_id)", ddl)
        self.assertIn("scheduler_backend String", ddl)
        self.assertIn(
            "ADD COLUMN IF NOT EXISTS scheduler_backend String",
            ddl,
        )
        self.assertIn(
            "CHECK scheduler_backend IN ('airflow', 'prefect')",
            ddl,
        )
        self.assertNotIn("scheduler_backend String DEFAULT", ddl)

    def test_registration_store_publishes_scheduler_snapshot_from_metastore(self) -> None:
        repository = _FakeDeploymentRepository()
        adapter = _FakeMetastoreAdapter(repository)

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
            patch("zeta4s.airflow.dag_source.publish_airflow_dag_sources"),
        ):
            home = Path(tmp_dir)
            path = registration_store.upsert_project_registration(
                project_id="retail",
                artifact_id="sha256:abc",
                profile_id="prod",
                scheduler_backend="airflow",
                dags=[{"dag_id": "retail__daily", "job_id": "daily", "config": "jobs/daily.yml"}],
                home=home,
            )

            self.assertEqual(path, home / "registered" / "registered-dags.yml")
            registration_store.upsert_project_registration(
                project_id="warehouse",
                artifact_id="sha256:def",
                profile_id="prod",
                scheduler_backend="prefect",
                dags=[{"dag_id": "warehouse__daily", "job_id": "daily", "config": "jobs/daily.yml"}],
                home=home,
            )

            snapshot = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual([item["project_id"] for item in snapshot["registrations"]], ["retail"])
            self.assertEqual(snapshot["registrations"][0]["artifact_id"], "sha256:abc")
            self.assertEqual(snapshot["registrations"][0]["profile_id"], "prod")
            self.assertEqual(snapshot["registrations"][0]["scheduler_backend"], "airflow")
            self.assertEqual(snapshot["registrations"][0]["dags"][0]["dag_id"], "retail__daily")

            loaded = registration_store.load_registrations(home)
            self.assertEqual(
                [(item["project_id"], item["scheduler_backend"]) for item in loaded["registrations"]],
                [("retail", "airflow"), ("warehouse", "prefect")],
            )

            removed_path, removed = registration_store.remove_project_registration("retail", home=home)
            self.assertEqual(removed_path, path)
            self.assertEqual(removed["project_id"], "retail")
            self.assertEqual(load_scheduler_snapshot(path)["registrations"], [])

    def test_registration_store_rejects_unknown_scheduler_backend(self) -> None:
        repository = _FakeDeploymentRepository()
        adapter = _FakeMetastoreAdapter(repository)

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch(
                "zeta4s.api.services.registration_store.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            with self.assertRaisesRegex(ValueError, "unsupported scheduler backend"):
                registration_store.upsert_project_registration(
                    project_id="retail",
                    artifact_id="sha256:abc",
                    profile_id="prod",
                    scheduler_backend="unknown",
                    dags=[],
                    home=Path(tmp_dir),
                )

        self.assertEqual(repository.items, {})

    def test_scheduler_snapshot_publish_is_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            home = Path(tmp_dir)
            path = publish_scheduler_snapshot(
                registrations=[
                    {"project_id": "b_project", "artifact_id": "sha256:b", "dags": []},
                    {"project_id": "a_project", "artifact_id": "sha256:a", "dags": []},
                ],
                home=home,
            )

            data = load_scheduler_snapshot(path)
            self.assertEqual([item["project_id"] for item in data["registrations"]], ["a_project", "b_project"])

    def test_scheduler_snapshot_publish_writes_last_good_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            home = Path(tmp_dir)
            publish_scheduler_snapshot(
                registrations=[{"project_id": "retail", "artifact_id": "sha256:abc", "dags": []}],
                home=home,
            )

            data = load_scheduler_snapshot(scheduler_last_good_snapshot_path(home))
            self.assertEqual(data["registrations"][0]["project_id"], "retail")

    def test_airflow_loader_uses_last_good_snapshot_when_current_snapshot_is_invalid(self) -> None:
        _install_airflow_stub()
        from zeta4s.airflow import dynamic_loader

        with tempfile.TemporaryDirectory() as tmp_dir:
            home = Path(tmp_dir)
            current_path = home / "registered" / "registered-dags.yml"
            current_path.parent.mkdir(parents=True)
            current_path.write_text("- invalid\n", encoding="utf-8")
            last_good_path = scheduler_last_good_snapshot_path(home)
            publish_scheduler_snapshot(
                registrations=[{"project_id": "retail", "artifact_id": "sha256:abc", "dags": []}],
                home=home,
            )
            current_path.write_text("- invalid\n", encoding="utf-8")

            with patch.object(dynamic_loader, "LAST_GOOD_SCHEDULER_SNAPSHOT_FILE", last_good_path):
                registrations = list(dynamic_loader._iter_registered_items(current_path))

            self.assertEqual(registrations[0]["project_id"], "retail")

    def test_clickhouse_registration_reads_latest_project_state_by_revision(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "active_project",
                    "sha256:active",
                    "prod",
                    "airflow",
                    "2026-07-07T00:00:00+00:00",
                    '[{"dag_id": "active_project__daily"}]',
                    "active",
                )
            ]
        )
        repository = ClickHouseDeploymentRepository(_FakeClickHouseAdapter(client))

        registrations = repository.list_active()

        self.assertEqual([item.project_id for item in registrations], ["active_project"])
        self.assertEqual([item.profile_id for item in registrations], ["prod"])
        self.assertEqual([item.scheduler_backend for item in registrations], ["airflow"])
        self.assertIn("argMax", client.queries[0])
        self.assertIn("revision", client.queries[0])
        self.assertIn("tuple(revision, updated_at)", client.queries[0])
        self.assertIn("WHERE tupleElement(latest, 6) = 'active'", client.queries[0])

    def test_clickhouse_registration_writes_revision_and_updated_at(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "retail",
                    "sha256:abc",
                    "prod",
                    "airflow",
                    "2026-07-07T00:00:00+00:00",
                    '[{"dag_id": "retail__daily"}]',
                    "active",
                )
            ]
        )
        repository = ClickHouseDeploymentRepository(_FakeClickHouseAdapter(client))

        repository.upsert_active(
            project_id="retail",
            artifact_id="sha256:def",
            profile_id="prod",
            scheduler_backend="prefect",
            dags=[{"dag_id": "retail__daily"}],
        )
        repository.remove_active("retail")

        self.assertEqual(client.insert_column_names[0][-2:], ["revision", "updated_at"])
        self.assertEqual(client.insert_rows[0][0][3], "prefect")
        self.assertEqual(client.insert_rows[0][0][6], "active")
        self.assertEqual(client.insert_column_names[1][-2:], ["revision", "updated_at"])
        self.assertEqual(client.insert_rows[1][0][3], "airflow")
        self.assertEqual(client.insert_rows[1][0][6], "removed")

    def test_clickhouse_artifact_repository_records_storage_metadata(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseArtifactRepository(_FakeClickHouseAdapter(client))

        artifact = repository.upsert_artifact(
            artifact_id="sha256:abc",
            project_id="retail",
            storage_uri="/var/lib/zeta4s/artifacts/sha256-abc",
            runtime_connections=[{"conn_id": "analytics_clickhouse"}],
            dags=[{"dag_id": "retail__daily"}],
        )

        self.assertEqual(artifact.artifact_id, "sha256:abc")
        self.assertEqual(
            client.insert_column_names[0],
            [
                "artifact_id",
                "project_id",
                "storage_uri",
                "runtime_connections_json",
                "dags_json",
                "created_at",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][2], "/var/lib/zeta4s/artifacts/sha256-abc")
        self.assertIn('"conn_id": "analytics_clickhouse"', client.insert_rows[0][0][3])
        self.assertIsInstance(client.insert_rows[0][0][6], int)

    def test_artifact_metadata_record_uses_metastore_without_artifact_yaml(self) -> None:
        repository = _FakeArtifactRepository()
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), artifact_repository=repository)

        with (
            tempfile.TemporaryDirectory() as tmp_dir,
            patch(
                "zeta4s.api.services.artifact_store.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            home = Path(tmp_dir)
            record_artifact_metadata(
                artifact_id="sha256:abc",
                project_id="retail",
                runtime_connections=[{"conn_id": "analytics_clickhouse"}],
                dags=[{"dag_id": "retail__daily"}],
                home=home,
            )
            expected_storage_uri = str(artifact_root(home, "sha256:abc"))
            self.assertFalse((artifact_root(home, "sha256:abc") / "artifact.yml").exists())

        self.assertEqual(repository.upserts[0]["artifact_id"], "sha256:abc")
        self.assertEqual(repository.upserts[0]["project_id"], "retail")
        self.assertEqual(repository.upserts[0]["storage_uri"], expected_storage_uri)

    def test_clickhouse_artifact_repository_reads_runtime_connections_metadata(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "sha256:abc",
                    "retail",
                    "/var/lib/zeta4s/artifacts/sha256-abc",
                    '[{"conn_id": "analytics_clickhouse"}]',
                    '[{"dag_id": "retail__daily"}]',
                    "2026-07-09T00:00:00+00:00",
                )
            ]
        )
        repository = ClickHouseArtifactRepository(_FakeClickHouseAdapter(client))

        artifact = repository.get_artifact("sha256:abc")

        self.assertIsNotNone(artifact)
        self.assertEqual(artifact.runtime_connections[0]["conn_id"], "analytics_clickhouse")
        self.assertEqual(artifact.dags[0]["dag_id"], "retail__daily")
        self.assertIn("runtime_connections_json", client.queries[0])
        self.assertIn("ORDER BY revision DESC, updated_at DESC", client.queries[0])

    def test_clickhouse_operation_report_repository_records_report_json(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseOperationReportRepository(_FakeClickHouseAdapter(client))

        repository.save_report(
            {
                "operation_id": "op_1",
                "command": "z4s api deploy",
                "project_id": "retail",
                "status": "passed",
                "summary": {"ok": True},
            }
        )

        self.assertEqual(
            client.insert_column_names[0],
            [
                "report_id",
                "command",
                "project_id",
                "status",
                "report_json",
                "revision",
                "created_at",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][0], "op_1")
        self.assertIn('"summary": {"ok": true}', client.insert_rows[0][0][4])
        self.assertIsInstance(client.insert_rows[0][0][5], int)

    def test_operation_report_persistence_sanitizes_secret_bearing_fields(self) -> None:
        report = {
            "operation_id": "op_export",
            "command": "z4s api deploy",
            "project_id": "retail",
            "status": "passed",
            "summary": {
                "connection": {
                    "login": "metastore",
                    "password": "plain-secret",
                    "extra": {"token": "api-token", "region": "local"},
                }
            },
        }

        sanitized = _sanitize_operation_report_for_persistence(report)

        self.assertEqual(sanitized["summary"]["connection"]["password"], _REDACTED)
        self.assertEqual(sanitized["summary"]["connection"]["extra"], _REDACTED)

    def test_operation_report_normalizes_pydantic_values_before_streaming(self) -> None:
        repository = types.SimpleNamespace(save_report=lambda report: None)
        adapter = types.SimpleNamespace(operation_report_repository=repository)
        report = {
            "operation_id": "op_schedule",
            "command": "z4s api deploy",
            "status": "failed",
            "issues": [
                {
                    "code": "Z4E_PROJECT_001",
                    "context": ScheduleConfig(cron="0 * * * *", timezone="Asia/Seoul"),
                }
            ],
        }

        with patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter):
            normalized = _emit_operation_report(report)

        self.assertEqual(
            normalized["issues"][0]["context"],
            {
                "cron": "0 * * * *",
                "interval_seconds": None,
                "timezone": "Asia/Seoul",
                "paused": False,
            },
        )
        json.dumps(normalized)

    def test_clickhouse_secret_repository_records_ciphertext_only(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseSecretRepository(_FakeClickHouseAdapter(client))

        repository.put_secret_version(
            secret_key="prod.analytics_clickhouse.password",
            version=1,
            ciphertext="ciphertext",
            algorithm="AESGCM256",
            key_id="master-key-v1",
            status="active",
        )

        self.assertEqual(
            client.insert_column_names[0],
            [
                "secret_key",
                "version",
                "ciphertext",
                "algorithm",
                "key_id",
                "status",
                "created_at",
                "rotated_at",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][0], "prod.analytics_clickhouse.password")
        self.assertEqual(client.insert_rows[0][0][2], "ciphertext")
        self.assertEqual(client.insert_rows[0][0][3], "AESGCM256")

    def test_clickhouse_secret_repository_lists_metadata_without_ciphertext(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "prod.analytics_clickhouse.password",
                    1,
                    "AESGCM256",
                    "master-key-v1",
                    "active",
                    "2026-07-08T00:00:00+00:00",
                    None,
                )
            ]
        )
        repository = ClickHouseSecretRepository(_FakeClickHouseAdapter(client))

        metadata = repository.list_secret_metadata()

        self.assertEqual(
            metadata,
            [
                {
                    "secret_key": "prod.analytics_clickhouse.password",
                    "version": 1,
                    "algorithm": "AESGCM256",
                    "key_id": "master-key-v1",
                    "status": "active",
                    "created_at": "2026-07-08T00:00:00+00:00",
                    "rotated_at": None,
                }
            ],
        )
        self.assertNotIn("ciphertext", metadata[0])
        query = client.queries[0]
        self.assertIn("argMax", query)
        self.assertIn("GROUP BY secret_key, version", query)
        self.assertIn("ORDER BY secret_key ASC, version DESC", query)

    def test_clickhouse_secret_repository_reads_latest_row_before_active_status_check(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "prod.analytics_clickhouse.password",
                    1,
                    "ciphertext",
                    "AESGCM256",
                    "master-key-v1",
                    "revoked",
                    "2026-07-08T00:00:00+00:00",
                    "2026-07-08T01:00:00+00:00",
                )
            ]
        )
        repository = ClickHouseSecretRepository(_FakeClickHouseAdapter(client))

        active = repository.get_active_secret("prod.analytics_clickhouse.password")

        self.assertIsNone(active)
        query = client.queries[0]
        self.assertIn("argMax", query)
        self.assertIn("WHERE secret_key = {secret_key:String}", query)
        self.assertIn("GROUP BY secret_key, version", query)
        self.assertIn("WHERE status = 'active'", query)

    def test_clickhouse_secret_repository_preserves_active_newer_version_when_older_version_changes(self) -> None:
        client = _FakeClickHouseClient(
            rows=[
                (
                    "prod.analytics_clickhouse.password",
                    2,
                    "ciphertext-v2",
                    "AESGCM256",
                    "master-key-v1",
                    "active",
                    "2026-07-08T00:00:00+00:00",
                    None,
                )
            ]
        )
        repository = ClickHouseSecretRepository(_FakeClickHouseAdapter(client))

        active = repository.get_active_secret("prod.analytics_clickhouse.password")

        self.assertIsNotNone(active)
        self.assertEqual(active["version"], 2)
        self.assertEqual(active["ciphertext"], "ciphertext-v2")
        query = client.queries[0]
        self.assertIn("GROUP BY secret_key, version", query)
        self.assertIn("WHERE status = 'active'", query)
        self.assertIn("ORDER BY version DESC", query)

    def test_clickhouse_run_metadata_repository_creates_run(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseRunMetadataRepository(_FakeClickHouseAdapter(client))

        repository.create_run(
            {
                "run_id": "retail__daily__20260707T000000Z__abc12345",
                "scheduler_run_id": "retail__daily__20260707T000000Z__abc12345",
                "artifact_id": "sha256:abc",
                "project": "retail",
                "job_id": "daily",
                "created_at": "2026-07-07T00:00:00+00:00",
            }
        )

        self.assertEqual(
            client.insert_column_names[0],
            [
                "run_id",
                "project_id",
                "job_id",
                "artifact_id",
                "scheduler_run_id",
                "created_at",
                "run_json",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][0], "retail__daily__20260707T000000Z__abc12345")
        self.assertIn('"artifact_id": "sha256:abc"', client.insert_rows[0][0][6])
        self.assertIsInstance(client.insert_rows[0][0][7], int)

    def test_clickhouse_run_metadata_repository_reads_and_lists_runs(self) -> None:
        run_json = '{"run_id": "retail__daily__20260707T000000Z__abc12345", "project_id": "retail", "job_id": "daily"}'
        get_client = _FakeClickHouseClient(rows=[(run_json,)])
        get_repository = ClickHouseRunMetadataRepository(_FakeClickHouseAdapter(get_client))
        list_client = _FakeClickHouseClient(rows=[(run_json,)])
        list_repository = ClickHouseRunMetadataRepository(_FakeClickHouseAdapter(list_client))

        self.assertEqual(get_repository.get_run("retail__daily__20260707T000000Z__abc12345")["project_id"], "retail")
        self.assertEqual(list_repository.list_runs(project_id="retail", job_id="daily")[0]["job_id"], "daily")
        self.assertIn("WHERE run_id = {run_id:String}", get_client.queries[0])
        self.assertIn("ORDER BY revision DESC, updated_at DESC", get_client.queries[0])
        self.assertIn("SELECT tupleElement(latest, 1) AS run_json", list_client.queries[0])
        self.assertIn("tuple(revision, updated_at)", list_client.queries[0])
        self.assertIn("tuple(run_json, project_id, job_id, created_at, updated_at)", list_client.queries[0])

    def test_clickhouse_run_metadata_repository_updates_run_by_new_revision(self) -> None:
        run_json = (
            '{"run_id": "run_1", "project_id": "retail", "job_id": "daily", '
            '"status": "running", "created_at": "2026-07-07T00:00:00+00:00"}'
        )
        client = _FakeClickHouseClient(rows=[(run_json,)])
        repository = ClickHouseRunMetadataRepository(_FakeClickHouseAdapter(client))

        repository.update_run("run_1", {"status": "succeeded", "finished_at": "2026-07-07T00:01:00+00:00"})

        self.assertEqual(
            client.insert_column_names[0],
            [
                "run_id",
                "project_id",
                "job_id",
                "artifact_id",
                "scheduler_run_id",
                "created_at",
                "run_json",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][0], "run_1")
        self.assertIn('"status": "succeeded"', client.insert_rows[0][0][6])
        self.assertIn('"finished_at": "2026-07-07T00:01:00+00:00"', client.insert_rows[0][0][6])

    def test_run_store_uses_metastore_run_metadata_repository(self) -> None:
        repository = _FakeRunMetadataRepository()
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), run_metadata_repository=repository)
        run = {"run_id": "run_1", "project_id": "retail", "job_id": "daily"}

        with patch("zeta4s.api.services.run_store.metastore_adapter_factory", return_value=adapter):
            run_store.create_run(run)
            self.assertEqual(run_store.get_run("run_1"), run)
            self.assertEqual(run_store.list_runs(project_id="retail", job_id="daily"), [run])

    def test_metastore_run_reporter_records_run_lifecycle(self) -> None:
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository())
        context = ExecutionContext(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            profile="dev",
        )
        result = RunResult(
            job_id="daily",
            state=StepExecutionState.SUCCEEDED,
            steps=(),
            terminal_step_ids=("done",),
        )

        with patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter):
            reporter = MetastoreRunReporter()
            reporter.run_started(context)
            reporter.run_succeeded(context, result)

        self.assertEqual(adapter.run_metadata_repository.runs["run_1"]["project_id"], "retail")
        self.assertEqual(adapter.run_metadata_repository.runs["run_1"]["status"], "succeeded")
        self.assertIn("finished_at", adapter.run_metadata_repository.runs["run_1"])
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["run_started", "run_succeeded"],
        )
        self.assertEqual(adapter.step_event_repository.events[1]["event"]["terminal_step_ids"], ["done"])

    def test_metastore_run_reporter_records_failed_and_skipped_run_events(self) -> None:
        for state, method_name, event_type in (
            (StepExecutionState.FAILED, "run_failed", "run_failed"),
            (StepExecutionState.SKIPPED, "run_skipped", "run_skipped"),
        ):
            with self.subTest(state=state):
                adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository())
                context = ExecutionContext(
                    project_id="retail",
                    job_id="daily",
                    run_id=f"run_{state.value}",
                    profile="dev",
                )
                result = RunResult(
                    job_id="daily",
                    state=state,
                    steps=(),
                    terminal_step_ids=("done",),
                )

                reporter = MetastoreRunReporter(adapter=adapter)
                reporter.run_started(context)
                getattr(reporter, method_name)(context, result)

                run = adapter.run_metadata_repository.runs[context.run_id]
                self.assertEqual(run["status"], state.value)
                self.assertEqual(
                    [event["event_type"] for event in adapter.step_event_repository.events],
                    ["run_started", event_type],
                )
                self.assertEqual(adapter.step_event_repository.events[1]["status"], state.value)

    def test_metastore_run_reporter_records_terminal_outputs_in_run_result_event(self) -> None:
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository())
        context = ExecutionContext(
            project_id="retail",
            job_id="daily",
            run_id="run_terminal_outputs",
            profile="dev",
        )
        result = RunResult(
            job_id="daily",
            state=StepExecutionState.SUCCEEDED,
            steps=(),
            terminal_step_ids=("count_orders",),
            terminal_outputs={
                "count_orders": {
                    "row_count": StepOutputBinding(
                        step_id="count_orders",
                        output_name="row_count",
                        kind="scalar",
                        value=8,
                        ref={"kind": "scalar", "type": "int"},
                    )
                }
            },
        )

        reporter = MetastoreRunReporter(adapter=adapter)
        reporter.run_started(context)
        reporter.run_succeeded(context, result)

        event = adapter.step_event_repository.events[1]["event"]
        self.assertEqual(
            event["terminal_outputs"],
            {
                "count_orders": {
                    "row_count": {
                        "step_id": "count_orders",
                        "output_name": "row_count",
                        "kind": "scalar",
                        "value": 8,
                        "ref": {"kind": "scalar", "type": "int"},
                    }
                }
            },
        )

    def test_metastore_run_reporter_records_output_event_timestamp(self) -> None:
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository())
        context = ExecutionContext(
            project_id="retail",
            job_id="daily",
            run_id="run_output_event",
            profile="dev",
        )
        binding = StepOutputBinding(
            step_id="count_orders",
            output_name="row_count",
            kind="scalar",
            value=8,
            ref={"kind": "scalar", "type": "int"},
        )

        reporter = MetastoreRunReporter(adapter=adapter)
        reporter.step_output_produced(context, binding)

        event = adapter.step_event_repository.events[0]
        self.assertEqual(event["event_type"], "step_output_produced")
        self.assertIsNotNone(event["created_at"])
        self.assertIn("+00:00", event["created_at"])

    def test_airflow_note_sync_updates_run_metadata_from_dag_summary(self) -> None:
        repository = _FakeRunMetadataRepository()
        repository.create_run(
            {
                "run_id": "run_1",
                "project_id": "retail",
                "job_id": "daily",
                "status": "running",
            }
        )
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), run_metadata_repository=repository)

        with patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter):
            _sync_run_metadata_from_airflow_summary(
                "run_1",
                {
                    "dag_summary": {
                        "status": "success",
                        "started_at": "2026-07-10T00:00:00Z",
                        "ended_at": "2026-07-10T00:01:00Z",
                        "tasks": {"total": 2, "success": 2, "failed": 0, "skipped": 0},
                    }
                },
                dag_id="retail__daily",
                airflow_run_id="airflow_run_1",
            )

        self.assertEqual(repository.runs["run_1"]["status"], "succeeded")
        self.assertEqual(repository.runs["run_1"]["state"], "succeeded")
        self.assertEqual(repository.runs["run_1"]["started_at"], "2026-07-10T00:00:00Z")
        self.assertEqual(repository.runs["run_1"]["ended_at"], "2026-07-10T00:01:00Z")
        self.assertEqual(repository.runs["run_1"]["finished_at"], "2026-07-10T00:01:00Z")
        self.assertEqual(repository.runs["run_1"]["dag_summary"]["tasks"]["success"], 2)

    def test_airflow_note_sync_creates_missing_scheduled_run_metadata(self) -> None:
        repository = _FakeRunMetadataRepository()
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), run_metadata_repository=repository)

        with patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter):
            _sync_run_metadata_from_airflow_summary(
                "scheduled__2026-07-10",
                {
                    "dag_summary": {
                        "status": "failed",
                        "started_at": "2026-07-10T00:00:00Z",
                        "ended_at": "2026-07-10T00:03:00Z",
                    }
                },
                dag_id="retail__daily",
                airflow_run_id="scheduled__2026-07-10",
            )

        run = repository.runs["scheduled__2026-07-10"]
        self.assertEqual(run["project_id"], "retail")
        self.assertEqual(run["job_id"], "daily")
        self.assertEqual(run["adapter_metadata"]["native_job_id"], "retail__daily")
        self.assertEqual(run["scheduler_run_id"], "scheduled__2026-07-10")
        self.assertEqual(run["scheduler"], "airflow")
        self.assertEqual(run["source"], "scheduler")
        self.assertEqual(run["status"], "failed")
        self.assertEqual(run["state"], "failed")
        self.assertEqual(run["created_at"], "2026-07-10T00:00:00Z")
        self.assertEqual(run["finished_at"], "2026-07-10T00:03:00Z")

    def test_airflow_note_sync_does_not_finish_run_for_non_terminal_dag_summary(self) -> None:
        repository = _FakeRunMetadataRepository()
        repository.create_run(
            {
                "run_id": "run_1",
                "project_id": "retail",
                "job_id": "daily",
                "status": "running",
            }
        )
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), run_metadata_repository=repository)

        with patch("zeta4s.api.app.metastore_adapter_factory", return_value=adapter):
            _sync_run_metadata_from_airflow_summary("run_1", {"dag_summary": {"status": "running"}})

        self.assertEqual(repository.runs["run_1"]["status"], "running")

    def test_run_metadata_without_dag_id_uses_project_id_and_job_id(self) -> None:
        run = _normalize_zeta4s_run(
            {
                "run_id": "retail__daily__20260707T000000Z__abc12345",
                "project_id": "retail",
                "job_id": "daily",
            }
        )

        self.assertEqual(run["dag_id"], "retail__daily")
        self.assertEqual(run["project_id"], "retail")
        self.assertEqual(run["job_id"], "daily")

    def test_run_metadata_does_not_use_project_only_when_multiple_jobs_exist(self) -> None:
        run = _normalize_zeta4s_run(
            {
                "run_id": "retail__nightly__20260707T000000Z__abc12345",
                "project_id": "retail",
                "job_id": "nightly",
            },
            dag_id="retail__daily",
        )

        self.assertEqual(run["dag_id"], "retail__nightly")
        self.assertNotEqual(run["dag_id"], "retail__daily")

    def test_run_metadata_rejects_project_only_with_request_context(self) -> None:
        with self.assertRaises(HTTPException):
            _normalize_zeta4s_run(
                {
                    "run_id": "retail__unknown__20260707T000000Z__abc12345",
                    "project_id": "retail",
                },
                dag_id="retail__daily",
            )

    def test_clickhouse_step_execution_repository_records_attempt_state(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseStepExecutionRepository(_FakeClickHouseAdapter(client))

        repository.record_execution(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            step_id="build_mart",
            task_id="build_mart__orders",
            step_type="dbt.run",
            attempt=2,
            status="running",
            started_at="2026-07-07T00:00:00+00:00",
            metadata={"dag_id": "retail__daily"},
        )

        self.assertEqual(
            client.insert_column_names[0],
            [
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "task_id",
                "step_type",
                "attempt",
                "status",
                "started_at",
                "ended_at",
                "metadata_json",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][3], "build_mart")
        self.assertEqual(client.insert_rows[0][0][4], "build_mart__orders")
        self.assertEqual(client.insert_rows[0][0][6], 2)
        self.assertIn('"dag_id": "retail__daily"', client.insert_rows[0][0][10])
        self.assertIsInstance(client.insert_rows[0][0][11], int)

    def test_clickhouse_step_execution_repository_lists_by_revision_then_updated_at(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseStepExecutionRepository(_FakeClickHouseAdapter(client))

        repository.list_executions(project_id="retail", job_id="daily", run_id="run_1")

        self.assertIn("tuple(revision, updated_at)", client.queries[0])
        self.assertIn(
            "tuple(step_type, status, started_at, ended_at, metadata_json, revision, updated_at)", client.queries[0]
        )

    def test_clickhouse_step_state_repository_upserts_json_state(self) -> None:
        client = _FakeClickHouseClient(rows=[])
        repository = ClickHouseStepStateRepository(_FakeClickHouseAdapter(client))

        repository.upsert_state(
            project_id="retail",
            job_id="daily",
            step_id="extract_orders",
            state_key="watermark",
            state_value={"updated_at": "2026-07-07T00:00:00+00:00"},
            state_type="watermark",
            run_id="run_1",
        )

        self.assertEqual(
            client.insert_column_names[0],
            [
                "project_id",
                "job_id",
                "step_id",
                "state_key",
                "state_type",
                "state_value_json",
                "run_id",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(client.insert_rows[0][0][2], "extract_orders")
        self.assertEqual(client.insert_rows[0][0][3], "watermark")
        self.assertIn('"updated_at": "2026-07-07T00:00:00+00:00"', client.insert_rows[0][0][5])
        self.assertEqual(client.insert_rows[0][0][6], "run_1")
        self.assertIsInstance(client.insert_rows[0][0][7], int)

    def test_clickhouse_step_state_repository_reads_latest_state(self) -> None:
        row = (
            "retail",
            "daily",
            "extract_orders",
            "watermark",
            "watermark",
            '{"updated_at": "2026-07-07T00:00:00+00:00"}',
            "run_2",
            200,
            "2026-07-07T00:01:00+00:00",
        )
        get_client = _FakeClickHouseClient(rows=[row])
        get_repository = ClickHouseStepStateRepository(_FakeClickHouseAdapter(get_client))
        list_client = _FakeClickHouseClient(rows=[row])
        list_repository = ClickHouseStepStateRepository(_FakeClickHouseAdapter(list_client))

        state = get_repository.get_state(
            project_id="retail",
            job_id="daily",
            step_id="extract_orders",
            state_key="watermark",
        )
        states = list_repository.list_states(project_id="retail", job_id="daily", step_id="extract_orders")

        self.assertEqual(state["state_value"]["updated_at"], "2026-07-07T00:00:00+00:00")
        self.assertEqual(states[0]["run_id"], "run_2")
        self.assertIn("ORDER BY revision DESC, updated_at DESC", get_client.queries[0])
        self.assertIn("tuple(revision, updated_at)", list_client.queries[0])
        self.assertIn(
            "({job_id:Nullable(String)} IS NULL OR job_id = {job_id:Nullable(String)})", list_client.queries[0]
        )
        self.assertIn("tuple(state_type, state_value_json, run_id, revision, updated_at)", list_client.queries[0])

    def test_clickhouse_step_event_repository_records_and_lists_events(self) -> None:
        row = (
            "retail",
            "daily",
            "run_1",
            "extract_orders",
            "extract_orders__read",
            "extract_history",
            "success",
            '{"loaded_rows": 2, "output_name": "orders_raw"}',
            "2026-07-07T00:00:00+00:00",
            300,
            "2026-07-07T00:00:01+00:00",
        )
        client = _FakeClickHouseClient(rows=[row])
        repository = ClickHouseStepEventRepository(_FakeClickHouseAdapter(client))

        repository.record_event(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            step_id="extract_orders",
            task_id="extract_orders__read",
            event_type="extract_history",
            status="success",
            event={"loaded_rows": 2, "output_name": "orders_raw"},
            created_at="2026-07-07T00:00:00+00:00",
        )
        events = repository.list_events(project_id="retail", job_id="daily", event_type="extract_history", limit=10)

        self.assertEqual(
            client.insert_column_names[0],
            [
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "task_id",
                "event_type",
                "status",
                "event_json",
                "created_at",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(events[0]["event"]["output_name"], "orders_raw")
        self.assertEqual(events[0]["status"], "success")
        self.assertIn("FROM step_event", client.queries[0])
        self.assertIn("ORDER BY created_at DESC, revision DESC", client.queries[0])

    def test_clickhouse_step_output_binding_repository_records_and_reads_latest_binding(self) -> None:
        row = (
            "retail",
            "daily",
            "run_1",
            "extract_orders",
            "orders_rows",
            "rowset",
            '{"artifact_uri": "s3://bucket/orders.parquet", "format": "parquet"}',
            400,
            "2026-07-07T00:00:01+00:00",
        )
        get_client = _FakeClickHouseClient(rows=[row])
        get_repository = ClickHouseStepOutputBindingRepository(_FakeClickHouseAdapter(get_client))
        list_client = _FakeClickHouseClient(rows=[row])
        list_repository = ClickHouseStepOutputBindingRepository(_FakeClickHouseAdapter(list_client))

        get_repository.upsert_binding(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            step_id="extract_orders",
            output_name="orders_rows",
            output_kind="rowset",
            binding={"artifact_uri": "s3://bucket/orders.parquet", "format": "parquet"},
        )
        binding = get_repository.get_binding(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            step_id="extract_orders",
            output_name="orders_rows",
        )
        bindings = list_repository.list_bindings(project_id="retail", job_id="daily", run_id="run_1")

        self.assertEqual(
            get_client.insert_column_names[0],
            [
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "output_name",
                "output_kind",
                "binding_json",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(binding["binding"]["format"], "parquet")
        self.assertEqual(bindings[0]["output_name"], "orders_rows")
        self.assertIn("ORDER BY revision DESC, updated_at DESC", get_client.queries[0])
        self.assertIn("tuple(output_kind, binding_json, revision, updated_at)", list_client.queries[0])
        self.assertIn("tuple(revision, updated_at)", list_client.queries[0])

    def test_clickhouse_backend_registry_repository_records_and_lists_latest_backends(self) -> None:
        row = (
            "retail",
            "analytics_clickhouse",
            "clickhouse",
            '{"conn_id": "analytics_clickhouse", "database": "analytics"}',
            "active",
            500,
            "2026-07-07T00:00:01+00:00",
        )
        get_client = _FakeClickHouseClient(rows=[row])
        get_repository = ClickHouseBackendRegistryRepository(_FakeClickHouseAdapter(get_client))
        list_client = _FakeClickHouseClient(rows=[row])
        list_repository = ClickHouseBackendRegistryRepository(_FakeClickHouseAdapter(list_client))

        get_repository.upsert_backend(
            project_id="retail",
            backend_id="analytics_clickhouse",
            backend_type="clickhouse",
            backend={"conn_id": "analytics_clickhouse", "database": "analytics"},
            status="active",
        )
        backend = get_repository.get_backend(project_id="retail", backend_id="analytics_clickhouse")
        backends = list_repository.list_backends(project_id="retail", status="active")

        self.assertEqual(
            get_client.insert_column_names[0],
            [
                "project_id",
                "backend_id",
                "backend_type",
                "backend_json",
                "status",
                "revision",
                "updated_at",
            ],
        )
        self.assertEqual(backend["backend_type"], "clickhouse")
        self.assertEqual(backend["backend"]["database"], "analytics")
        self.assertEqual(backends[0]["backend_id"], "analytics_clickhouse")
        self.assertIn("ORDER BY revision DESC, updated_at DESC", get_client.queries[0])
        self.assertIn("tuple(backend_type, backend_json, status, revision, updated_at)", list_client.queries[0])
        self.assertIn(
            "WHERE ({status:Nullable(String)} IS NULL OR tupleElement(latest, 3) = {status:Nullable(String)})",
            list_client.queries[0],
        )

    def test_extract_history_helper_records_step_event(self) -> None:
        repository = _FakeStepEventRepository()
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), step_event_repository=repository)

        with patch("zeta4s.runtime.project_metadata.metastore_adapter_factory", return_value=adapter):
            record_extract_history(
                ExtractHistoryEvent(
                    project_id="retail",
                    job_id="daily",
                    run_id="run_1",
                    step_id="extract_orders",
                    task_id="extract_orders",
                    output_name="orders_rows",
                    source_kind="oracle",
                    source_conn="retail_oracle",
                    source_object="orders",
                    mode="rowset",
                    watermark_column="updated_at",
                    selected_from=None,
                    selected_to=datetime(2026, 7, 7, 0, 0),
                    loaded_rows=2,
                    status="success",
                    error_message=None,
                    started_at=datetime(2026, 7, 7, 0, 0),
                    ended_at=datetime(2026, 7, 7, 0, 1),
                )
            )

        event = repository.events[0]
        self.assertEqual(event["project_id"], "retail")
        self.assertEqual(event["job_id"], "daily")
        self.assertEqual(event["step_id"], "extract_orders")
        self.assertEqual(event["event_type"], "extract_history")
        self.assertEqual(event["event"]["output_name"], "orders_rows")
        self.assertEqual(event["event"]["selected_to"], "2026-07-07T00:00:00")

    def test_extract_history_helper_accepts_platform_identity_ids(self) -> None:
        repository = _FakeStepEventRepository()
        adapter = _FakeMetastoreAdapter(_FakeDeploymentRepository(), step_event_repository=repository)

        with patch("zeta4s.runtime.project_metadata.metastore_adapter_factory", return_value=adapter):
            record_extract_history(
                ExtractHistoryEvent(
                    project_id="my-project.dev",
                    job_id="daily-load",
                    run_id="run_1",
                    step_id="extract-orders",
                    task_id="extract-orders",
                    output_name="orders_rows",
                    source_kind="oracle",
                    source_conn="retail_oracle",
                    source_object="orders",
                    mode="rowset",
                    watermark_column="updated_at",
                    selected_from=None,
                    selected_to=datetime(2026, 7, 7, 0, 0),
                    loaded_rows=2,
                    status="success",
                    error_message=None,
                    started_at=datetime(2026, 7, 7, 0, 0),
                    ended_at=datetime(2026, 7, 7, 0, 1),
                )
            )

        self.assertEqual(repository.events[0]["project_id"], "my-project.dev")
        self.assertEqual(repository.events[0]["job_id"], "daily-load")
        self.assertEqual(repository.events[0]["step_id"], "extract-orders")


class _FakeMetastoreAdapter:
    def __init__(
        self,
        repository,
        run_metadata_repository=None,
        step_execution_repository=None,
        step_state_repository=None,
        step_event_repository=None,
        step_output_binding_repository=None,
        backend_registry_repository=None,
        artifact_repository=None,
    ):
        self.deployment_repository = repository
        self.artifact_repository = artifact_repository or _FakeArtifactRepository()
        self.operation_report_repository = _FakeOperationReportRepository()
        self.run_metadata_repository = run_metadata_repository or _FakeRunMetadataRepository()
        self.step_execution_repository = step_execution_repository or _FakeStepExecutionRepository()
        self.step_state_repository = step_state_repository or _FakeStepStateRepository()
        self.step_event_repository = step_event_repository or _FakeStepEventRepository()
        self.step_output_binding_repository = step_output_binding_repository or _FakeStepOutputBindingRepository()
        self.backend_registry_repository = backend_registry_repository or _FakeBackendRegistryRepository()

    def bootstrap(self) -> None:
        pass


class _FakeDeploymentRepository:
    def __init__(self):
        self.items: dict[str, DeploymentRegistration] = {}

    def list_active(self) -> list[DeploymentRegistration]:
        return [self.items[key] for key in sorted(self.items)]

    def upsert_active(
        self,
        *,
        project_id,
        artifact_id,
        profile_id,
        scheduler_backend,
        dags,
    ):
        item = DeploymentRegistration(
            project_id=project_id,
            artifact_id=artifact_id,
            profile_id=profile_id,
            scheduler_backend=scheduler_backend,
            registered_at="2026-07-07T00:00:00+00:00",
            dags=dags,
        )
        self.items[project_id] = item
        return item

    def remove_active(self, project_id):
        return self.items.pop(project_id, None)

    def artifact_is_active(self, artifact_id):
        return any(item.artifact_id == artifact_id for item in self.items.values())


class _FakeArtifactRepository:
    def __init__(self):
        self.upserts = []

    def upsert_artifact(self, **kwargs):
        self.upserts.append(kwargs)
        return kwargs

    def get_artifact(self, artifact_id):
        return None


class _FakeOperationReportRepository:
    def __init__(self):
        self.reports = []

    def save_report(self, report):
        self.reports.append(report)


class _FakeRunMetadataRepository:
    def __init__(self):
        self.runs = {}
        self.list_filters = []

    def create_run(self, run):
        self.runs[run["run_id"]] = run

    def update_run(self, run_id, patch):
        self.runs[run_id] = {**self.runs[run_id], **patch, "run_id": run_id}

    def get_run(self, run_id):
        return self.runs.get(run_id)

    def list_runs(self, *, project_id=None, job_id=None, limit=30):
        self.list_filters.append({"project_id": project_id, "job_id": job_id, "limit": limit})
        runs = list(self.runs.values())
        if project_id is not None:
            runs = [run for run in runs if run.get("project_id") == project_id]
        if job_id is not None:
            runs = [run for run in runs if run.get("job_id") == job_id]
        return runs[:limit]


class _FakeStepExecutionRepository:
    def __init__(self):
        self.records = []

    def record_execution(self, **kwargs):
        self.records.append(kwargs)

    def list_executions(self, **kwargs):
        return []


class _FakeStepStateRepository:
    def __init__(self):
        self.states = []

    def upsert_state(self, **kwargs):
        self.states.append(kwargs)

    def get_state(self, **kwargs):
        return None

    def list_states(self, **kwargs):
        states = list(self.states)
        project_id = kwargs.get("project_id")
        job_id = kwargs.get("job_id")
        step_id = kwargs.get("step_id")
        if project_id is not None:
            states = [state for state in states if state.get("project_id") == project_id]
        if job_id is not None:
            states = [state for state in states if state.get("job_id") == job_id]
        if step_id is not None:
            states = [state for state in states if state.get("step_id") == step_id]
        return states


class _FakeStepEventRepository:
    def __init__(self):
        self.events = []

    def record_event(self, **kwargs):
        self.events.append(kwargs)

    def list_events(self, **kwargs):
        events = list(self.events)
        project_id = kwargs.get("project_id")
        job_id = kwargs.get("job_id")
        event_type = kwargs.get("event_type")
        limit = kwargs.get("limit")
        if project_id is not None:
            events = [event for event in events if event.get("project_id") == project_id]
        if job_id is not None:
            events = [event for event in events if event.get("job_id") == job_id]
        if event_type is not None:
            events = [event for event in events if event.get("event_type") == event_type]
        return events[:limit] if limit else events


class _FakeStepOutputBindingRepository:
    def __init__(self):
        self.bindings = []

    def upsert_binding(self, **kwargs):
        self.bindings.append(kwargs)

    def get_binding(self, **kwargs):
        for binding in reversed(self.bindings):
            if all(binding.get(key) == value for key, value in kwargs.items()):
                return binding
        return None

    def list_bindings(self, **kwargs):
        bindings = list(self.bindings)
        for key, value in kwargs.items():
            if value is not None:
                bindings = [binding for binding in bindings if binding.get(key) == value]
        return bindings


class _FakeBackendRegistryRepository:
    def __init__(self):
        self.backends = []

    def upsert_backend(self, **kwargs):
        self.backends.append(kwargs)

    def get_backend(self, **kwargs):
        for backend in reversed(self.backends):
            if all(backend.get(key) == value for key, value in kwargs.items()):
                return backend
        return None

    def list_backends(self, **kwargs):
        backends = list(self.backends)
        for key, value in kwargs.items():
            if value is not None:
                backends = [backend for backend in backends if backend.get(key) == value]
        return backends


class _FakeTask:
    def __init__(self, task_id):
        self.task_id = task_id
        self.params = {}


class _FakeTaskInstance:
    def __init__(self, task_id, try_number=1):
        self.task_id = task_id
        self.try_number = try_number
        self.start_date = None
        self.end_date = None


class _FakeDagRun:
    def __init__(self, dag_id, run_id, conf=None):
        self.dag_id = dag_id
        self.run_id = run_id
        self.conf = conf or {}


class _FakeClickHouseAdapter:
    def __init__(self, client):
        self._client = client

    def bootstrap(self) -> None:
        pass

    def client(self):
        return self._client


class _FakeClickHouseClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.commands = []
        self.insert_rows = []
        self.insert_column_names = []

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql, parameters=None):
        self.queries.append(sql)
        return _FakeClickHouseResult(self.rows)

    def insert(self, table, rows, column_names):
        self.insert_rows.append(rows)
        self.insert_column_names.append(column_names)


class _FakeClickHouseResult:
    def __init__(self, rows):
        self.result_rows = rows


if __name__ == "__main__":
    unittest.main()
