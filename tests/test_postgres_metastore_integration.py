from __future__ import annotations

import os
import unittest

from zeta4s.metastore.backends.postgres import PostgresMetastoreAdapter, REQUIRED_TABLES
from zeta4s.metastore.contracts import StepCheckpoint


@unittest.skipUnless(
    os.environ.get("ZETA4S_TEST_POSTGRES_DSN"),
    "ZETA4S_TEST_POSTGRES_DSN is required",
)
class PostgresMetastoreIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.adapter = PostgresMetastoreAdapter(dsn=os.environ["ZETA4S_TEST_POSTGRES_DSN"])
        cls.adapter.bootstrap()

    def setUp(self) -> None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE " + ", ".join(REQUIRED_TABLES) + " RESTART IDENTITY")

    def test_control_plane_repositories_round_trip(self) -> None:
        deployment = self.adapter.deployment_repository.upsert_active(
            project_id="integration-project",
            artifact_id="sha256:integration",
            profile_id="default",
            scheduler_backend="prefect",
            dags=[{"dag_id": "integration-job"}],
        )
        self.assertEqual(self.adapter.deployment_repository.list_active(), [deployment])
        self.assertTrue(self.adapter.deployment_repository.artifact_is_active("sha256:integration"))

        artifact = self.adapter.artifact_repository.upsert_artifact(
            artifact_id="sha256:integration",
            project_id="integration-project",
            storage_uri="file:///tmp/integration.tar.gz",
            dags=[{"dag_id": "integration-job"}],
            runtime_connections=[{"conn_id": "analytics"}],
        )
        self.assertEqual(
            self.adapter.artifact_repository.get_artifact("sha256:integration"),
            artifact,
        )

        report = {
            "operation_id": "operation-1",
            "command": "deploy",
            "project_id": "integration-project",
            "status": "success",
        }
        self.adapter.operation_report_repository.save_report(report)
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT report FROM operation_report WHERE report_id = %s",
                    ("operation-1",),
                )
                self.assertEqual(cursor.fetchone()["report"], report)

        removed = self.adapter.deployment_repository.remove_active("integration-project")
        self.assertEqual(removed, deployment)
        self.assertEqual(self.adapter.deployment_repository.list_active(), [])

    def test_runtime_state_repositories_round_trip(self) -> None:
        run = {
            "run_id": "integration-run",
            "project_id": "integration-project",
            "job_id": "integration-job",
            "status": "running",
            "created_at": "2026-07-11T00:00:00+00:00",
        }
        self.adapter.run_metadata_repository.create_run(run)
        self.adapter.run_metadata_repository.update_run("integration-run", {"status": "success"})
        stored_run = self.adapter.run_metadata_repository.get_run("integration-run")
        self.assertEqual(stored_run["status"], "success")
        self.assertEqual(
            self.adapter.run_metadata_repository.list_runs(project_id="integration-project", job_id="integration-job"),
            [stored_run],
        )

        execution = {
            "project_id": "integration-project",
            "job_id": "integration-job",
            "run_id": "integration-run",
            "step_id": "extract",
            "step_type": "oracle.sql",
            "attempt": 1,
            "task_id": "extract",
        }
        self.adapter.step_execution_repository.record_execution(**execution, status="running", metadata={"rows": 0})
        self.adapter.step_execution_repository.record_execution(**execution, status="success", metadata={"rows": 10})
        executions = self.adapter.step_execution_repository.list_executions(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
        )
        self.assertEqual(len(executions), 1)
        self.assertEqual(executions[0]["metadata"], {"rows": 10})

        checkpoint = StepCheckpoint(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
            step_id="extract",
            task_id="extract",
            attempt=1,
            sequence=1,
            unit_id="rows",
            storage_uri="iceberg://zeta4s_checkpoint/integration",
            table_identifier="zeta4s_checkpoint.integration",
            snapshot_id=101,
            continuation={"offset": "opaque-1"},
            schema_fingerprint="schema-1",
            rows=10,
            bytes=1024,
            created_at="2026-07-11T00:00:30+00:00",
        )
        self.adapter.step_checkpoint_repository.append_checkpoint(checkpoint)
        self.adapter.step_checkpoint_repository.append_checkpoint(checkpoint)
        stored_checkpoint = self.adapter.step_checkpoint_repository.latest_checkpoint(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
            step_id="extract",
            task_id="extract",
            unit_id="rows",
        )
        self.assertEqual(stored_checkpoint, checkpoint)
        self.assertEqual(
            self.adapter.step_checkpoint_repository.list_checkpoints(
                project_id="integration-project",
                job_id="integration-job",
                run_id="integration-run",
                step_id="extract",
            ),
            [checkpoint],
        )

        self.adapter.step_state_repository.upsert_state(
            project_id="integration-project",
            job_id="integration-job",
            step_id="extract",
            state_key="cursor",
            state_value={"id": 42},
            state_type="json",
            run_id="integration-run",
        )
        state = self.adapter.step_state_repository.get_state(
            project_id="integration-project",
            job_id="integration-job",
            step_id="extract",
            state_key="cursor",
        )
        self.assertEqual(state["state_value"], {"id": 42})
        self.assertEqual(
            self.adapter.step_state_repository.list_states(project_id="integration-project", job_id="integration-job"),
            [state],
        )

        self.adapter.step_event_repository.record_event(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
            step_id="extract",
            task_id="extract",
            event_type="progress",
            status="success",
            event={"rows": 10},
            created_at="2026-07-11T00:01:00+00:00",
        )
        events = self.adapter.step_event_repository.list_events(
            project_id="integration-project",
            job_id="integration-job",
            event_type="progress",
        )
        self.assertEqual(events[0]["event"], {"rows": 10})
        self.assertNotIn("event_id", events[0])

        self.adapter.step_output_binding_repository.upsert_binding(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
            step_id="extract",
            output_name="rows",
            output_kind="table",
            binding={"table": "staging.events"},
        )
        binding = self.adapter.step_output_binding_repository.get_binding(
            project_id="integration-project",
            job_id="integration-job",
            run_id="integration-run",
            step_id="extract",
            output_name="rows",
        )
        self.assertEqual(binding["binding"], {"table": "staging.events"})
        self.assertEqual(
            self.adapter.step_output_binding_repository.list_bindings(
                project_id="integration-project",
                job_id="integration-job",
                run_id="integration-run",
            ),
            [binding],
        )

        self.adapter.backend_registry_repository.upsert_backend(
            project_id="integration-project",
            backend_id="warehouse",
            backend_type="postgres",
            backend={"database": "analytics"},
            status="active",
        )
        backend = self.adapter.backend_registry_repository.get_backend(
            project_id="integration-project", backend_id="warehouse"
        )
        self.assertEqual(backend["backend"], {"database": "analytics"})
        self.assertEqual(
            self.adapter.backend_registry_repository.list_backends(project_id="integration-project", status="active"),
            [backend],
        )

    def test_secret_repository_rotates_ciphertext_envelopes(self) -> None:
        repository = self.adapter.secret_repository
        repository.put_secret_version(
            secret_key="warehouse/password",
            version=1,
            ciphertext="encrypted-v1",
            algorithm="AES-256-GCM",
            key_id="integration-key",
            status="active",
        )
        repository.put_secret_version(
            secret_key="warehouse/password",
            version=2,
            ciphertext="encrypted-v2",
            algorithm="AES-256-GCM",
            key_id="integration-key",
            status="active",
        )

        active = repository.get_active_secret("warehouse/password")
        self.assertEqual(active["version"], 2)
        self.assertEqual(active["ciphertext"], "encrypted-v2")
        metadata = repository.list_secret_metadata()
        self.assertEqual([item["status"] for item in metadata], ["active", "inactive"])
        self.assertTrue(all("ciphertext" not in item for item in metadata))


if __name__ == "__main__":
    unittest.main()
