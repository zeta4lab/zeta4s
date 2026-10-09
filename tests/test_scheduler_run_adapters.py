from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from zeta4s.airflow.run_adapter import AirflowRunAdapter
from zeta4s.api.services.scheduler_runs import adapter_for_registration
from zeta4s.prefect.run_adapter import PrefectRunAdapter


class SchedulerRunAdapterTest(unittest.TestCase):
    def test_dispatches_from_active_registration(self) -> None:
        self.assertIsInstance(adapter_for_registration({"scheduler_backend": "airflow"}), AirflowRunAdapter)
        self.assertIsInstance(adapter_for_registration({"scheduler_backend": "prefect"}), PrefectRunAdapter)
        with self.assertRaisesRegex(ValueError, "unsupported scheduler"):
            adapter_for_registration({})

    def test_airflow_create_projects_parameters_to_native_conf(self) -> None:
        row = {
            "run_id": "run-1",
            "state": "queued",
            "queued_at": "2026-07-17T00:00:00Z",
            "conf": {"region": "kr", "z4_run_id": "run-1"},
        }
        with patch("zeta4s.airflow.run_adapter.runs.trigger_dag_run", return_value=row) as trigger:
            snapshot = AirflowRunAdapter({}).create_run(
                project_id="retail", job_id="daily", run_id="run-1", parameters={"region": "kr"}
            )

        trigger.assert_called_once_with("retail__daily", "run-1", {"region": "kr", "z4_run_id": "run-1"})
        self.assertEqual(snapshot.state, "queued")
        self.assertEqual(snapshot.parameters, {"region": "kr"})
        self.assertEqual(snapshot.adapter_metadata["native_job_id"], "retail__daily")

    def test_prefect_create_projects_parameters_and_keeps_canonical_run_id(self) -> None:
        native = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            state=SimpleNamespace(type=SimpleNamespace(value="PENDING")),
            start_time=None,
            end_time=None,
            created=SimpleNamespace(isoformat=lambda: "2026-07-17T00:00:00+00:00"),
            total_run_time=timedelta(0),
            deployment_id="00000000-0000-0000-0000-000000000002",
        )
        adapter = PrefectRunAdapter({"profile_id": "prod"})
        with (
            patch(
                "zeta4s.prefect.run_adapter.trigger_prefect_deployment",
                return_value=str(native.id),
            ) as trigger,
            patch("zeta4s.prefect.run_adapter.read_prefect_flow_run", return_value=native),
        ):
            snapshot = adapter.create_run(
                project_id="retail", job_id="daily", run_id="canonical-run", parameters={"region": "kr"}
            )

        trigger.assert_called_once_with(
            "zeta4s-scheduled-job/retail__daily__prod",
            parameters={"region": "kr"},
            idempotency_key="canonical-run",
            run_id="canonical-run",
        )
        self.assertEqual(snapshot.run_id, "canonical-run")
        self.assertEqual(snapshot.scheduler_run_id, str(native.id))
        self.assertEqual(snapshot.state, "queued")


if __name__ == "__main__":
    unittest.main()
