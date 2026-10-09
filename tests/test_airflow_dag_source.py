from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from zeta4s.airflow.dag_source import render_airflow_dag_source


class AirflowDagSourceTest(unittest.TestCase):
    def test_generated_source_compiles_without_zeta4s_import(self) -> None:
        step = SimpleNamespace(
            id="extract",
            flow=SimpleNamespace(retry={"max_attempts": 2, "delay_seconds": 3}, timeout={"seconds": 30}),
            step=SimpleNamespace(when=None, join=None),
        )
        plan = SimpleNamespace(
            schedule=None,
            steps=[step],
            terminal_step_ids={"extract"},
            upstream_ids_by_step={"extract": ()},
        )
        project = SimpleNamespace(registered_at=None)
        registration = {
            "project_id": "retail",
            "artifact_id": "sha256:abc",
            "profile_id": "prod",
        }
        dag_spec = {"dag_id": "retail__daily", "job_id": "daily"}

        with (
            patch("zeta4s.airflow.dag_source.load_scheduled_plan", return_value=(plan, project)),
            patch("zeta4s.airflow.dag_source.execution_step_pool_name", return_value="retail.extract"),
        ):
            source, dag_id = render_airflow_dag_source(registration, dag_spec, home=Path("/tmp/runtime"))

        self.assertEqual(dag_id, "retail__daily")
        compile(source, "retail__daily.py", "exec")
        self.assertNotIn("from zeta4s", source)
        self.assertNotIn("import zeta4s", source)
        self.assertIn('"artifact:" + SPEC["artifact_id"]', source)
        self.assertIn("/internal/v1/runtime/steps/execute", source)
        self.assertIn("/internal/v1/runtime/runs/finalize", source)


if __name__ == "__main__":
    unittest.main()
