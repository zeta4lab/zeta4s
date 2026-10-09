from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from zeta4s.airflow.dag_source import render_airflow_dag_source
from zeta4s.project.step_graph import ScheduleConfig


def _render(schedule: ScheduleConfig | None = None, *, project_timezone: str = "Asia/Seoul") -> tuple[str, str]:
    step = SimpleNamespace(
        id="extract",
        flow=SimpleNamespace(retry={"max_attempts": 2, "delay_seconds": 3}, timeout={"seconds": 30}),
        step=SimpleNamespace(when=None, join=None),
    )
    plan = SimpleNamespace(
        schedule=schedule,
        steps=[step],
        terminal_step_ids={"extract"},
        upstream_ids_by_step={"extract": ()},
    )
    project = SimpleNamespace(registered_at=None, timezone=project_timezone)
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
        return render_airflow_dag_source(registration, dag_spec, home=Path("/tmp/runtime"))


def _spec(source: str) -> dict:
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "SPEC":
            return json.loads(ast.literal_eval(node.value.args[0]))
    raise AssertionError("SPEC assignment not found")


class AirflowDagSourceTest(unittest.TestCase):
    def test_generated_source_compiles_without_zeta4s_import(self) -> None:
        source, dag_id = _render()

        self.assertEqual(dag_id, "retail__daily")
        compile(source, "retail__daily.py", "exec")
        self.assertNotIn("from zeta4s", source)
        self.assertNotIn("import zeta4s", source)
        self.assertIn('"artifact:" + SPEC["artifact_id"]', source)
        self.assertIn("/internal/v1/runtime/steps/execute", source)
        self.assertIn("/internal/v1/runtime/runs/finalize", source)

    def test_schedule_timezone_falls_back_to_project_timezone(self) -> None:
        cases = [
            (None, "Asia/Seoul", None, "Asia/Seoul"),
            (ScheduleConfig(cron="0 2 * * *"), "Asia/Seoul", "0 2 * * *", "Asia/Seoul"),
            (ScheduleConfig(cron="0 2 * * *", timezone="Europe/Berlin"), "Asia/Seoul", "0 2 * * *", "Europe/Berlin"),
            (ScheduleConfig(interval_seconds=300), "America/New_York", "@continuous:300", "America/New_York"),
        ]
        for schedule, project_timezone, expected_schedule, expected_timezone in cases:
            with self.subTest(schedule=schedule, project_timezone=project_timezone):
                spec = _spec(_render(schedule, project_timezone=project_timezone)[0])
                self.assertEqual(spec["schedule"], expected_schedule)
                self.assertEqual(spec["timezone"], expected_timezone)

    def test_generated_dag_start_date_carries_schedule_timezone(self) -> None:
        source, _ = _render(ScheduleConfig(cron="0 2 * * *"))

        self.assertIn('.astimezone(ZoneInfo(SPEC["timezone"]))', source)
        self.assertIn('"start_date": start_date', source)


if __name__ == "__main__":
    unittest.main()
