"""note 요약 집계의 동작 계약.

입력은 Airflow REST dict 다. 기대값은 **집계 결과를 바이트 단위로** 고정한다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from zeta4s.airflow.run_notes import (
    _dag_run_summary,
    _effective_dag_status,
    _task_execution_summary,
)


def _ti(
    task_id: str,
    state: str,
    *,
    try_number: int = 1,
    start: str | None = "2026-07-15T15:40:58.950242Z",
    end: str | None = "2026-07-15T15:40:59.359801Z",
    duration: float | None = 0.409559,
    map_index: int = -1,
) -> dict:
    """실측한 REST task instance row 모양이다."""
    return {
        "task_id": task_id,
        "state": state,
        "try_number": try_number,
        "start_date": start,
        "end_date": end,
        "duration": duration,
        "map_index": map_index,
    }


class TaskExecutionSummaryTest(unittest.TestCase):
    def test_summary_keeps_rest_timestamps_verbatim(self) -> None:
        summary = _task_execution_summary(_ti("delete_sales_index", "success"))

        self.assertEqual(
            summary,
            {
                "task_id": "delete_sales_index",
                "state": "success",
                "try_number": 1,
                "started_at": "2026-07-15T15:40:58.950242Z",
                "ended_at": "2026-07-15T15:40:59.359801Z",
                "duration_seconds": 0.409559,
            },
        )

    def test_missing_fields_become_empty_not_crash(self) -> None:
        summary = _task_execution_summary({"task_id": "t"})

        self.assertEqual(summary["task_id"], "t")
        self.assertEqual(summary["state"], "")
        self.assertIsNone(summary["try_number"])
        self.assertIsNone(summary["started_at"])
        self.assertIsNone(summary["duration_seconds"])


class DagRunSummaryTest(unittest.TestCase):
    def test_counts_states_and_totals_row_metrics(self) -> None:
        dag_run = {
            "state": "failed",
            "start_date": "2026-07-15T15:40:58.887331Z",
            "end_date": "2026-07-15T15:41:16.712540Z",
            "queued_at": "2026-07-15T15:40:58.740582Z",
        }
        tasks = [
            _ti("a", "success"),
            _ti("b", "failed"),
            _ti("c", "upstream_failed"),
            _ti("d", "skipped"),
        ]
        results = {
            "a": {"metrics": {"input_rows": 10, "output_rows": 10, "success_rows": 12}},
            "b": {"error": {"message": "boom"}},
        }

        summary = _dag_run_summary(dag_run, tasks, results)

        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["tasks"], {"total": 4, "success": 1, "failed": 2, "skipped": 1, "with_result": 2})
        self.assertEqual(summary["metrics"]["input_rows"], 10)
        self.assertEqual(summary["metrics"]["output_rows"], 10)
        self.assertEqual(summary["metrics"]["success_rows"], 12)
        self.assertEqual(summary["metrics"]["failed_rows"], 0)
        self.assertEqual(
            summary["failed_tasks"],
            [
                {"task_id": "b", "state": "failed", "message": "boom"},
                {"task_id": "c", "state": "upstream_failed", "message": "upstream_failed"},
            ],
        )

    def test_duration_is_computed_from_iso_strings(self) -> None:
        """REST 는 datetime 이 아니라 ISO 문자열을 준다.

        문자열을 그대로 빼면 duration 이 조용히 None 이 된다. 경계에서 파싱해야 한다.
        """
        dag_run = {
            "state": "success",
            "start_date": "2026-07-15T15:40:58.887331Z",
            "end_date": "2026-07-15T15:41:16.712540Z",
        }
        summary = _dag_run_summary(dag_run, [_ti("a", "success")], {})

        self.assertAlmostEqual(summary["duration_seconds"], 17.825209, places=5)
        self.assertEqual(summary["started_at"], "2026-07-15T15:40:58.887331Z")
        self.assertEqual(summary["ended_at"], "2026-07-15T15:41:16.712540Z")

    def test_falls_back_to_queued_at_and_latest_task_end(self) -> None:
        dag_run = {"state": "running", "start_date": None, "end_date": None, "queued_at": "2026-07-15T15:40:00Z"}
        tasks = [
            _ti("a", "success", end="2026-07-15T15:40:10Z"),
            _ti("b", "success", end="2026-07-15T15:40:30Z"),
        ]

        summary = _dag_run_summary(dag_run, tasks, {})

        self.assertEqual(summary["started_at"], "2026-07-15T15:40:00Z")
        self.assertEqual(summary["ended_at"], "2026-07-15T15:40:30Z")
        self.assertAlmostEqual(summary["duration_seconds"], 30.0, places=5)


class EffectiveDagStatusTest(unittest.TestCase):
    def test_terminal_dag_state_wins(self) -> None:
        self.assertEqual(_effective_dag_status("success", ["running"]), "success")
        self.assertEqual(_effective_dag_status("failed", ["success"]), "failed")

    def test_non_terminal_task_keeps_dag_state(self) -> None:
        self.assertEqual(_effective_dag_status("running", ["success", "running"]), "running")
        self.assertEqual(_effective_dag_status("", []), "-")

    def test_all_terminal_tasks_resolve_status(self) -> None:
        self.assertEqual(_effective_dag_status("running", ["success", "skipped"]), "success")
        self.assertEqual(_effective_dag_status("running", ["success", "upstream_failed"]), "failed")


class IsoBoundaryTest(unittest.TestCase):
    def test_naive_datetime_is_treated_as_utc(self) -> None:
        from zeta4s.airflow.run_notes import _isoformat

        naive = datetime(2026, 7, 15, 15, 40, 58)
        self.assertEqual(_isoformat(naive), "2026-07-15T15:40:58Z")
        aware = datetime(2026, 7, 15, 15, 40, 58, tzinfo=timezone.utc)
        self.assertEqual(_isoformat(aware), "2026-07-15T15:40:58Z")


if __name__ == "__main__":
    unittest.main()
