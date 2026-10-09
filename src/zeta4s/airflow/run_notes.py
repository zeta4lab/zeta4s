"""zeta4s task result 를 Airflow note 로 반영한다. zeta4s-api process 에서 돈다.

Airflow 에는 REST 로만 붙는다. 이 module 은 airflow 를 import 하지 않는다. worker 측
callback 은 `task_result_notes.py` 에 있다 — 실행 위치가 다르므로 module 을 가른다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any
import urllib.parse

from zeta4s.airflow.rest_client import AirflowRestClient
from zeta4s.runtime.task_result import format_dag_run_note, format_task_execution_note, load_task_results

logger = logging.getLogger(__name__)

ROW_METRIC_KEYS = ("input_rows", "output_rows", "success_rows", "failed_rows", "skipped_rows", "error_rows")


def _require_rest_client() -> AirflowRestClient:
    client = AirflowRestClient.from_env(os.environ)
    if client is None:
        raise RuntimeError(
            "Airflow REST API is not configured: set ZETA4S_AIRFLOW_REST_API_BASE_URL",
        )
    return client


def sync_airflow_run_notes(
    *,
    dag_id: str,
    airflow_run_id: str,
    result_run_id: str,
    home: str | Path | None = None,
) -> dict[str, Any]:
    """Airflow 실행 통계와 task result artifact 를 Airflow note 로 합친다."""
    results = _task_results_by_task_id(result_run_id, home)
    client = _require_rest_client()
    dag_path = urllib.parse.quote(dag_id, safe="")
    run_path = urllib.parse.quote(airflow_run_id, safe="")

    # map 되지 않은 task 만 note 를 단다. metastore 경로의 map_index == -1 필터와 같다.
    task_instances = [
        row
        for row in client.collect(
            f"/api/v2/dags/{dag_path}/dagRuns/{run_path}/taskInstances",
            items_key="task_instances",
            query={},
        )
        if row.get("map_index") == -1
    ]
    task_instances.sort(key=lambda row: str(row.get("task_id") or ""))

    task_notes_updated = 0
    for task_instance in task_instances:
        task_id = str(task_instance.get("task_id") or "")
        note = format_task_execution_note(_task_execution_summary(task_instance), results.get(task_id))
        _patch_note(
            client,
            f"/api/v2/dags/{dag_path}/dagRuns/{run_path}/taskInstances/{urllib.parse.quote(task_id, safe='')}",
            note,
        )
        task_notes_updated += 1

    dag_run = client.get(f"/api/v2/dags/{dag_path}/dagRuns/{run_path}")
    dag_summary = _dag_run_summary(dag_run, task_instances, results) if dag_run else {}
    dag_note_updated = False
    if dag_run:
        _patch_note(client, f"/api/v2/dags/{dag_path}/dagRuns/{run_path}", format_dag_run_note(dag_summary))
        dag_note_updated = True

    return {
        "task_notes_updated": task_notes_updated,
        "dag_note_updated": dag_note_updated,
        "result_count": len(results),
        "dag_summary": dag_summary,
    }


def _patch_note(client: AirflowRestClient, path: str, note: str) -> None:
    """`update_mask` 를 반드시 준다.

    `DAGRunPatchBody` 와 `PatchTaskInstanceBody` 는 note 말고도 optional field 를 갖는다.
    mask 없이 보내면 body 에 없는 field 까지 기본값으로 덮어쓴다. TaskInstance PATCH 는
    mapped task 를 한 번에 다루므로 응답이 단일 객체가 아니라 collection 이다. note 반영에는
    응답이 필요 없으므로 읽지 않는다.
    """
    client.patch(path, body={"note": note}, query={"update_mask": ["note"]})


def _task_results_by_task_id(run_id: str, home: str | Path | None) -> dict[str, dict[str, Any]]:
    root = Path(home) if home is not None else None
    return {
        str(result.get("task_id")): result
        for result in load_task_results(run_id, root)
        if isinstance(result, dict) and result.get("task_id")
    }


def _task_execution_summary(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": str(task.get("task_id") or ""),
        "state": str(task.get("state") or ""),
        "try_number": task.get("try_number"),
        "started_at": _isoformat(task.get("start_date")),
        "ended_at": _isoformat(task.get("end_date")),
        "duration_seconds": _optional_numeric_metric(task.get("duration")),
    }


def _dag_run_summary(
    dag_run: dict[str, Any], task_instances: list[dict[str, Any]], results: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    task_counts = {
        "total": len(task_instances),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "with_result": len(results),
    }
    row_totals = dict.fromkeys(ROW_METRIC_KEYS, 0)
    failed_tasks: list[dict[str, Any]] = []

    task_states: list[str] = []
    task_end_dates: list[datetime] = []
    for task in task_instances:
        task_id = str(task.get("task_id") or "")
        state = str(task.get("state") or "")
        if state:
            task_states.append(state)
        end_date = _parse_datetime(task.get("end_date"))
        if end_date is not None:
            task_end_dates.append(end_date)
        if state == "success":
            task_counts["success"] += 1
        elif state in {"failed", "upstream_failed", "up_for_retry"}:
            task_counts["failed"] += 1
        elif state == "skipped":
            task_counts["skipped"] += 1

        result = results.get(task_id) or {}
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        for key in ROW_METRIC_KEYS:
            row_totals[key] += _numeric_metric(metrics.get(key))
        if state in {"failed", "upstream_failed", "up_for_retry"}:
            failed_tasks.append({"task_id": task_id, "state": state, "message": _error_message(result) or state})

    start_at = dag_run.get("start_date") or dag_run.get("queued_at")
    end_at = dag_run.get("end_date") or _latest_datetime(task_end_dates)
    return {
        "status": _effective_dag_status(str(dag_run.get("state") or ""), task_states),
        "started_at": _isoformat(start_at),
        "ended_at": _isoformat(end_at),
        "duration_seconds": _duration_seconds(start_at, end_at),
        "tasks": task_counts,
        "metrics": row_totals,
        "failed_tasks": failed_tasks,
    }


def _numeric_metric(value: Any) -> int | float:
    return value if isinstance(value, (int, float)) else 0


def _optional_numeric_metric(value: Any) -> int | float | None:
    return value if isinstance(value, (int, float)) else None


def _error_message(result: dict[str, Any]) -> str | None:
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    message = error.get("message")
    return str(message) if message else None


def _parse_datetime(value: Any) -> datetime | None:
    """REST 는 `2026-07-15T15:40:58.887331Z` 형태의 ISO 문자열을 준다.

    경계에서 파싱하지 않으면 `_duration_seconds` 가 조용히 None 이 된다.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _isoformat(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _duration_seconds(start: Any, end: Any) -> float | None:
    start_at = _parse_datetime(start)
    end_at = _parse_datetime(end)
    if start_at is None or end_at is None:
        return None
    duration = end_at.astimezone(timezone.utc) - start_at.astimezone(timezone.utc)
    return round(max(duration.total_seconds(), 0.0), 6)


def _latest_datetime(values: list[datetime]) -> datetime | None:
    if not values:
        return None
    return max(value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc) for value in values)


def _effective_dag_status(dag_state: str, task_states: list[str]) -> str:
    if dag_state in {"success", "failed"}:
        return dag_state
    if not task_states:
        return dag_state or "-"
    terminal_states = {"success", "failed", "skipped", "upstream_failed", "removed"}
    if any(state not in terminal_states for state in task_states):
        return dag_state or "-"
    if any(state in {"failed", "upstream_failed"} for state in task_states):
        return "failed"
    return "success"
