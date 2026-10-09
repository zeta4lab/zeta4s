"""Runtime task result payload and artifact helpers."""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)

AIRFLOW_NOTE_CONTENT_LIMIT = 1000


def log_task_event(
    event_logger: logging.Logger,
    event: str,
    *,
    context: dict[str, Any] | None = None,
    **fields: Any,
) -> None:
    """Emit one-line operational event logs for Airflow task logs."""
    payload = {
        "event": event,
        "task_id": _task_id(context),
        "dag_id": _dag_id(context),
        "run_id": _run_id(context),
        **fields,
    }
    parts = [f"{key}={_format_log_value(value)}" for key, value in payload.items() if value is not None]
    event_logger.info("zeta4s event: %s", " ".join(parts))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def task_result(
    *,
    task_id: str | None,
    stage: str,
    status: str,
    started_at: str,
    ended_at: str | None = None,
    duration_seconds: float | None = None,
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "stage": stage,
        "status": status,
        "started_at": started_at,
        "ended_at": ended_at or utc_now_iso(),
        "duration_seconds": duration_seconds,
        "metrics": _normalize_metrics(metrics or {}),
        "details": details or {},
        "error": error or {"message": None, "type": None},
    }


def success_result(
    *,
    stage: str,
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    started_at: str | None = None,
    start_monotonic: float | None = None,
) -> dict[str, Any]:
    ended_at = utc_now_iso()
    return task_result(
        task_id=_task_id(context),
        stage=stage,
        status="success",
        started_at=started_at or ended_at,
        ended_at=ended_at,
        duration_seconds=_duration(start_monotonic),
        metrics=metrics,
        details=details,
    )


def failure_result(
    *,
    stage: str,
    exc: BaseException,
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    started_at: str | None = None,
    start_monotonic: float | None = None,
) -> dict[str, Any]:
    ended_at = utc_now_iso()
    return task_result(
        task_id=_task_id(context),
        stage=stage,
        status="failed",
        started_at=started_at or ended_at,
        ended_at=ended_at,
        duration_seconds=_duration(start_monotonic),
        metrics=metrics,
        details=details,
        error={"message": str(exc), "type": type(exc).__name__},
    )


@contextmanager
def result_context(stage: str, context: dict[str, Any] | None = None) -> Iterator[tuple[str, float]]:
    started_at = utc_now_iso()
    start_monotonic = time.monotonic()
    log_task_event(logger, f"{stage}.start", context=context, stage=stage, started_at=started_at)
    try:
        yield started_at, start_monotonic
    except Exception as exc:
        result = failure_result(
            stage=stage, exc=exc, context=context, started_at=started_at, start_monotonic=start_monotonic
        )
        save_task_result(result, context=context)
        logger.info("zeta4s task result: %s", json.dumps(result, ensure_ascii=False, sort_keys=True))
        raise


def save_task_result(
    result: dict[str, Any], context: dict[str, Any] | None = None, home: Path | None = None
) -> Path | None:
    run_id = _run_id(context)
    task_id = str(result.get("task_id") or _task_id(context) or "")
    if not run_id or not task_id:
        return None
    root = home or _default_home()
    path = root / "runs" / run_id / "tasks" / f"{_safe_name(task_id)}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return path


def load_task_results(run_id: str, home: Path | None = None) -> list[dict[str, Any]]:
    root = (home or _default_home()) / "runs" / run_id / "tasks"
    if not root.exists():
        return []
    results: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, dict):
            results.append(data)
    return results


def record_success(
    *,
    stage: str,
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    started_at: str | None = None,
    start_monotonic: float | None = None,
) -> dict[str, Any]:
    result = success_result(
        stage=stage,
        metrics=metrics,
        details=details,
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )
    save_task_result(result, context=context)
    logger.info("zeta4s task result: %s", json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def format_task_note(result: dict[str, Any]) -> str:
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    error_message = error.get("message")
    lines = [
        "zeta4s task result",
        f"stage: {result.get('stage')}",
        f"status: {result.get('status')}",
        f"duration: {_format_duration(result.get('duration_seconds'))}",
        f"started: {_format_optional(result.get('started_at'))}",
        f"ended: {_format_optional(result.get('ended_at'))}",
        "",
        "rows",
        f"input: {_format_number(metrics.get('input_rows'))}",
        f"output: {_format_number(metrics.get('output_rows'))}",
        f"success: {_format_number(metrics.get('success_rows'))}",
        f"failed: {_format_number(metrics.get('failed_rows'))}",
        f"skipped: {_format_number(metrics.get('skipped_rows'))}",
        f"error: {_format_number(metrics.get('error_rows'))}",
    ]
    if error_message:
        lines.extend(["", "error", f"{_format_optional(error.get('type'))}: {_truncate(str(error_message), 500)}"])
    return _limit_note(lines)


def format_task_execution_note(task: dict[str, Any], result: dict[str, Any] | None = None) -> str:
    result = result or {}
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    error_message = error.get("message")
    lines = [
        "zeta4s task execution",
        f"task_id: {_format_optional(task.get('task_id'))}",
        f"state: {_format_optional(task.get('state'))}",
        f"duration: {_format_duration(task.get('duration_seconds', task.get('duration')))}",
        f"started: {_format_optional(task.get('started_at', task.get('start_date')))}",
        f"ended: {_format_optional(task.get('ended_at', task.get('end_date')))}",
        f"try: {_format_optional(task.get('try_number'))}",
        "",
        "result",
        f"available: {'yes' if result else 'no'}",
        f"stage: {_format_optional(result.get('stage'))}",
        f"status: {_format_optional(result.get('status'))}",
    ]
    if metrics:
        lines.extend(
            [
                "",
                "rows",
                f"input: {_format_number(metrics.get('input_rows'))}",
                f"output: {_format_number(metrics.get('output_rows'))}",
                f"success: {_format_number(metrics.get('success_rows'))}",
                f"failed: {_format_number(metrics.get('failed_rows'))}",
                f"skipped: {_format_number(metrics.get('skipped_rows'))}",
                f"error: {_format_number(metrics.get('error_rows'))}",
            ]
        )
    if error_message:
        lines.extend(["", "error", f"{_format_optional(error.get('type'))}: {_truncate(str(error_message), 500)}"])
    return _limit_note(lines)


def format_dag_run_note(summary: dict[str, Any]) -> str:
    metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else {}
    tasks = summary.get("tasks") if isinstance(summary.get("tasks"), dict) else {}
    failed_tasks = summary.get("failed_tasks") if isinstance(summary.get("failed_tasks"), list) else []
    lines = [
        "zeta4s DAG result",
        f"status: {_format_optional(summary.get('status'))}",
        f"duration: {_format_duration(summary.get('duration_seconds'))}",
        f"started: {_format_optional(summary.get('started_at'))}",
        f"ended: {_format_optional(summary.get('ended_at'))}",
        "",
        "tasks",
        f"total: {_format_number(tasks.get('total'))}",
        f"success: {_format_number(tasks.get('success'))}",
        f"failed: {_format_number(tasks.get('failed'))}",
        f"skipped: {_format_number(tasks.get('skipped'))}",
        f"with_result: {_format_number(tasks.get('with_result'))}",
        "",
        "rows",
        f"input: {_format_number(metrics.get('input_rows'))}",
        f"output: {_format_number(metrics.get('output_rows'))}",
        f"success: {_format_number(metrics.get('success_rows'))}",
        f"failed: {_format_number(metrics.get('failed_rows'))}",
        f"skipped: {_format_number(metrics.get('skipped_rows'))}",
        f"error: {_format_number(metrics.get('error_rows'))}",
    ]
    if failed_tasks:
        lines.extend(["", "failed tasks"])
        for item in failed_tasks[:5]:
            if not isinstance(item, dict):
                continue
            task_id = _format_optional(item.get("task_id"))
            message = str(item.get("message") or item.get("state") or "-")
            if not _append_limited_line(lines, f"{task_id}: ", message):
                break
    return _limit_note(lines)


def _normalize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "input_rows": None,
        "output_rows": None,
        "success_rows": None,
        "failed_rows": None,
        "skipped_rows": None,
        "error_rows": None,
    }
    return {**defaults, **metrics}


def _format_optional(value: Any) -> str:
    if value is None:
        return "-"
    return str(value)


def _format_number(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    return str(value)


def _format_duration(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    seconds = max(float(value), 0.0)
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes):02d}m {remainder:06.3f}s"
    hours, minutes = divmod(int(minutes), 60)
    return f"{hours:02d}h {minutes:02d}m {remainder:06.3f}s"


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    if limit <= 0:
        return ""
    if limit <= 3:
        return value[:limit]
    return value[: limit - 3] + "..."


def _format_log_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        return "[" + ",".join(_format_log_value(item) for item in value) + "]"
    text = str(value)
    if not text:
        return "''"
    if any(ch.isspace() for ch in text):
        return json.dumps(text, ensure_ascii=False)
    return text


def _limit_note(lines: list[str], limit: int = AIRFLOW_NOTE_CONTENT_LIMIT) -> str:
    return "\n".join(lines)[:limit]


def _append_limited_line(lines: list[str], prefix: str, value: str, limit: int = AIRFLOW_NOTE_CONTENT_LIMIT) -> bool:
    current = "\n".join(lines)
    separator_length = 1 if current else 0
    remaining = limit - len(current) - separator_length
    if remaining <= 0:
        return False
    if remaining <= len(prefix):
        lines.append(_truncate(prefix, remaining))
        return False
    lines.append(f"{prefix}{_truncate(value, remaining - len(prefix))}")
    return True


def _duration(start_monotonic: float | None) -> float | None:
    if start_monotonic is None:
        return None
    return round(time.monotonic() - start_monotonic, 6)


def _task_id(context: dict[str, Any] | None) -> str | None:
    if not context:
        return None
    return str(context["task_id"]) if context.get("task_id") else None


def _dag_id(context: dict[str, Any] | None) -> str | None:
    if not context:
        return None
    return str(context["dag_id"]) if context.get("dag_id") else None


def _run_id(context: dict[str, Any] | None) -> str | None:
    if not context:
        return None
    if context.get("z4_run_id"):
        return str(context["z4_run_id"])
    return str(context["run_id"]) if context.get("run_id") else None


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value)


def _default_home() -> Path:
    return Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))
