"""Shared dbt runtime execution and result reporting."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from zeta4s.dbt.graph import dbt_executable
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.task_result import failure_result, log_task_event, record_success, result_context, save_task_result

logger = logging.getLogger(__name__)


def run_dbt_node_with_profile(
    *,
    conn_id: str,
    conn_type: str,
    dbt_project_path: str,
    unique_id: str,
    resource_type: str,
    node_name: str,
    command: str,
    profiles_yml: str,
    **kwargs,
) -> dict[str, Any]:
    context = _current_context(kwargs)
    with result_context(_stage(resource_type), context) as (started_at, start_monotonic):
        dbt_path = Path(dbt_project_path)
        target_path = _target_path(context)
        profiles_dir = target_path / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        (profiles_dir / "profiles.yml").write_text(profiles_yml, encoding="utf-8")
        cmd = [
            dbt_executable(),
            "--log-path",
            str(target_path / "logs"),
            command,
            "--profiles-dir",
            str(profiles_dir),
            "--project-dir",
            str(dbt_path),
            "--select",
            node_name,
            "--target-path",
            str(target_path),
        ]
        log_task_event(
            logger,
            "dbt.plan",
            context=context,
            command=command,
            resource_type=resource_type,
            node_name=node_name,
            selector=node_name,
            conn_id=conn_id,
            conn_type=conn_type,
            dbt_project_path=str(dbt_path),
            target_path=str(target_path),
        )
        result = subprocess.run(cmd, cwd=str(dbt_path), text=True, capture_output=True, check=False)
        _emit_dbt_output(result)
        parsed = _parse_run_results(target_path / "run_results.json")
        metrics = _metrics_from_run_results(parsed)
        details = {
            "conn_id": conn_id,
            "conn_type": conn_type,
            "unique_id": unique_id,
            "resource_type": resource_type,
            "node_name": node_name,
            "command": command,
            "selector": node_name,
            "dbt_project_path": str(dbt_path),
            "target_path": str(target_path),
            "message": _result_message(parsed),
        }
        failed_message = None
        if not _has_results(parsed):
            failed_message = f"dbt {command} selected no nodes: {node_name}"
        elif result.returncode != 0 or metrics["failed_rows"]:
            failed_message = (
                _failure_message(parsed)
                or (result.stderr or result.stdout).strip()
                or f"dbt {command} failed: {unique_id}"
            )

    if failed_message:
        exc = RuntimeError(failed_message)
        failure = failure_result(
            stage=_stage(resource_type),
            exc=exc,
            metrics=metrics,
            details=details,
            context=context,
            started_at=started_at,
            start_monotonic=start_monotonic,
        )
        save_task_result(failure, context=context)
        logger.info("zeta4s task result: %s", json.dumps(failure, ensure_ascii=False, sort_keys=True))
        raise exc

    return record_success(
        stage=_stage(resource_type),
        metrics=metrics,
        details=details,
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _stage(resource_type: str) -> str:
    if resource_type == "model":
        return "dbt_run"
    if resource_type == "test":
        return "dbt_test"
    return f"dbt_{resource_type}"


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs, exclude_keys={"connections", "connection_types"})


def _target_path(context: dict[str, Any]) -> Path:
    home = Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))
    run_id = _run_id(context) or "adhoc"
    task_id = _target_id(context) or "dbt_task"
    path = home / "runs" / run_id / "dbt" / _safe_name(task_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_id(context: dict[str, Any]) -> str | None:
    if context.get("z4_run_id"):
        return str(context["z4_run_id"])
    if context.get("run_id"):
        return str(context["run_id"])
    return None


def _task_id(context: dict[str, Any]) -> str | None:
    if context.get("task_id"):
        return str(context["task_id"])
    return None


def _target_id(context: dict[str, Any]) -> str | None:
    value = context.get("dbt_target_id")
    if value:
        return str(value)
    return _task_id(context)


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in value)


def _emit_dbt_output(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info("[dbt stdout] %s", line)
    if result.stderr:
        for line in result.stderr.splitlines():
            logger.error("[dbt stderr] %s", line)


def _parse_run_results(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"results": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"dbt run_results.json is invalid: {path}") from e
    return payload if isinstance(payload, dict) else {"results": []}


def _metrics_from_run_results(payload: dict[str, Any]) -> dict[str, Any]:
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    output_rows = 0
    success_rows = 0
    failed_rows = 0
    skipped_rows = 0
    error_rows = 0
    for result in results:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "")
        failures = _int_or_zero(result.get("failures"))
        rows_affected = _rows_affected(result.get("adapter_response"))
        if status in {"success", "pass"}:
            success_rows += rows_affected if rows_affected is not None else 1
            if rows_affected is not None:
                output_rows += rows_affected
        elif status in {"fail", "error"}:
            failed = failures if failures else 1
            failed_rows += failed
            error_rows += failed
        elif status == "skipped":
            skipped_rows += 1
    return {
        "input_rows": None,
        "output_rows": output_rows if output_rows else None,
        "success_rows": success_rows,
        "failed_rows": failed_rows,
        "skipped_rows": skipped_rows,
        "error_rows": error_rows,
    }


def _has_results(payload: dict[str, Any]) -> bool:
    return bool(payload.get("results")) if isinstance(payload.get("results"), list) else False


def _rows_affected(adapter_response: Any) -> int | None:
    if not isinstance(adapter_response, dict):
        return None
    for key in ("rows_affected", "rows_inserted", "rows"):
        value = adapter_response.get(key)
        if value is not None:
            return _int_or_none(value)
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    return _int_or_none(value) or 0


def _result_message(payload: dict[str, Any]) -> str:
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    messages = [str(result.get("message")) for result in results if isinstance(result, dict) and result.get("message")]
    return "; ".join(messages)


def _failure_message(payload: dict[str, Any]) -> str | None:
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    for result in results:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "")
        if status in {"fail", "error"}:
            message = result.get("message")
            return str(message) if message else None
    return None
