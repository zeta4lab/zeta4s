"""SQL step runtime callables."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from zeta4s.project.extract_sql import _mask_sql_string_literals, _strip_header_comments, mask_oracle_hints
from zeta4s.runtime.connections import resolve_runtime_connection
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.task_result import log_task_event, record_success, result_context

logger = logging.getLogger(__name__)

_ORACLE_ALLOWED_START = ("select", "insert", "update", "delete", "merge", "call", "begin")
_ORACLE_FORBIDDEN_DDL = ("create", "drop", "alter", "truncate", "grant", "revoke")


def load_native_sql(project_root: str | Path, sql_path: str) -> str:
    return _normalize_native_sql(_load_native_sql_text(project_root, sql_path))


def _load_native_sql_text(project_root: str | Path, sql_path: str) -> str:
    root = Path(project_root).resolve()
    candidate = (root / sql_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"native SQL file must stay inside project root: {sql_path}") from exc
    return candidate.read_text(encoding="utf-8")


def _normalize_native_sql(sql: str) -> str:
    statement = _strip_header_comments(sql).strip()
    if statement.endswith(";") and not statement.lower().startswith("begin"):
        statement = statement[:-1].strip()
    if not statement:
        raise ValueError("native SQL file is empty")
    masked = mask_oracle_hints(_mask_sql_string_literals(statement)).lower()
    if "--" in masked or "/*" in masked or "*/" in masked:
        raise ValueError("native SQL comments are allowed only before the statement header")
    starts = masked.lstrip().split(None, 1)[0]
    if starts not in _ORACLE_ALLOWED_START:
        raise ValueError("native SQL must start with SELECT, INSERT, UPDATE, DELETE, MERGE, CALL or BEGIN")
    for token in _ORACLE_FORBIDDEN_DDL:
        if f"{token} " in masked or f"{token}\n" in masked or masked.startswith(token):
            raise ValueError(f"native SQL contains forbidden DDL token: {token}")
    return statement


def run_native_steps(
    *,
    project_root: str,
    steps: list[dict[str, Any]],
    default_conn: str | None = None,
    default_engine: str = "oracle",
    job_id: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    context = _current_context(kwargs)
    connection_types = _connection_types(kwargs)
    connections = kwargs.get("connections")
    stage = _native_result_stage(steps)
    with result_context(stage, context) as (started_at, start_monotonic):
        log_task_event(
            logger,
            f"{stage}.plan",
            context=context,
            job_id=job_id,
            step_count=len(steps),
            default_engine=default_engine,
            result_contract=stage,
        )
        affected_rows = 0
        for index, step in enumerate(steps, start=1):
            affected_rows += _run_native_step(
                project_root=project_root,
                step=step,
                default_conn=default_conn,
                default_engine=default_engine,
                connection_types=connection_types,
                connections=connections,
                step_index=index,
                context=context,
                job_id=job_id,
                result_stage=stage,
            )
    return record_success(
        stage=stage,
        metrics={
            "input_rows": None,
            "output_rows": affected_rows,
            "success_rows": len(steps),
            "failed_rows": 0,
            "skipped_rows": 0,
            "error_rows": 0,
        },
        details={"job_id": job_id, "steps": len(steps), "result_contract": stage},
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _native_result_stage(steps: list[dict[str, Any]]) -> str:
    types = {str(step.get("type") or "") for step in steps if isinstance(step, dict)}
    if types and types <= {"check"}:
        return "sql_check"
    return "sql_transform"


def run_sql_scalar(
    *,
    project_root: str,
    step: dict[str, Any],
    default_conn: str | None = None,
    default_engine: str = "oracle",
    job_id: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    context = _current_context(kwargs)
    connection_types = _connection_types(kwargs)
    connections = kwargs.get("connections")
    with result_context("sql_scalar", context) as (started_at, start_monotonic):
        log_task_event(
            logger,
            "sql_scalar.plan",
            context=context,
            job_id=job_id,
            step=step.get("name"),
            default_engine=default_engine,
        )
        outputs = _run_scalar_step(
            project_root=project_root,
            step=step,
            default_conn=default_conn,
            default_engine=default_engine,
            connection_types=connection_types,
            connections=connections,
            context=context,
        )
    return record_success(
        stage="sql_scalar",
        metrics={
            "input_rows": None,
            "output_rows": 1,
            "success_rows": 1,
            "failed_rows": 0,
            "skipped_rows": 0,
            "error_rows": 0,
        },
        details={"job_id": job_id, "outputs": outputs},
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _run_scalar_step(
    *,
    project_root: str,
    step: dict[str, Any],
    default_conn: str | None,
    default_engine: str,
    connection_types: dict[str, str] | None,
    connections: dict[str, Any] | None,
    context: dict[str, Any],
) -> dict[str, Any]:
    conn = step.get("conn") or default_conn
    engine = _resolve_native_engine(
        step=step,
        default_engine=default_engine,
        conn=conn,
        connection_types=connection_types,
        connections=connections,
    )
    if not conn:
        raise ValueError(f"sql.scalar step requires conn: step={step.get('name')}")
    if engine == "clickhouse":
        from zeta4s.runtime.backends.clickhouse.sql import run_clickhouse_scalar_step

        return run_clickhouse_scalar_step(
            project_root=project_root,
            step=step,
            conn_id=conn,
            context=context,
            bind_params=_oracle_bind_params,
            connections=connections,
        )
    if engine == "oracle":
        from zeta4s.runtime.backends.oracle.sql import run_oracle_scalar_step

        return run_oracle_scalar_step(
            project_root=project_root,
            step=step,
            conn_id=conn,
            context=context,
            normalize_native_sql=_normalize_native_sql,
            load_native_sql_text=_load_native_sql_text,
            bind_params=_oracle_bind_params,
            connections=connections,
        )
    raise ValueError(f"unsupported sql.scalar connection type: {engine}")


def _run_native_step(
    *,
    project_root: str,
    step: dict[str, Any],
    default_conn: str | None,
    default_engine: str,
    connection_types: dict[str, str] | None,
    connections: dict[str, Any] | None,
    step_index: int,
    context: dict[str, Any],
    job_id: str | None = None,
    result_stage: str = "sql_transform",
) -> int:
    conn = step.get("conn") or default_conn
    engine = _resolve_native_engine(
        step=step,
        default_engine=default_engine,
        conn=conn,
        connection_types=connection_types,
        connections=connections,
    )
    if not conn:
        raise ValueError(f"native step requires conn: step={step.get('name') or step_index}")
    if engine == "oracle":
        from zeta4s.runtime.backends.oracle.sql import run_oracle_step

        return run_oracle_step(
            project_root=project_root,
            step=step,
            conn_id=conn,
            step_index=step_index,
            context=context,
            job_id=job_id,
            result_stage=result_stage,
            logger=logger,
            normalize_native_sql=_normalize_native_sql,
            load_native_sql_text=_load_native_sql_text,
            bind_params=_oracle_bind_params,
            connections=connections,
        )
    if engine == "clickhouse":
        from zeta4s.runtime.backends.clickhouse.sql import run_clickhouse_step

        return run_clickhouse_step(
            project_root=project_root,
            step=step,
            conn_id=conn,
            step_index=step_index,
            context=context,
            result_stage=result_stage,
            bind_params=_oracle_bind_params,
            connections=connections,
        )
    raise ValueError(f"unsupported native step engine: {engine}")


def _resolve_native_engine(
    *,
    step: dict[str, Any],
    default_engine: str,
    conn: str | None,
    connection_types: dict[str, str] | None = None,
    connections: dict[str, Any] | None = None,
) -> str:
    if default_engine != "auto":
        return default_engine
    if not conn:
        raise ValueError(f"native step requires conn for engine resolution: step={step.get('name') or step.get('id')}")
    if connection_types and conn in connection_types:
        conn_type = str(connection_types[conn]).strip().lower()
        if conn_type in {"oracle", "clickhouse"}:
            return conn_type
        raise ValueError(f"unsupported native step connection type: conn={conn} conn_type={conn_type!r}")
    conn_type = str(resolve_runtime_connection(conn, connections=connections).conn_type or "").strip().lower()
    if conn_type in {"oracle", "clickhouse"}:
        return conn_type
    raise ValueError(f"unsupported native step connection type: conn={conn} conn_type={conn_type!r}")


def _oracle_bind_params(
    step: dict[str, Any],
    context: dict[str, Any],
    sql: str | None = None,
) -> dict[str, Any]:
    params = _runtime_context_params(context)
    for key, value in (step.get("params") or {}).items():
        if isinstance(value, str) and value.startswith("$context."):
            context_key = value.removeprefix("$context.")
            if context_key not in params:
                raise ValueError(f"native step param references unknown runtime context key: {context_key}")
            params[key] = params[context_key]
        else:
            params[key] = value
    if sql is None:
        return params
    names = _bind_placeholders(sql)
    return {key: value for key, value in params.items() if key in names}


def _bind_placeholders(sql: str) -> set[str]:
    masked = _mask_sql_string_literals(sql)
    return {match.group(1) for match in re.finditer(r":([A-Za-z][A-Za-z0-9_$#]*)", masked)}


def _runtime_context_params(context: dict[str, Any]) -> dict[str, Any]:
    z4_run_id = context.get("z4_run_id") or context.get("run_id")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    logical_date = context.get("logical_date")
    return {
        "z4_run_id": z4_run_id,
        "run_id": context.get("run_id") or z4_run_id,
        "dag_id": context.get("dag_id"),
        "task_id": context.get("task_id"),
        "try_number": context.get("try_number"),
        "logical_date": logical_date,
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "ds": context.get("ds"),
        "ts": context.get("ts"),
    }


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs, exclude_keys={"connection_types", "connections"})


def _connection_types(kwargs: dict[str, Any]) -> dict[str, str] | None:
    raw = kwargs.get("connection_types")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("connection_types must be a mapping")
    return {str(conn_id): str(conn_type).strip().lower() for conn_id, conn_type in raw.items()}
