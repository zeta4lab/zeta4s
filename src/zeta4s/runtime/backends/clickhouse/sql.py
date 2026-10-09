"""ClickHouse native SQL step backend."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from zeta4s.project.extract_sql import _mask_sql_string_literals, _strip_header_comments
from zeta4s.runtime.backends.clickhouse.client import get_clickhouse_runtime_client
from zeta4s.runtime.backends.clickhouse.params import bind_clickhouse_named_params
from zeta4s.runtime.task_result import log_task_event

logger = logging.getLogger(__name__)


def run_clickhouse_step(
    *,
    project_root: str,
    step: dict[str, Any],
    conn_id: str,
    step_index: int,
    context: dict[str, Any],
    result_stage: str,
    bind_params: Callable[[dict[str, Any], dict[str, Any], str | None], dict[str, Any]],
    connections: dict[str, Any] | None = None,
) -> int:
    step_type = step.get("type")
    sql = clickhouse_step_sql(project_root, step)
    log_task_event(
        logger,
        f"{result_stage}.step",
        context=context,
        step_index=step_index,
        step_type=step_type,
        name=step.get("name"),
        engine="clickhouse",
        conn_id=conn_id,
    )
    client = get_clickhouse_runtime_client(conn_id, connections=connections)
    sql, params = bind_clickhouse_named_params(sql, bind_params(step, context, sql))
    if step_type == "check":
        row = client.query(sql, parameters=params).first_row
        if not _coerce_bool_output(row[0] if row else None):
            raise RuntimeError(
                "native check failed: "
                f"{step.get('name') or step_index}, value={row[0] if row else None} "
                "(expected true or 1 for success)"
            )
        return 0
    client.command(sql, parameters=params)
    return 0


def run_clickhouse_scalar_step(
    *,
    project_root: str,
    step: dict[str, Any],
    conn_id: str,
    context: dict[str, Any],
    bind_params: Callable[[dict[str, Any], dict[str, Any], str | None], dict[str, Any]],
    connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sql = clickhouse_step_sql(project_root, step)
    sql, params = bind_clickhouse_named_params(sql, bind_params(step, context, sql))
    output_specs = step.get("outputs") or {}
    if not isinstance(output_specs, dict) or not output_specs:
        raise ValueError("sql.scalar step requires outputs mapping")
    client = get_clickhouse_runtime_client(conn_id, connections=connections)
    row = client.query(sql, parameters=params).first_row
    if row is None:
        raise RuntimeError(f"sql.scalar returned no rows: {step.get('name')}")
    outputs = {
        name: _scalar_output_value(row, name, spec) for name, spec in output_specs.items() if isinstance(spec, dict)
    }
    missing = sorted(name for name in output_specs if name not in outputs)
    if missing:
        raise ValueError(f"sql.scalar outputs must be mappings: {', '.join(missing)}")
    return outputs


def clickhouse_step_sql(
    project_root: str,
    step: dict[str, Any],
) -> str:
    if step.get("query"):
        sql = load_clickhouse_sql(project_root, step["query"])
    elif step.get("sql"):
        sql = step["sql"]
    else:
        raise ValueError(f"native {step.get('type')} step requires query or sql")
    return normalize_clickhouse_sql(sql)


def load_clickhouse_sql(project_root: str, sql_path: str) -> str:
    root = Path(project_root).resolve()
    candidate = (root / sql_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"ClickHouse SQL file must stay inside project root: {sql_path}") from exc
    return candidate.read_text(encoding="utf-8")


def normalize_clickhouse_sql(sql: str) -> str:
    statement = _strip_header_comments(sql).strip()
    if statement.endswith(";"):
        statement = statement[:-1].strip()
    if not statement:
        raise ValueError("ClickHouse SQL is empty")
    masked = _mask_sql_string_literals(statement).lower()
    if "--" in masked or "/*" in masked or "*/" in masked:
        raise ValueError("ClickHouse SQL comments are allowed only before the statement header")
    starts = masked.lstrip().split(None, 1)[0]
    if starts not in {"select", "insert", "create", "alter", "drop", "truncate", "optimize"}:
        raise ValueError("ClickHouse SQL must start with SELECT, INSERT, CREATE, ALTER, DROP, TRUNCATE or OPTIMIZE")
    return statement


def _scalar_output_value(row, name: str, spec: dict[str, Any]) -> Any:
    column = spec.get("column", 1)
    if isinstance(column, int):
        index = column - 1
    elif isinstance(column, str) and column.isdigit():
        index = int(column) - 1
    else:
        raise ValueError(f"sql.scalar output {name} column must be 1-based integer")
    if index < 0 or index >= len(row):
        raise ValueError(f"sql.scalar output {name} column index out of range: {column}")
    return _coerce_scalar_output(row[index], spec.get("type"))


def _coerce_scalar_output(value: Any, output_type: str | None) -> Any:
    if value is None or not output_type:
        return value
    normalized = str(output_type).lower()
    if normalized == "int":
        return int(value)
    if normalized == "float":
        return float(value)
    if normalized == "bool":
        return _coerce_bool_output(value)
    if normalized == "str":
        return str(value)
    return value


def _coerce_bool_output(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if value in {0, 1}:
            return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise ValueError(f"sql.scalar bool output must be one of true, false, 1, 0: {value!r}")
