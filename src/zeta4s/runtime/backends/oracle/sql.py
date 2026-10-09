"""Oracle SQL runtime backend."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from zeta4s.project.extract_sql import _mask_sql_string_literals, _strip_header_comments, mask_oracle_hints
from zeta4s.runtime.backends.oracle.client import get_oracle_conn
from zeta4s.runtime.task_result import log_task_event

_ORACLE_SQL_ALLOWED_START = (
    "select",
    "insert",
    "update",
    "delete",
    "merge",
    "call",
    "begin",
    "create",
    "drop",
    "alter",
    "truncate",
)


def load_oracle_sql(project_root: str | Path, sql_path: str) -> str:
    return normalize_oracle_sql(_load_oracle_sql_text(project_root, sql_path))


def _load_oracle_sql_text(project_root: str | Path, sql_path: str) -> str:
    root = Path(project_root).resolve()
    candidate = (root / sql_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Oracle SQL file must stay inside project root: {sql_path}") from exc
    return candidate.read_text(encoding="utf-8")


def normalize_oracle_sql(sql: str) -> str:
    statement = _strip_header_comments(sql).strip()
    if statement.endswith(";") and not statement.lower().startswith("begin"):
        statement = statement[:-1].strip()
    if not statement:
        raise ValueError("Oracle SQL is empty")
    masked = mask_oracle_hints(_mask_sql_string_literals(statement)).lower()
    if "--" in masked or "/*" in masked or "*/" in masked:
        raise ValueError("Oracle SQL comments are allowed only before the statement header")
    starts = masked.lstrip().split(None, 1)[0]
    if starts not in _ORACLE_SQL_ALLOWED_START:
        raise ValueError(
            "Oracle SQL must start with SELECT, INSERT, UPDATE, DELETE, MERGE, "
            "CALL, BEGIN, CREATE, DROP, ALTER or TRUNCATE"
        )
    return statement


def run_oracle_step(
    *,
    project_root: str,
    step: dict[str, Any],
    conn_id: str,
    step_index: int,
    context: dict[str, Any],
    job_id: str | None,
    result_stage: str,
    logger,
    normalize_native_sql: Callable[[str], str],
    load_native_sql_text: Callable[[str | Path, str], str],
    bind_params: Callable[[dict[str, Any], dict[str, Any], str | None], dict[str, Any]],
    connections: dict[str, Any] | None = None,
) -> int:
    step_type = step.get("type")
    sql = oracle_step_sql(
        project_root=project_root,
        step=step,
        normalize_native_sql=normalize_native_sql,
        load_native_sql_text=load_native_sql_text,
    )
    log_task_event(
        logger,
        f"{result_stage}.step",
        context=context,
        step_index=step_index,
        step_type=step_type,
        name=step.get("name"),
        engine="oracle",
    )
    params = bind_params(step, context, sql)
    ora_conn = get_oracle_conn(conn_id, connections=connections)
    try:
        cursor = ora_conn.cursor()
        try:
            cursor.execute(sql, params)
            if step_type == "check":
                row = cursor.fetchone()
                if not _coerce_bool_output(row[0] if row else None):
                    raise RuntimeError(
                        "native check failed: "
                        f"{step.get('name') or step_index}, value={row[0] if row else None} "
                        "(expected true or 1 for success)"
                    )
            affected = int(cursor.rowcount or 0)
            ora_conn.commit()
            return max(affected, 0)
        finally:
            cursor.close()
    finally:
        ora_conn.close()


def run_oracle_scalar_step(
    *,
    project_root: str,
    step: dict[str, Any],
    conn_id: str,
    context: dict[str, Any],
    normalize_native_sql: Callable[[str], str],
    load_native_sql_text: Callable[[str | Path, str], str],
    bind_params: Callable[[dict[str, Any], dict[str, Any], str | None], dict[str, Any]],
    connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sql = oracle_step_sql(
        project_root=project_root,
        step=step,
        normalize_native_sql=normalize_native_sql,
        load_native_sql_text=load_native_sql_text,
    )
    params = bind_params(step, context, sql)
    output_specs = step.get("outputs") or {}
    if not isinstance(output_specs, dict) or not output_specs:
        raise ValueError("sql.scalar step requires outputs mapping")
    ora_conn = get_oracle_conn(conn_id, connections=connections)
    try:
        cursor = ora_conn.cursor()
        try:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"sql.scalar returned no rows: {step.get('name')}")
            outputs = {
                name: _scalar_output_value(row, name, spec)
                for name, spec in output_specs.items()
                if isinstance(spec, dict)
            }
            missing = sorted(name for name in output_specs if name not in outputs)
            if missing:
                raise ValueError(f"sql.scalar outputs must be mappings: {', '.join(missing)}")
            return outputs
        finally:
            cursor.close()
    finally:
        ora_conn.close()


def oracle_step_sql(
    *,
    project_root: str,
    step: dict[str, Any],
    normalize_native_sql: Callable[[str], str],
    load_native_sql_text: Callable[[str | Path, str], str],
) -> str:
    step_type = step.get("type")
    if step_type == "call":
        call = step.get("call")
        if not call:
            raise ValueError("native call step requires call")
        return f"BEGIN {call}; END;"
    if step.get("query"):
        if step_type == "sql":
            sql = _load_oracle_sql_text(project_root, step["query"])
            return normalize_oracle_sql(sql)
        sql = load_native_sql_text(project_root, step["query"])
        return normalize_native_sql(sql)
    if step.get("sql"):
        if step_type == "sql":
            return normalize_oracle_sql(step["sql"])
        return normalize_native_sql(step["sql"])
    raise ValueError(f"native {step_type} step requires query or sql")


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
