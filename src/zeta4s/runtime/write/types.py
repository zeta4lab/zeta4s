"""Write target type contracts for deploy and task execution."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import pyarrow as pa


ORACLE_DECIMAL_MAX_PRECISION = 38
_TYPE_HEAD_RE = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9]*)\((?P<inner>.*)\)$")


@dataclass(frozen=True)
class WriteTypeIssue:
    code: str
    severity: str
    message: str
    step: str
    job: str | None = None
    write: str | None = None
    model: str | None = None
    target_type: str | None = None
    target_table: str | None = None
    column: str | None = None
    clickhouse_type: str | None = None
    arrow_type: str | None = None
    reason: str | None = None
    suggestion: str | None = None

    def to_report_issue(self) -> dict[str, Any]:
        details = {
            key: value
            for key, value in {
                "job": self.job,
                "write": self.write,
                "model": self.model,
                "target_type": self.target_type,
                "target_table": self.target_table,
                "column": self.column,
                "clickhouse_type": self.clickhouse_type,
                "arrow_type": self.arrow_type,
                "reason": self.reason,
                "suggestion": self.suggestion,
            }.items()
            if value is not None
        }
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "step": self.step,
            **details,
            "details": details,
        }


def validate_clickhouse_type_for_oracle(
    *,
    column: str,
    clickhouse_type: str,
    job: str | None = None,
    write: str | None = None,
    model: str | None = None,
    target_table: str | None = None,
    step: str = "target_schema_validate",
) -> WriteTypeIssue | None:
    normalized = normalize_clickhouse_type(clickhouse_type)
    base = unwrap_clickhouse_type(normalized)
    family = clickhouse_type_family(base)
    common = {
        "step": step,
        "job": job,
        "write": write,
        "model": model,
        "target_type": "oracle",
        "target_table": target_table,
        "column": column,
        "clickhouse_type": clickhouse_type,
    }
    if family in {"Array", "Map", "Tuple", "Nested", "Object", "JSON"}:
        return _issue(
            code="Z4E_WRITE_TYPE_COMPLEX_UNSUPPORTED",
            message="Oracle write does not accept ClickHouse complex type.",
            reason=f"{family} type cannot be bound as an Oracle scalar column.",
            suggestion="Convert it in the dbt model to a scalar String/Number/DateTime column that Oracle can store.",
            **common,
        )
    return None


def validate_arrow_schema_for_oracle(
    schema: pa.Schema,
    *,
    columns: list[str],
    job: str | None = None,
    write: str | None = None,
    model: str | None = None,
    target_table: str | None = None,
    step: str = "write_arrow_schema",
) -> list[WriteTypeIssue]:
    by_name = {field.name: field for field in schema}
    issues: list[WriteTypeIssue] = []
    for column in columns:
        field = by_name.get(column)
        if field is None:
            issues.append(
                _issue(
                    code="Z4E_WRITE_ARROW_COLUMN_MISSING",
                    message="Write Arrow schema is missing a configured column.",
                    reason=f"pyarrow.Schema has no column named {column}.",
                    suggestion="Align write.columns with the dbt model column names.",
                    step=step,
                    job=job,
                    write=write,
                    model=model,
                    target_type="oracle",
                    target_table=target_table,
                    column=column,
                )
            )
            continue
        issue = validate_arrow_type_for_oracle(
            column=column,
            arrow_type=field.type,
            job=job,
            write=write,
            model=model,
            target_table=target_table,
            step=step,
        )
        if issue:
            issues.append(issue)
    return issues


def validate_arrow_type_for_oracle(
    *,
    column: str,
    arrow_type: pa.DataType,
    job: str | None = None,
    write: str | None = None,
    model: str | None = None,
    target_table: str | None = None,
    step: str = "write_arrow_schema",
) -> WriteTypeIssue | None:
    common = {
        "step": step,
        "job": job,
        "write": write,
        "model": model,
        "target_type": "oracle",
        "target_table": target_table,
        "column": column,
        "arrow_type": str(arrow_type),
    }
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_date(arrow_type)
        or pa.types.is_timestamp(arrow_type)
    ):
        if pa.types.is_uint64(arrow_type):
            return _issue(
                code="Z4E_WRITE_ARROW_UINT64_UNSAFE",
                message="Oracle write does not accept Arrow uint64 without explicit cast.",
                reason="uint64 can exceed Oracle NUMBER precision used by the writer.",
                suggestion="In the dbt model, cast with toInt64OrNull() if the Int64 range is guaranteed, or explicitly convert to String.",
                **common,
            )
        return None
    if pa.types.is_decimal(arrow_type):
        precision = getattr(arrow_type, "precision", None)
        if precision is not None and precision <= ORACLE_DECIMAL_MAX_PRECISION:
            return None
        return _issue(
            code="Z4E_WRITE_ARROW_DECIMAL_PRECISION",
            message="Oracle write Arrow decimal precision is too large.",
            reason=f"Oracle write supports Decimal precision <= {ORACLE_DECIMAL_MAX_PRECISION}.",
            suggestion="In the dbt model, cast to a Decimal with precision <= 38 or explicitly convert to String.",
            **common,
        )
    return _issue(
        code="Z4E_WRITE_ARROW_TYPE_UNSUPPORTED",
        message="Oracle write Arrow type is not supported.",
        reason=f"Arrow type {arrow_type} has no Oracle write contract.",
        suggestion="Convert it in the dbt model to a scalar String/Number/DateTime column that Oracle can store.",
        **common,
    )


def normalize_clickhouse_type(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())


def unwrap_clickhouse_type(value: str) -> str:
    current = normalize_clickhouse_type(value)
    while True:
        parsed = _TYPE_HEAD_RE.match(current)
        if not parsed:
            return current
        name = parsed.group("name")
        if name not in {"Nullable", "LowCardinality"}:
            return current
        current = parsed.group("inner")


def clickhouse_type_family(value: str) -> str:
    value = unwrap_clickhouse_type(value)
    parsed = _TYPE_HEAD_RE.match(value)
    return parsed.group("name") if parsed else value


def decimal_precision(value: str) -> int | None:
    parsed = _TYPE_HEAD_RE.match(unwrap_clickhouse_type(value))
    if not parsed or not parsed.group("name").startswith("Decimal"):
        return None
    fixed_precision = {
        "Decimal32": 9,
        "Decimal64": 18,
        "Decimal128": 38,
        "Decimal256": 76,
    }.get(parsed.group("name"))
    if fixed_precision is not None:
        return fixed_precision
    args = [part.strip() for part in parsed.group("inner").split(",")]
    if not args:
        return None
    try:
        return int(args[0])
    except ValueError:
        return None


def _issue(**kwargs: Any) -> WriteTypeIssue:
    return WriteTypeIssue(severity="error", **kwargs)
