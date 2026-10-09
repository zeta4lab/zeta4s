"""SQL identifier validation for zeta4s-generated SQL.

The zeta4s intentionally supports only unquoted identifiers. This keeps
generated SQL deterministic across Oracle and ClickHouse and blocks accidental
SQL fragments from YAML config values.
"""

from __future__ import annotations

import re
from collections.abc import Iterable


_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
_RAW_ASSET_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_FORBIDDEN_PREDICATE_RE = re.compile(
    r";|--|/\*|\*/|"
    r"\b(select|union)\b|"
    r":[A-Za-z][A-Za-z0-9_]*",
    re.IGNORECASE,
)
INTERNAL_TABLE_PREFIX = "__zeta4s_"


def validate_sql_identifier(value: str, label: str) -> str:
    """Validate a single unquoted SQL identifier and return it unchanged."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty SQL identifier")
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{label} must be an unquoted SQL identifier (letters, digits, _, $, #; starts with a letter): {value!r}"
        )
    return value


def validate_raw_asset_identifier(value: str, label: str) -> str:
    """Validate a zeta4s-owned raw asset identifier and return it unchanged."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty raw asset identifier")
    if not _RAW_ASSET_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{label} must be a raw asset identifier "
            f"(letters, digits, _; starts with a letter): {value!r}. "
            "Put Oracle special object names (G$, V$, GV$, ...) in an extract.queries[] SQL file "
            "and use a safe asset name for extract.queries[].name."
        )
    return value


def validate_table_identifier(value: str, label: str, *, max_parts: int = 3) -> str:
    """Validate a dot-qualified table identifier.

    Oracle source/target tables may be schema-qualified. ClickHouse runtime
    table names should pass max_parts=1 where the zeta4s supplies database
    context separately.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty SQL table identifier")

    parts = value.split(".")
    if len(parts) > max_parts:
        raise ValueError(f"{label} has too many qualifier parts: {value!r} (max_parts={max_parts})")

    for idx, part in enumerate(parts):
        validate_sql_identifier(part, f"{label}[{idx}]")
    return value


def validate_user_table_name(table_name: str, label: str) -> str:
    """Validate a user-controlled table name against zeta4s-reserved names."""
    if table_name.startswith(INTERNAL_TABLE_PREFIX):
        raise ValueError(f"{label} must not start with reserved prefix {INTERNAL_TABLE_PREFIX}")
    return table_name


def validate_sql_identifier_list(values: Iterable[str], label: str) -> list[str]:
    """Validate a non-empty list of SQL identifiers."""
    result = list(values or [])
    if not result:
        raise ValueError(f"{label} must contain at least one SQL identifier")
    for idx, value in enumerate(result):
        validate_sql_identifier(value, f"{label}[{idx}]")
    return result


def validate_static_predicate(value: str, label: str) -> str:
    """Validate a static source-row predicate fragment.

    This deliberately accepts only a single WHERE-condition fragment, not a full
    SQL statement or a templated query. Transform SQL belongs in dbt models.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty SQL predicate")
    predicate = value.strip()
    if _FORBIDDEN_PREDICATE_RE.search(predicate):
        raise ValueError(
            f"{label} must be a static WHERE predicate only; SQL statements, "
            f"comments, bind placeholders, and subqueries/unions are not allowed: {value!r}"
        )
    return predicate
