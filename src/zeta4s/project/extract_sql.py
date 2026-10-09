"""Validation helpers for project-local extract SQL files."""

from __future__ import annotations

import re
from pathlib import Path

from zeta4s.common.errors import UserFacingError

_EXTRACT_SQL_FORBIDDEN_RE = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|commit|rollback|execute|call)\b",
    re.IGNORECASE,
)


def _mask_sql_string_literals(statement: str) -> str:
    """Return SQL with single-quoted string literal contents replaced by spaces."""
    chars = list(statement)
    idx = 0
    while idx < len(chars):
        if chars[idx] != "'":
            idx += 1
            continue
        idx += 1
        while idx < len(chars):
            if chars[idx] == "'":
                if idx + 1 < len(chars) and chars[idx + 1] == "'":
                    chars[idx] = " "
                    chars[idx + 1] = " "
                    idx += 2
                    continue
                idx += 1
                break
            chars[idx] = " "
            idx += 1
    return "".join(chars)


def mask_oracle_hints(statement: str) -> str:
    """Return SQL with Oracle optimizer hint blocks replaced by spaces."""
    chars = list(statement)
    idx = 0
    while idx < len(chars):
        if idx + 2 < len(chars) and chars[idx] == "/" and chars[idx + 1] == "*" and chars[idx + 2] == "+":
            chars[idx] = " "
            chars[idx + 1] = " "
            chars[idx + 2] = " "
            idx += 3
            closed = False
            while idx + 1 < len(chars):
                if chars[idx] == "*" and chars[idx + 1] == "/":
                    chars[idx] = " "
                    chars[idx + 1] = " "
                    idx += 2
                    closed = True
                    break
                chars[idx] = " "
                idx += 1
            if not closed:
                raise UserFacingError(
                    "extract.sql.oracle_hint_unclosed",
                    default_message="extract.queries[].sql Oracle hint block is not closed",
                )
            continue
        idx += 1
    return "".join(chars)


def _strip_header_comments(sql: str) -> str:
    idx = 0
    while idx < len(sql):
        while idx < len(sql) and sql[idx].isspace():
            idx += 1
        if sql.startswith("--", idx):
            newline = sql.find("\n", idx + 2)
            if newline == -1:
                return ""
            idx = newline + 1
            continue
        if sql.startswith("/*", idx):
            end = sql.find("*/", idx + 2)
            if end == -1:
                raise UserFacingError(
                    "extract.sql.header_block_unclosed",
                    default_message="extract.queries[].sql header block comment is not closed",
                )
            idx = end + 2
            continue
        break
    return sql[idx:]


def normalize_extract_select_sql(sql: str) -> str:
    """Validate project SQL extract text and return a single SELECT body.

    Header comments are allowed before the SQL statement. Comments inside the
    executable SQL body remain forbidden to keep extract wrapping deterministic.
    """
    statement = _strip_header_comments(sql).strip()
    if statement.endswith(";"):
        statement = statement[:-1].strip()
    if not statement:
        raise UserFacingError("extract.sql.empty", default_message="extract.queries[].sql file is empty")
    masked_statement = mask_oracle_hints(_mask_sql_string_literals(statement))
    if ";" in masked_statement:
        raise UserFacingError(
            "extract.sql.multiple_statements",
            default_message="extract.queries[].sql must contain exactly one SELECT statement",
        )
    if "--" in masked_statement or "/*" in masked_statement or "*/" in masked_statement:
        raise UserFacingError(
            "extract.sql.comment_position_invalid",
            default_message="extract.queries[].sql allows comments only before the SELECT/WITH header",
        )
    if not re.match(r"^(select|with)\b", statement, re.IGNORECASE):
        raise UserFacingError(
            "extract.sql.statement_type_invalid",
            default_message="extract.queries[].sql must start with SELECT or WITH after header comments",
        )
    forbidden = _EXTRACT_SQL_FORBIDDEN_RE.search(masked_statement)
    if forbidden:
        token = forbidden.group(1)
        raise UserFacingError(
            "extract.sql.forbidden_token",
            params={"token": token},
            default_message=f"extract.queries[].sql contains forbidden token: {token}",
        )
    return statement


def load_extract_sql(project_root: str | Path, sql_path: str) -> str:
    """Read and validate a SQL extract file from inside a project root."""
    root = Path(project_root).resolve()
    candidate = (root / sql_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise UserFacingError(
            "extract.sql.path_outside_project",
            params={"sql_file": sql_path},
            default_message="extract.queries[].sql must stay inside project root",
        ) from exc
    return normalize_extract_select_sql(candidate.read_text(encoding="utf-8"))
