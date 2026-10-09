"""User-facing z4s message catalog keyed by stable message code."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

MESSAGES: dict[str, str] = {
    "cli.error": "Error: {message}",
    "report.more_issues": "... and {count} more issue(s)",
    "project.check.passed": "project check passed: {issues} issue(s)",
    "project.check.failed": "project check failed: {issues} issue(s)",
    "project.output_path.outside_project": "output path must be inside the project root.",
    "project.structure.missing_entries": "missing required project entries: {entries}",
    "extract.sql.empty": "extract SQL file is empty.",
    "extract.sql.multiple_statements": "extract SQL must contain exactly one SELECT/WITH statement.",
    "extract.sql.comment_position_invalid": "extract SQL comments are allowed only in the header before SELECT/WITH.",
    "extract.sql.header_block_unclosed": "extract SQL header block comment is not closed.",
    "extract.sql.statement_type_invalid": "extract SQL must start with SELECT or WITH after the header comments.",
    "extract.sql.forbidden_token": "extract SQL contains a forbidden token: {token}",
    "extract.sql.path_outside_project": "extract SQL file path must be inside the project root.",
    "project.extract_query_sql.invalid": "extract query SQL validation failed.",
}


class _SafeParams(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def render_message(
    code: str,
    params: Mapping[str, Any] | None = None,
    default: str | None = None,
) -> str:
    values = dict(params or {})
    template = MESSAGES.get(code)
    if not template:
        if default:
            return default
        encoded = json.dumps(values, ensure_ascii=False, sort_keys=True)
        return f"message not found: {code} params={encoded}"
    return template.format_map(_SafeParams(values))
