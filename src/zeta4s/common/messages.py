"""Small ko/en message catalog for user-facing z4s output."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

DEFAULT_LANG = "ko"
SUPPORTED_LANGS = {"ko", "en"}

MESSAGES: dict[str, dict[str, str]] = {
    "cli.error": {
        "ko": "오류: {message}",
        "en": "ERROR: {message}",
    },
    "report.more_issues": {
        "ko": "추가 이슈 {count}개",
        "en": "and {count} more",
    },
    "project.check.passed": {
        "ko": "project check 통과: {issues}개 이슈",
        "en": "project check passed: {issues} issues",
    },
    "project.check.failed": {
        "ko": "project check 실패: {issues}개 이슈",
        "en": "project check failed: {issues} issues",
    },
    "project.output_path.outside_project": {
        "ko": "output path는 project root 내부여야 합니다.",
        "en": "output path must be inside project root",
    },
    "project.structure.missing_entries": {
        "ko": "project 필수 항목이 없습니다: {entries}",
        "en": "missing project entries: {entries}",
    },
    "project.assets_reference.missing": {
        "ko": "assets config에 runtime reference가 없습니다: {missing}",
        "en": "assets config is missing runtime references: {missing}",
    },
    "extract.sql.empty": {
        "ko": "extract SQL 파일이 비어 있습니다.",
        "en": "extract SQL file is empty.",
    },
    "extract.sql.multiple_statements": {
        "ko": "extract SQL은 하나의 SELECT/WITH 문만 포함해야 합니다.",
        "en": "extract SQL must contain exactly one SELECT/WITH statement.",
    },
    "extract.sql.comment_position_invalid": {
        "ko": "extract SQL 주석은 SELECT/WITH 앞 header 위치에만 둘 수 있습니다.",
        "en": "extract SQL comments are allowed only before the SELECT/WITH header.",
    },
    "extract.sql.header_block_unclosed": {
        "ko": "extract SQL header block comment가 닫히지 않았습니다.",
        "en": "extract SQL header block comment is not closed.",
    },
    "extract.sql.statement_type_invalid": {
        "ko": "extract SQL은 header comment 이후 SELECT 또는 WITH로 시작해야 합니다.",
        "en": "extract SQL must start with SELECT or WITH after header comments.",
    },
    "extract.sql.forbidden_token": {
        "ko": "extract SQL에 금지된 token이 포함되어 있습니다: {token}",
        "en": "extract SQL contains a forbidden token: {token}",
    },
    "extract.sql.path_outside_project": {
        "ko": "extract SQL 파일 경로는 project root 내부여야 합니다.",
        "en": "extract SQL file path must stay inside the project root.",
    },
    "project.extract_query_sql.invalid": {
        "ko": "extract query SQL 검증에 실패했습니다.",
        "en": "extract query SQL validation failed.",
    },
}


class _SafeParams(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def normalize_language(value: str | None) -> str:
    if not value:
        return DEFAULT_LANG
    lang = value.strip().lower().replace("_", "-")
    primary = lang.split("-", 1)[0]
    if primary in SUPPORTED_LANGS:
        return primary
    return DEFAULT_LANG


def resolve_language(
    explicit: str | None = None,
    config: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    if explicit:
        return normalize_language(explicit)
    env = environ if environ is not None else os.environ
    if env.get("ZETA4S_LANG"):
        return normalize_language(env.get("ZETA4S_LANG"))
    if config and config.get("language"):
        return normalize_language(str(config.get("language")))
    return DEFAULT_LANG


def render_message(
    code: str,
    params: Mapping[str, Any] | None = None,
    lang: str | None = None,
    default: str | None = None,
) -> str:
    selected = normalize_language(lang)
    values = dict(params or {})
    template = MESSAGES.get(code, {}).get(selected) or MESSAGES.get(code, {}).get(DEFAULT_LANG)
    if not template:
        if default:
            return default
        encoded = json.dumps(values, ensure_ascii=False, sort_keys=True)
        return f"message not found: {code} params={encoded}"
    return template.format_map(_SafeParams(values))
