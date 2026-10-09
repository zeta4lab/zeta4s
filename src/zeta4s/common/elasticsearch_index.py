"""Elasticsearch index name contract shared by source extract and write."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ES_INDEX_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
ES_INDEX_FORBIDDEN_CHARS = set(r'\/:*?"<>| #,')
ES_INDEX_TEMPLATE_RE = re.compile(r"\{(data_interval_end|logical_date|run_date):([^{}]+)\}")


def validate_elasticsearch_index(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} 은 비어 있을 수 없다.")
    value = value.strip()
    if (
        value in {".", ".."}
        or not ES_INDEX_RE.fullmatch(value)
        or any(char in ES_INDEX_FORBIDDEN_CHARS for char in value)
    ):
        raise ValueError(f"{label} 은 lowercase Elasticsearch index name 이어야 한다.")
    return value


def validate_elasticsearch_index_template(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} 은 비어 있을 수 없다.")
    value = value.strip()
    stripped = ES_INDEX_TEMPLATE_RE.sub("x", value)
    if "{" in stripped or "}" in stripped:
        raise ValueError(
            f"{label} 은 "
            "{data_interval_end:<strftime>}, {logical_date:<strftime>} 또는 "
            "{run_date:<strftime>} 만 허용한다."
        )
    sample = ES_INDEX_TEMPLATE_RE.sub(
        lambda match: datetime(2026, 1, 1).strftime(match.group(2)),
        value,
    )
    validate_elasticsearch_index(sample, label)
    return value


def _as_template_datetime(value: Any, label: str, timezone_name: str | None, timezone_label: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"{label} must be a datetime") from e
    if not isinstance(value, datetime):
        raise ValueError(f"{label} must be a datetime")
    if timezone_name:
        try:
            target_tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"{timezone_label} is not a valid IANA timezone: {timezone_name}") from e
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(target_tz).replace(tzinfo=None)
    if value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


def render_elasticsearch_index_template(
    index_template: str,
    context: dict[str, Any],
    index_timezone: str | None = None,
    *,
    label: str,
    timezone_label: str,
) -> str:
    index_template = validate_elasticsearch_index_template(index_template, label)

    def replace(match: re.Match) -> str:
        key = match.group(1)
        fmt = match.group(2)
        value = context.get(key)
        if value is None:
            raise ValueError(f"{label} requires Airflow context value: {key}")
        return _as_template_datetime(value, key, index_timezone, timezone_label).strftime(fmt)

    rendered = ES_INDEX_TEMPLATE_RE.sub(replace, index_template)
    if "{" in rendered or "}" in rendered:
        raise ValueError(
            f"{label} only supports "
            "{data_interval_end:<strftime>}, {logical_date:<strftime>} or {run_date:<strftime>}"
        )
    return validate_elasticsearch_index(rendered, label)


def resolve_elasticsearch_index(
    index: str | None,
    index_template: str | None,
    context: dict[str, Any],
    index_timezone: str | None = None,
    *,
    index_label: str,
    index_template_label: str,
    index_timezone_label: str,
) -> str:
    if bool(index) == bool(index_template):
        raise ValueError(f"{index_label} 또는 {index_template_label} 중 하나만 필요하다.")
    if index_template:
        return render_elasticsearch_index_template(
            index_template,
            context,
            index_timezone,
            label=index_template_label,
            timezone_label=index_timezone_label,
        )
    if index_timezone:
        raise ValueError(f"{index_timezone_label} 은 {index_template_label} 과 함께 사용해야 한다.")
    assert index is not None
    return validate_elasticsearch_index(index, index_label)
