"""Runtime time display helpers."""

from __future__ import annotations

from datetime import UTC, datetime
import os
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_DISPLAY_TIMEZONE = "Asia/Seoul"
DISPLAY_TIMEZONE_ENV = "ZETA4S_DISPLAY_TIMEZONE"

_ISO_UTC_PATTERN = re.compile(r"(?<![\w])(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|\+00:00|\+0000)(?![\w])")


def display_timezone_name(value: str | None = None) -> str:
    return (value or os.environ.get(DISPLAY_TIMEZONE_ENV) or DEFAULT_DISPLAY_TIMEZONE).strip()


def display_timezone(value: str | None = None) -> ZoneInfo:
    name = display_timezone_name(value)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"unknown timezone: {name}") from e


def utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_timezone_suffix(value: str) -> str:
    safe = "".join(char for char in value if char.isalnum())
    return safe or "LOCAL"


def local_run_timestamp(now: datetime | None = None, timezone_name: str | None = None) -> str:
    local = (now or utc_now()).astimezone(display_timezone(timezone_name))
    suffix = _safe_timezone_suffix(local.tzname() or display_timezone_name(timezone_name))
    return local.strftime("%Y%m%dT%H%M%S") + suffix


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def format_display_time(value: str | datetime | None, timezone_name: str | None = None) -> str | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    local = parsed.astimezone(display_timezone(timezone_name))
    return local.strftime("%Y-%m-%d %H:%M:%S %Z%z")


def localize_log_timestamps(text: str, timezone_name: str | None = None) -> str:
    tz = display_timezone(timezone_name)

    def replace(match: re.Match[str]) -> str:
        value = f"{match.group(1)}T{match.group(2)}{match.group(3) or ''}{match.group(4)}"
        local = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(tz)
        return local.strftime("%Y-%m-%d %H:%M:%S %Z%z")

    return _ISO_UTC_PATTERN.sub(replace, text)
