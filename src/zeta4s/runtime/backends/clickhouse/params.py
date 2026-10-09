"""ClickHouse named parameter binding.

zeta4s SQL contract uses ``:name`` placeholders for every backend. ClickHouse does not parse
``:name``; ``clickhouse_connect`` binds either client-side ``%(name)s`` or server-side
``{name:Type}``. This module rewrites ``:name`` into server-side ``{name:Type}`` at the ClickHouse
backend boundary.

Server-side binding is chosen over client-side ``%(name)s`` because:

- client-side binding applies Python ``%`` formatting to the whole statement, so every literal
  ``%`` (for example ``LIKE '%x%'``) would have to be escaped;
- client-side binding renders ``datetime`` without sub-second precision, which shifts
  ``DateTime64`` watermark/window predicates, while ``DateTime64(6)`` compares correctly against
  ``Date``, ``DateTime`` and ``DateTime64`` columns.

The ClickHouse type of each placeholder is inferred from the Python value (see
``clickhouse_param_type``). Values whose type cannot be inferred fail with a clear error instead of
being sent with a guessed type.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
import re
from typing import Any
import uuid

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_HEREDOC_RE = re.compile(r"\$([A-Za-z0-9_]*)\$")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_INT256_MIN = -(2**255)
_INT256_MAX = 2**255 - 1


def bind_clickhouse_named_params(sql: str, params: dict[str, Any] | None) -> tuple[str, dict[str, Any] | None]:
    """Rewrite ``:name`` placeholders into ``{name:Type}`` and return the parameters they use.

    String literals, quoted identifiers, heredocs, comments and ``::`` casts are left untouched.
    Parameters that the statement does not reference are dropped. A placeholder without a matching
    parameter raises ``ValueError``. When the statement has no placeholder the SQL is returned
    unchanged with ``None`` parameters.
    """
    params = params or {}
    out: list[str] = []
    used: dict[str, Any] = {}
    types: dict[str, str] = {}
    missing: list[str] = []
    idx = 0
    length = len(sql)
    while idx < length:
        char = sql[idx]
        if char in "'\"`":
            end = _quoted_end(sql, idx, char)
            out.append(sql[idx:end])
            idx = end
            continue
        if sql.startswith("--", idx):
            end = sql.find("\n", idx)
            end = length if end < 0 else end
            out.append(sql[idx:end])
            idx = end
            continue
        if sql.startswith("/*", idx):
            end = sql.find("*/", idx + 2)
            end = length if end < 0 else end + 2
            out.append(sql[idx:end])
            idx = end
            continue
        if char == "$":
            heredoc = _HEREDOC_RE.match(sql, idx)
            if heredoc:
                close = sql.find(heredoc.group(0), heredoc.end())
                end = length if close < 0 else close + len(heredoc.group(0))
                out.append(sql[idx:end])
                idx = end
                continue
        if char == ":":
            if sql.startswith("::", idx):
                out.append("::")
                idx += 2
                continue
            prev = sql[idx - 1] if idx > 0 else ""
            match = _NAME_RE.match(sql, idx + 1)
            if match and not (prev.isalnum() or prev in "_."):
                name = match.group(0)
                if name not in params:
                    if name not in missing:
                        missing.append(name)
                else:
                    if name not in types:
                        value, param_type = clickhouse_param_value(name, params[name])
                        used[name] = value
                        types[name] = param_type
                    out.append("{" + name + ":" + types[name] + "}")
                idx = match.end()
                continue
        out.append(char)
        idx += 1
    if missing:
        raise ValueError("ClickHouse SQL references parameters without values: " + ", ".join(f":{n}" for n in missing))
    if not used:
        return sql, None
    return "".join(out), used


def clickhouse_param_value(name: str, value: Any) -> tuple[Any, str]:
    """Return the value to send and its ClickHouse type for one placeholder."""
    try:
        param_type = clickhouse_param_type(value)
    except TypeError as exc:
        raise ValueError(f"ClickHouse SQL parameter :{name} {exc}") from exc
    return _wire_value(value), param_type


def clickhouse_param_type(value: Any) -> str:
    """Infer the ClickHouse query parameter type for a Python value.

    ``datetime`` is sent as ``DateTime64(6)`` so sub-second precision survives; a timezone-aware
    value is normalized to UTC and typed ``DateTime64(6, 'UTC')``, a naive value is interpreted in
    the ClickHouse session timezone. ``date`` uses ``Date32`` for its wider range. ``None`` uses
    ``Nullable(Nothing)`` so it fits any comparison or ``coalesce`` supertype.
    """
    if value is None:
        return "Nullable(Nothing)"
    if isinstance(value, bool):
        return "Bool"
    if isinstance(value, int):
        if _INT64_MIN <= value <= _INT64_MAX:
            return "Int64"
        if _INT256_MIN <= value <= _INT256_MAX:
            return "Int256"
        raise TypeError("integer is out of ClickHouse Int256 range")
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise TypeError("decimal must be finite")
        exponent = value.as_tuple().exponent
        scale = -exponent if isinstance(exponent, int) and exponent < 0 else 0
        if scale > 76:
            raise TypeError("decimal scale exceeds ClickHouse Decimal precision 76")
        return f"Decimal(76, {scale})"
    if isinstance(value, dt.datetime):
        return "DateTime64(6, 'UTC')" if value.tzinfo is not None else "DateTime64(6)"
    if isinstance(value, dt.date):
        return "Date32"
    if isinstance(value, (str, bytes, bytearray)):
        return "String"
    if isinstance(value, uuid.UUID):
        return "UUID"
    if isinstance(value, (list, tuple)):
        element_types = {clickhouse_param_type(item) for item in value if item is not None}
        if len(element_types) > 1:
            raise TypeError("list elements must share one ClickHouse type: " + ", ".join(sorted(element_types)))
        element_type = element_types.pop() if element_types else "Nothing"
        if any(item is None for item in value):
            element_type = f"Nullable({element_type})"
        return f"Array({element_type})"
    raise TypeError(f"has unsupported type {type(value).__name__}")


def _wire_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_wire_value(item) for item in value]
    return value


def _quoted_end(sql: str, start: int, quote: str) -> int:
    """Return the index after a quoted literal/identifier, honoring backslash and doubled quotes."""
    idx = start + 1
    length = len(sql)
    while idx < length:
        char = sql[idx]
        if char == "\\":
            idx += 2
            continue
        if char == quote:
            if idx + 1 < length and sql[idx + 1] == quote:
                idx += 2
                continue
            return idx + 1
        idx += 1
    return length
