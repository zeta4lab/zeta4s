"""Runtime context helpers independent from any scheduler at import time."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


CORE_EXECUTION_KWARGS = frozenset({"step_execution", "step_execution_payload"})


def current_context_from_kwargs(
    kwargs: dict[str, Any],
    *,
    exclude_keys: set[str] | None = None,
    set_run_date: bool = False,
) -> dict[str, Any]:
    explicit_context = kwargs.get("runtime_context")
    if explicit_context is None:
        explicit_context = kwargs.get("context")
    excluded = {"runtime_context", "context", *CORE_EXECUTION_KWARGS, *(exclude_keys or set())}
    if explicit_context is not None:
        if not isinstance(explicit_context, dict):
            raise ValueError("runtime_context must be a mapping")
        context = dict(explicit_context)
        for key, value in kwargs.items():
            if key not in excluded:
                context.setdefault(key, value)
    else:
        context = {key: value for key, value in kwargs.items() if key not in excluded}
    if set_run_date:
        context.setdefault("run_date", datetime.now(timezone.utc))
    return context
