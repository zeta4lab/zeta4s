"""Prometheus text exposition helpers for zeta4s-api."""

from __future__ import annotations

import math
from typing import Any


def label_value(value: Any) -> str:
    text = str(value)
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def labels(values: dict[str, Any]) -> str:
    if not values:
        return ""
    rendered = ",".join(f'{key}="{label_value(value)}"' for key, value in sorted(values.items()))
    return "{" + rendered + "}"


def sample(name: str, value: int | float, metric_labels: dict[str, Any] | None = None) -> str:
    numeric = float(value)
    if not math.isfinite(numeric):
        numeric = 0.0
    return f"{name}{labels(metric_labels or {})} {numeric:g}"


def family_header(name: str, help_text: str, metric_type: str = "gauge") -> list[str]:
    return [
        f"# HELP {name} {help_text}",
        f"# TYPE {name} {metric_type}",
    ]


def render_prometheus_text(lines: list[str]) -> str:
    return "\n".join(lines).rstrip() + "\n"
