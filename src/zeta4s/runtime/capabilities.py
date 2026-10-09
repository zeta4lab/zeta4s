"""Runtime engine capability and cleanup policy selection."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class EngineCapability:
    ddl: bool = False
    truncate: bool = False
    create_object: bool = False
    delete: bool = False
    merge: bool = False
    reindex: bool = False

    @classmethod
    def for_engine(cls, engine: str) -> "EngineCapability":
        engine = engine.lower()
        if engine == "oracle":
            return cls(delete=True, merge=True)
        if engine == "elasticsearch":
            return cls(delete=True, reindex=True)
        if engine == "clickhouse":
            return cls(ddl=True, truncate=True, create_object=True, delete=True, merge=True)
        raise ValueError(f"unsupported capability engine: {engine}")

    @classmethod
    def from_config(cls, engine: str, value: dict[str, Any] | None = None) -> "EngineCapability":
        capability = cls.for_engine(engine)
        if not value:
            return capability
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(f"unknown capability field: {unknown}")
        overrides = {key: bool(item) for key, item in value.items()}
        return replace(capability, **overrides)


def normalize_cleanup_strategy(value: str | list[str] | None) -> list[str]:
    if value is None:
        return ["truncate", "delete_all", "none"]
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not value:
        raise ValueError("cleanup_strategy must be a non-empty string or list")
    allowed = {"truncate", "delete_all", "delete_where", "none"}
    normalized = []
    for item in value:
        if item not in allowed:
            raise ValueError(f"unsupported cleanup_strategy: {item}")
        normalized.append(item)
    return normalized


def select_cleanup_action(
    capability: EngineCapability,
    strategy: str | list[str] | None,
    *,
    cleanup_where: str | None = None,
) -> str:
    for candidate in normalize_cleanup_strategy(strategy):
        if candidate == "truncate" and capability.truncate:
            return candidate
        if candidate == "delete_all" and capability.delete:
            return candidate
        if candidate == "delete_where" and capability.delete and cleanup_where:
            return candidate
        if candidate == "none":
            return candidate
    raise ValueError(
        "no cleanup strategy is allowed by engine capability: "
        f"strategy={strategy}, cleanup_where={cleanup_where!r}, capability={capability}"
    )


def cleanup_sql(table_name: str, action: str, cleanup_where: str | None = None) -> str | None:
    if action == "truncate":
        return f"TRUNCATE TABLE {table_name}"
    if action == "delete_all":
        return f"DELETE FROM {table_name}"
    if action == "delete_where":
        if not cleanup_where:
            raise ValueError("cleanup_where is required for cleanup_strategy=delete_where")
        return f"DELETE FROM {table_name} WHERE {cleanup_where}"
    if action == "none":
        return None
    raise ValueError(f"unsupported cleanup action: {action}")
