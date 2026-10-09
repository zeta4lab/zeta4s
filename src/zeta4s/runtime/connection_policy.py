"""Runtime connection policy projection helpers."""

from __future__ import annotations

import json
from typing import Any


def runtime_connection_policy_from_profile(profile: dict[str, Any]) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = []
    for conn_id, payload in profile_connection_payloads(profile):
        item = dict(payload)
        item["conn_id"] = conn_id
        item.pop("password", None)
        if item.get("extra") is not None:
            item["extra"] = _extra_dict(item["extra"])
        policies.append(item)
    return policies


def profile_connection_payloads(profile: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    connections = profile.get("connections") or {}
    if not isinstance(connections, dict):
        raise ValueError("profile.connections must be a mapping")
    payloads: list[tuple[str, dict[str, Any]]] = []
    for conn_id, item in sorted(connections.items()):
        if not isinstance(item, dict):
            raise ValueError(f"profile.connections[{conn_id}] must be a mapping")
        conn_type = str(item.get("type") or "").strip()
        if not conn_type:
            raise ValueError(f"profile.connections[{conn_id}] requires type")
        payload: dict[str, Any] = {
            "conn_id": str(conn_id),
            "conn_type": conn_type,
        }
        mapping = {
            "host": "host",
            "port": "port",
            "username": "login",
            "database": "schema",
        }
        for source, target in mapping.items():
            value = item.get(source)
            if value is not None:
                payload[target] = value
        extra: dict[str, Any] = {}
        if item.get("database") is not None:
            extra["database"] = item["database"]
        if item.get("schema") is not None:
            extra["schema"] = item["schema"]
        if item.get("password_ref") is not None:
            extra["password_ref"] = str(item["password_ref"])
        options = item.get("options")
        if options is not None:
            if not isinstance(options, dict):
                raise ValueError(f"profile.connections[{conn_id}].options must be a mapping")
            extra.update(options)
        if extra:
            payload["extra"] = json.dumps(extra, ensure_ascii=False, sort_keys=True)
        payloads.append((str(conn_id), payload))
    return payloads


def airflow_connection_projection(policy: dict[str, Any], *, password: str | None) -> dict[str, Any]:
    projection = dict(policy)
    projection["extra"] = dict(policy.get("extra") or {})
    if password is not None:
        projection["password"] = password
    return projection


def password_ref(policy: dict[str, Any]) -> str | None:
    extra = policy.get("extra") or {}
    if not isinstance(extra, dict):
        extra = _extra_dict(extra)
    value = extra.get("password_ref")
    return str(value) if value is not None and str(value) else None


def _extra_dict(value: Any) -> dict[str, Any]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        data = json.loads(value)
        if not isinstance(data, dict):
            raise ValueError("connection extra must decode to a mapping")
        return data
    raise ValueError("connection extra must be a mapping or JSON object string")
