"""Runtime connection resolution independent from Airflow at import time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RuntimeConnection:
    conn_id: str
    conn_type: str | None = None
    host: str | None = None
    port: int | None = None
    login: str | None = None
    password: str | None = None
    schema: str | None = None
    extra_dejson: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProfileConnectionResolver:
    profile: dict[str, Any]
    secret_store: Any | None = None

    def __post_init__(self) -> None:
        _profile_connections(self.profile)

    def resolve(self, conn_id: str) -> RuntimeConnection:
        return resolve_runtime_connection(
            conn_id,
            connections=_profile_connections(self.profile),
            secret_resolver=self._resolve_secret,
        )

    def _resolve_secret(self, secret_key: str) -> str:
        store = self.secret_store
        if store is None:
            from zeta4s.runtime.secrets import EncryptedSecretStore

            store = EncryptedSecretStore()
        return str(store.resolve_secret(secret_key))


def resolve_runtime_connection(
    conn_id: str,
    *,
    connections: dict[str, Any] | None = None,
    secret_resolver: Any | None = None,
) -> Any:
    if connections and conn_id in connections:
        return _connection_from_value(conn_id, connections[conn_id], secret_resolver=secret_resolver)
    raise KeyError(f"runtime connection not provided: {conn_id}")


def _connection_from_value(conn_id: str, value: Any, *, secret_resolver: Any | None = None) -> Any:
    if isinstance(value, RuntimeConnection):
        return value
    if isinstance(value, dict):
        return RuntimeConnection(
            conn_id=conn_id,
            conn_type=_optional_str(value.get("conn_type") or value.get("type")),
            host=_optional_str(value.get("host")),
            port=_optional_int(value.get("port")),
            login=_optional_str(value.get("login") or value.get("user") or value.get("username")),
            password=_connection_password(value, secret_resolver=secret_resolver),
            schema=_optional_str(value.get("schema") or value.get("database")),
            extra_dejson=_extra(value),
        )
    return value


def _profile_connections(profile: dict[str, Any]) -> dict[str, Any]:
    connections = profile.get("connections")
    if connections is None:
        return {}
    if not isinstance(connections, dict):
        raise ValueError("profile.connections must be a mapping")
    return connections


def _connection_password(value: dict[str, Any], *, secret_resolver: Any | None = None) -> str | None:
    password = value.get("password")
    if password is not None:
        return _optional_str(password)
    password_ref = value.get("password_ref")
    if password_ref is None:
        return None
    if secret_resolver is None:
        raise ValueError("connection password_ref requires a secret resolver")
    return _optional_str(secret_resolver(str(password_ref)))


def _extra(value: dict[str, Any]) -> dict[str, Any]:
    raw = value.get("extra_dejson")
    if raw is None:
        raw = value.get("extra")
    if raw is None:
        raw = value.get("options")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("connection extra/options must be a mapping")
    return dict(raw)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
