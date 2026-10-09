"""dbt profile rendering for runtime backend adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import yaml

from zeta4s.dbt.graph import dbt_profile_name


@dataclass(frozen=True)
class DbtProfileConnection:
    conn_id: str
    conn_type: str
    host: str | None = None
    port: int | None = None
    login: str | None = None
    password: str | None = None
    schema: str | None = None
    extra: dict[str, Any] | None = None


def dbt_profile_connection_from_asset(conn_id: str, payload: dict[str, Any]) -> DbtProfileConnection:
    return DbtProfileConnection(
        conn_id=conn_id,
        conn_type=str(payload.get("conn_type") or "").strip(),
        host=_optional_str(payload.get("host")),
        port=_optional_int(payload.get("port")),
        login=_optional_str(payload.get("login")),
        password=_optional_str(payload.get("password")),
        schema=_optional_str(payload.get("schema")),
        extra=_extra_dict(payload.get("extra")),
    )


def dbt_profile_connection_from_profile(conn_id: str, payload: dict[str, Any]) -> DbtProfileConnection:
    extra = dict(payload.get("options") or {})
    if payload.get("database") is not None:
        extra.setdefault("database", payload["database"])
    if payload.get("schema") is not None:
        extra.setdefault("schema", payload["schema"])
    if payload.get("password_ref") is not None:
        extra.setdefault("password_ref", str(payload["password_ref"]))
    return DbtProfileConnection(
        conn_id=conn_id,
        conn_type=str(payload.get("type") or "").strip(),
        host=_optional_str(payload.get("host")),
        port=_optional_int(payload.get("port")),
        login=_optional_str(payload.get("username")),
        password=None,
        schema=_optional_str(payload.get("database")),
        extra=extra,
    )


def dbt_profile_connection_from_runtime(conn_id: str, runtime_connection: Any) -> DbtProfileConnection:
    return DbtProfileConnection(
        conn_id=conn_id,
        conn_type=str(getattr(runtime_connection, "conn_type", "") or "").strip(),
        host=_optional_str(getattr(runtime_connection, "host", None)),
        port=_optional_int(getattr(runtime_connection, "port", None)),
        login=_optional_str(getattr(runtime_connection, "login", None)),
        password=_optional_str(getattr(runtime_connection, "password", None)),
        schema=_optional_str(getattr(runtime_connection, "schema", None)),
        extra=_extra_dict(getattr(runtime_connection, "extra_dejson", None)),
    )


def render_dbt_profiles_yml(connection: DbtProfileConnection) -> str:
    adapter = _dbt_profile_adapter(connection.conn_type)
    profile = {
        dbt_profile_name(connection.conn_id): {
            "target": "runtime",
            "outputs": {
                "runtime": adapter(connection),
            },
        }
    }
    return yaml.safe_dump(profile, sort_keys=False)


def _dbt_profile_adapter(conn_type: str):
    if conn_type == "clickhouse":
        return _clickhouse_profile_output
    if conn_type == "oracle":
        return _oracle_profile_output
    raise ValueError(f"dbt runtime adapter is not implemented for conn_type={conn_type!r}")


def _clickhouse_profile_output(connection: DbtProfileConnection) -> dict[str, Any]:
    extra = connection.extra or {}
    return {
        "type": "clickhouse",
        "host": connection.host or "127.0.0.1",
        "port": connection.port or 8123,
        "user": connection.login or "default",
        "password": connection.password or "",
        "schema": extra.get("database") or connection.schema or "default",
        "secure": bool(extra.get("secure", False)),
    }


def _oracle_profile_output(connection: DbtProfileConnection) -> dict[str, Any]:
    extra = connection.extra or {}
    output: dict[str, Any] = {
        "type": "oracle",
        "user": connection.login or "",
        "password": connection.password or "",
        "database": extra.get("database")
        or extra.get("dbname")
        or connection.schema
        or extra.get("service_name")
        or "",
        "schema": extra.get("schema") or connection.login or connection.schema or "",
        "threads": int(extra.get("threads") or 1),
    }
    if extra.get("connection_string"):
        output["connection_string"] = str(extra["connection_string"])
    elif extra.get("dsn"):
        output["connection_string"] = str(extra["dsn"])
    else:
        if not connection.host:
            raise ValueError(f"Oracle dbt connection {connection.conn_id!r} requires host unless extra.dsn is set")
        output["protocol"] = str(extra.get("protocol") or "tcp")
        output["host"] = connection.host
        output["port"] = connection.port or 1521
        if extra.get("service_name"):
            output["service"] = str(extra["service_name"])
        else:
            output["service"] = connection.schema or output["database"]
    return output


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


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
