"""Oracle runtime backend client helpers."""

from __future__ import annotations

import oracledb
from typing import Any

from zeta4s.runtime.connections import resolve_runtime_connection


def oracle_db_types(*names: str) -> set:
    return {value for name in names if (value := getattr(oracledb, name, None)) is not None}


def oracle_text_lob_output_type_handler(cursor, metadata):
    """Fetch Oracle text LOBs as Python strings instead of LOB locators."""
    db_type = getattr(metadata, "type", None)
    if db_type == getattr(oracledb, "DB_TYPE_CLOB", None):
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if db_type == getattr(oracledb, "DB_TYPE_NCLOB", None):
        return cursor.var(oracledb.DB_TYPE_LONG_NVARCHAR, arraysize=cursor.arraysize)
    return None


def oracle_dsn_from_runtime_connection(runtime_connection, conn_id: str) -> str:
    """Build an Oracle DSN from a runtime connection payload."""
    extra = runtime_connection.extra_dejson or {}
    if extra.get("dsn"):
        return extra["dsn"]

    host = runtime_connection.host
    port = runtime_connection.port
    service_name = extra.get("service_name") or getattr(runtime_connection, "schema", None)
    sid = extra.get("sid")
    if not host or not port:
        raise ValueError(f"Oracle runtime connection {conn_id!r} requires host and port unless extra.dsn is set.")
    if service_name and sid:
        raise ValueError(f"Oracle runtime connection {conn_id!r} must set only one of extra.service_name or extra.sid.")
    if service_name:
        return oracledb.makedsn(host, port, service_name=service_name)
    if sid:
        return oracledb.makedsn(host, port, sid=sid)
    raise ValueError(
        f"Oracle runtime connection {conn_id!r} requires one of extra.dsn, extra.service_name, extra.sid, or schema."
    )


def get_oracle_conn(conn_id: str, *, connections: dict[str, Any] | None = None):
    runtime_connection = resolve_runtime_connection(conn_id, connections=connections)
    dsn = oracle_dsn_from_runtime_connection(runtime_connection, conn_id)
    conn = oracledb.connect(user=runtime_connection.login, password=runtime_connection.password, dsn=dsn)
    conn.outputtypehandler = oracle_text_lob_output_type_handler
    return conn
