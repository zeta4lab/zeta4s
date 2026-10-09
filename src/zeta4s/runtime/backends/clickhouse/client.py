"""ClickHouse runtime client helpers."""

from __future__ import annotations

from typing import Any

from zeta4s.runtime.connections import resolve_runtime_connection


def get_clickhouse_runtime_client(conn_id: str, *, connections: dict[str, Any] | None = None):
    import clickhouse_connect

    conn = resolve_runtime_connection(conn_id, connections=connections)
    extra = conn.extra_dejson or {}
    return clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=extra.get("database") or conn.schema or "default",
    )


def get_clickhouse_source_client(conn_id: str, *, connections: dict[str, Any] | None = None):
    return get_clickhouse_runtime_client(conn_id, connections=connections)
