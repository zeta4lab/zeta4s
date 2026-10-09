"""ClickHouse dbt runtime backend adapter."""

from __future__ import annotations

from typing import Any

from zeta4s.runtime.connections import resolve_runtime_connection
from zeta4s.runtime.dbt_profiles import dbt_profile_connection_from_runtime, render_dbt_profiles_yml
from zeta4s.runtime.dbt_runner import run_dbt_node_with_profile


def run_clickhouse_dbt_node(
    *,
    conn_id: str,
    dbt_project_path: str,
    unique_id: str,
    resource_type: str,
    node_name: str,
    command: str,
    **kwargs,
) -> dict[str, Any]:
    connection = dbt_profile_connection_from_runtime(
        conn_id,
        resolve_runtime_connection(conn_id, connections=kwargs.get("connections")),
    )
    return run_dbt_node_with_profile(
        conn_id=conn_id,
        conn_type="clickhouse",
        dbt_project_path=dbt_project_path,
        unique_id=unique_id,
        resource_type=resource_type,
        node_name=node_name,
        command=command,
        profiles_yml=render_dbt_profiles_yml(connection),
        **kwargs,
    )
