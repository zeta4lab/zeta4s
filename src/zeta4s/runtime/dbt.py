"""dbt runtime router."""

from __future__ import annotations

from typing import Any

from zeta4s.runtime.connections import resolve_runtime_connection


def run_dbt_node(
    *,
    conn_id: str,
    dbt_project_path: str,
    unique_id: str,
    resource_type: str,
    node_name: str,
    command: str,
    **kwargs,
) -> dict[str, Any]:
    runtime_connection = resolve_runtime_connection(conn_id, connections=kwargs.get("connections"))
    if runtime_connection.conn_type == "clickhouse":
        from zeta4s.runtime.backends.clickhouse.dbt import run_clickhouse_dbt_node

        return run_clickhouse_dbt_node(
            conn_id=conn_id,
            dbt_project_path=dbt_project_path,
            unique_id=unique_id,
            resource_type=resource_type,
            node_name=node_name,
            command=command,
            **kwargs,
        )
    if runtime_connection.conn_type == "oracle":
        from zeta4s.runtime.backends.oracle.dbt import run_oracle_dbt_node

        return run_oracle_dbt_node(
            conn_id=conn_id,
            dbt_project_path=dbt_project_path,
            unique_id=unique_id,
            resource_type=resource_type,
            node_name=node_name,
            command=command,
            **kwargs,
        )
    raise NotImplementedError(f"dbt runtime adapter is not implemented for conn_type={runtime_connection.conn_type!r}")
