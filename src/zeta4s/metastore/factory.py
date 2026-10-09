"""Metastore adapter factory."""

from __future__ import annotations

import os

from zeta4s.metastore.contracts import MetastoreAdapter


def metastore_adapter_factory() -> MetastoreAdapter:
    metastore_type = os.environ.get("ZETA4S_METASTORE_TYPE", "postgres")
    if metastore_type == "postgres":
        from zeta4s.metastore.backends.postgres import PostgresMetastoreAdapter

        return PostgresMetastoreAdapter()
    if metastore_type == "clickhouse":
        from zeta4s.metastore.backends.clickhouse import ClickHouseMetastoreAdapter

        return ClickHouseMetastoreAdapter()
    raise NotImplementedError(f"unsupported metastore type: {metastore_type}")
