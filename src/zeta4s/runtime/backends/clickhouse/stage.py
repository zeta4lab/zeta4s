"""ClickHouse rowset stage backend."""

from __future__ import annotations

from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.backends.clickhouse.rowset_io import (
    DEFAULT_BATCH_SIZE,
    insert_rowset_batches,
    require_rowset_schema,
)
from zeta4s.runtime.backends.clickhouse.client import get_clickhouse_runtime_client
from zeta4s.runtime.rowsets import rowset_column_specs_from_schema_metadata
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.types import clickhouse_type_from_arrow_field, clickhouse_type_from_column_spec


def stage_clickhouse_rowset(
    *,
    rowset,
    stage_conn: str,
    target_table: str,
    target_namespace: str | None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[int, str]:
    kwargs = dict(kwargs or {})
    client = get_clickhouse_runtime_client(stage_conn, connections=kwargs.get("connections"))
    after = kwargs.get("_rowset_after")
    on_checkpoint = kwargs.get("_rowset_on_checkpoint")
    _ensure_namespace(client, target_namespace)
    target_ref = _table_ref(target_table, target_namespace)
    schema = rowset.schema
    require_rowset_schema(schema, rowset.source_ref)
    specs = _column_specs(rowset, schema)
    columns = [column for column, _, _ in specs]
    if after is None:
        _drop_clickhouse_table(client, target_ref)
        _create_clickhouse_table(client, target_ref, specs)
    elif not _clickhouse_table_exists(client, target_ref):
        raise ValueError(f"checkpoint target table is missing: {target_ref}")
    loaded, _ = insert_rowset_batches(
        client=client,
        target_ref=target_ref,
        rowset=rowset,
        columns=columns,
        batch_size=DEFAULT_BATCH_SIZE,
        after=after,
        on_checkpoint=on_checkpoint,
    )
    return loaded, target_ref


def _column_specs(rowset, schema) -> list[ColumnSpec]:
    if rowset.column_specs:
        return list(rowset.column_specs)
    metadata_specs = rowset_column_specs_from_schema_metadata(schema)
    if metadata_specs:
        return metadata_specs
    return _clickhouse_column_specs_from_arrow(schema)


def _ensure_namespace(client, namespace: str | None) -> None:
    if namespace:
        client.command(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace, 'clickhouse.stage.namespace')}")


def _table_ref(table_name: str, namespace: str | None) -> str:
    table = _quote_identifier(table_name, "clickhouse.stage.table")
    if not namespace:
        return table
    return f"{_quote_identifier(namespace, 'clickhouse.stage.namespace')}.{table}"


def _quote_identifier(value: str, label: str) -> str:
    return f"`{validate_sql_identifier(value, label)}`"


def _clickhouse_column_specs_from_arrow(schema) -> list[ColumnSpec]:
    return [
        ColumnSpec.from_type(
            validate_sql_identifier(field.name, "clickhouse.stage.rowset.column"),
            clickhouse_type_from_arrow_field(field),
            field.nullable,
            source_backend="arrow",
        )
        for field in schema
    ]


def _drop_clickhouse_table(client, target_ref: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {target_ref}")


def _clickhouse_table_exists(client, target_ref: str) -> bool:
    row = client.query(f"EXISTS TABLE {target_ref}").first_row
    return bool(row and int(row[0]) > 0)


def _create_clickhouse_table(client, target_ref: str, column_specs: list[ColumnSpec]) -> None:
    columns = [
        f"{validate_sql_identifier(spec.name, 'clickhouse.stage.rowset.column')} {clickhouse_type_from_column_spec(spec)}"
        for spec in column_specs
    ]
    if not columns:
        raise ValueError(f"rowset has no columns: {target_ref}")
    client.command(f"CREATE TABLE {target_ref} (" + ", ".join(columns) + ") ENGINE = MergeTree ORDER BY tuple()")
