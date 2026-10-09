"""Oracle rowset stage backend."""

from __future__ import annotations

from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.backends.oracle.rowset_io import (
    DEFAULT_BATCH_SIZE,
    insert_rowset_batches,
    require_rowset_schema,
    set_oracle_input_sizes,
)
from zeta4s.runtime.backends.oracle.client import get_oracle_conn
from zeta4s.runtime.rowsets import rowset_column_specs_from_schema_metadata
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.types import clickhouse_type_from_arrow_field, oracle_type_from_column_spec


def stage_oracle_rowset(
    *,
    rowset,
    stage_conn: str,
    target_table: str,
    target_namespace: str | None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[int, str]:
    kwargs = dict(kwargs or {})
    after = kwargs.get("_rowset_after")
    on_checkpoint = kwargs.get("_rowset_on_checkpoint")
    schema = rowset.schema
    require_rowset_schema(schema, rowset.source_ref)
    specs = _column_specs(rowset, schema)
    target_ref = oracle_table_ref(target_table, target_namespace)
    conn = get_oracle_conn(stage_conn, connections=kwargs.get("connections"))
    loaded = 0
    try:
        cursor = conn.cursor()
        try:
            if after is None:
                drop_oracle_table(cursor, target_ref)
            elif not oracle_table_exists(cursor, target_ref):
                raise ValueError(f"checkpoint target table is missing: {target_ref}")
            source_columns = [column for column, _, _ in specs]
            columns = [
                validate_sql_identifier(column, "oracle.stage.rowset.column").upper() for column in source_columns
            ]
            column_defs = ", ".join(
                f"{column} {oracle_type_from_column_spec(spec)}" for column, spec in zip(columns, specs, strict=True)
            )
            if after is None:
                cursor.execute(f"CREATE TABLE {target_ref} ({column_defs})")
            placeholders = ", ".join(f":{index + 1}" for index in range(len(columns)))
            sql = f"INSERT INTO {target_ref} ({', '.join(columns)}) VALUES ({placeholders})"
            set_oracle_input_sizes(cursor, specs)
            loaded, _ = insert_rowset_batches(
                cursor=cursor,
                sql=sql,
                rowset=rowset,
                columns=source_columns,
                batch_size=DEFAULT_BATCH_SIZE,
                after=after,
                on_checkpoint=(
                    (lambda loaded, batches, position: (conn.commit(), on_checkpoint(loaded, batches, position)))
                    if on_checkpoint is not None
                    else None
                ),
            )
            conn.commit()
        finally:
            cursor.close()
    finally:
        conn.close()
    return loaded, target_ref


def oracle_table_ref(table_name: str, namespace: str | None) -> str:
    table = validate_table_identifier(table_name, "oracle.stage.target_table", max_parts=1).upper()
    if not namespace:
        return table
    namespace = validate_table_identifier(namespace, "oracle.stage.target_namespace", max_parts=1).upper()
    return f"{namespace}.{table}"


def oracle_table_exists(cursor, table_name: str) -> bool:
    table_name = validate_table_identifier(table_name, "oracle.table", max_parts=2).upper()
    parts = table_name.split(".")
    if len(parts) == 2:
        owner, name = parts
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :table_name",
            {"owner": owner, "table_name": name},
        )
    else:
        cursor.execute(
            "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :table_name",
            {"table_name": table_name},
        )
    row = cursor.fetchone()
    return bool(row and int(row[0]) > 0)


def drop_oracle_table(cursor, table_name: str) -> None:
    table_name = validate_table_identifier(table_name, "oracle.table", max_parts=2).upper()
    if oracle_table_exists(cursor, table_name):
        cursor.execute(f"DROP TABLE {table_name} PURGE")


def _column_specs(rowset, schema) -> list[ColumnSpec]:
    if rowset.column_specs:
        return list(rowset.column_specs)
    metadata_specs = rowset_column_specs_from_schema_metadata(schema)
    if metadata_specs:
        return metadata_specs
    return _column_specs_from_arrow(schema)


def _column_specs_from_arrow(schema) -> list[ColumnSpec]:
    return [
        ColumnSpec.from_type(
            validate_sql_identifier(field.name, "oracle.stage.rowset.column"),
            clickhouse_type_from_arrow_field(field),
            field.nullable,
            source_backend="arrow",
        )
        for field in schema
    ]
