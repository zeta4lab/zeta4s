"""Oracle rowset write backend."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.backends.oracle.rowset_io import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_DELETE_BATCH_SIZE,
    insert_rowset_batches,
    require_rowset_schema,
    rows_from_batch,
    set_oracle_input_sizes,
    validate_no_nulls,
)
from zeta4s.runtime.rowsets import ResolvedRowsetRef, rowset_column_specs_from_schema_metadata
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.task_result import log_task_event
from zeta4s.runtime.types import clickhouse_type_from_arrow_field, oracle_type_from_column_spec

import logging

logger = logging.getLogger(__name__)


def write_oracle_rowset(
    *,
    rowset: ResolvedRowsetRef,
    target_conn: str,
    target_table: str,
    target_namespace: str | None,
    mode: str,
    columns: list[str],
    key: list[str] | None,
    options: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    options = dict(options or {})
    after = options.pop("_rowset_after", None)
    on_checkpoint = options.pop("_rowset_on_checkpoint", None)
    mode = str(mode or "").strip()
    if mode not in {"replace", "append", "upsert"}:
        raise ValueError("oracle.write mode must be replace, append or upsert")
    columns = [validate_sql_identifier(column, "oracle.write.columns[]") for column in columns]
    key = [validate_sql_identifier(column, "oracle.write.key[]") for column in (key or [])]
    if mode == "upsert" and not key:
        raise ValueError("oracle.write mode=upsert requires key[]")

    schema = rowset.schema
    require_rowset_schema(schema, rowset.source_ref)
    specs = _selected_column_specs(rowset, schema, columns)
    missing_key = [column for column in key if column not in columns]
    if missing_key:
        raise ValueError(f"oracle.write key columns must be included in columns: {missing_key}")
    batch_size = int(options.get("batch_size") or DEFAULT_BATCH_SIZE)
    if batch_size <= 0:
        raise ValueError("oracle.write batch_size must be greater than zero")
    delete_batch_size = int(options.get("delete_batch_size") or min(batch_size, DEFAULT_DELETE_BATCH_SIZE))
    if delete_batch_size <= 0:
        raise ValueError("oracle.write delete_batch_size must be greater than zero")
    validate_no_nulls(rowset, key, batch_size=batch_size, label="oracle.write key")

    target_ref = oracle_table_ref(target_table, target_namespace)
    from zeta4s.runtime.backends.oracle.client import get_oracle_conn

    conn = get_oracle_conn(target_conn, connections=connections)
    loaded = 0
    batches = 0
    try:
        cursor = conn.cursor()
        try:
            target_columns = [validate_sql_identifier(column, "oracle.write.column").upper() for column in columns]
            if mode == "replace" and after is None:
                _drop_table(cursor, target_ref)
                _create_table(cursor, target_ref, specs, key=key)
            else:
                _ensure_table(cursor, target_ref, columns=columns, key=key, specs=specs)
            if mode == "upsert":
                _delete_matching_keys(
                    cursor,
                    target_ref,
                    rowset,
                    key,
                    batch_size=delete_batch_size,
                    context=context,
                    after=after,
                )

            placeholders = ", ".join(f":{index + 1}" for index in range(len(target_columns)))
            sql = f"INSERT INTO {target_ref} ({', '.join(target_columns)}) VALUES ({placeholders})"
            set_oracle_input_sizes(cursor, specs)
            loaded, batches = insert_rowset_batches(
                cursor=cursor,
                sql=sql,
                rowset=rowset,
                columns=columns,
                batch_size=batch_size,
                on_progress=lambda loaded_rows, _batches: log_task_event(
                    logger,
                    "write.oracle.progress",
                    context=context,
                    target=target_ref,
                    mode=mode,
                    loaded_rows=loaded_rows,
                ),
                after=after,
                on_checkpoint=(
                    (
                        lambda loaded_rows, batch_count, position: (
                            conn.commit(),
                            on_checkpoint(loaded_rows, batch_count, position),
                        )
                    )
                    if on_checkpoint is not None
                    else None
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()
    return {
        "target": target_ref,
        "input_rows": loaded,
        "output_rows": loaded,
        "success_rows": loaded,
        "failed_rows": 0,
        "skipped_rows": 0,
        "batches": batches,
    }


def oracle_table_ref(table_name: str, namespace: str | None) -> str:
    table = validate_table_identifier(table_name, "oracle.write.target_table", max_parts=1).upper()
    if not namespace:
        return table
    namespace = validate_table_identifier(namespace, "oracle.write.target_namespace", max_parts=1).upper()
    return f"{namespace}.{table}"


def _selected_column_specs(rowset: ResolvedRowsetRef, schema, columns: list[str]) -> list[ColumnSpec]:
    available = set(schema.names)
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(f"oracle.write columns not found in rowset: {missing}")
    specs = _rowset_column_specs(rowset, schema)
    spec_by_name = {spec.name: spec for spec in specs}
    return [spec_by_name[column] for column in columns]


def _rowset_column_specs(rowset: ResolvedRowsetRef, schema) -> list[ColumnSpec]:
    if rowset.column_specs:
        return list(rowset.column_specs)
    metadata_specs = rowset_column_specs_from_schema_metadata(schema)
    if metadata_specs:
        return metadata_specs
    return [
        ColumnSpec.from_type(
            validate_sql_identifier(field.name, "oracle.write.rowset.column"),
            clickhouse_type_from_arrow_field(field),
            field.nullable,
            source_backend="arrow",
        )
        for field in schema
    ]


def _table_exists(cursor, table_name: str) -> bool:
    table_name = validate_table_identifier(table_name, "oracle.write.table", max_parts=2).upper()
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


def _drop_table(cursor, target_ref: str) -> None:
    if _table_exists(cursor, target_ref):
        cursor.execute(f"DROP TABLE {target_ref} PURGE")


def _create_table(cursor, target_ref: str, specs: list[ColumnSpec], *, key: list[str]) -> None:
    key_columns = {column.upper() for column in key}
    column_defs = []
    for spec in specs:
        column = validate_sql_identifier(spec.name, "oracle.write.column").upper()
        oracle_type = oracle_type_from_column_spec(replace(spec, nullable=False) if column in key_columns else spec)
        not_null = " NOT NULL" if column in key_columns else ""
        column_defs.append(f"{column} {oracle_type}{not_null}")
    if not column_defs:
        raise ValueError(f"rowset has no columns: {target_ref}")
    cursor.execute(f"CREATE TABLE {target_ref} ({', '.join(column_defs)})")


def _ensure_table(cursor, target_ref: str, *, columns: list[str], key: list[str], specs: list[ColumnSpec]) -> None:
    if not _table_exists(cursor, target_ref):
        _create_table(cursor, target_ref, specs, key=key)
        return
    target_columns = _target_columns(cursor, target_ref)
    missing = [column for column in columns if column.upper() not in target_columns]
    if missing:
        raise ValueError(f"oracle.write target table is missing columns: {missing}")
    nullable_keys = [column for column in key if target_columns[column.upper()] != "N"]
    if nullable_keys:
        raise ValueError(f"oracle.write target key columns must be non-null: {nullable_keys}")


def _target_columns(cursor, target_ref: str) -> dict[str, str]:
    target_ref = validate_table_identifier(target_ref, "oracle.write.target", max_parts=2).upper()
    parts = target_ref.split(".")
    if len(parts) == 2:
        owner, table = parts
        cursor.execute(
            """
            SELECT COLUMN_NAME, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            """,
            {"owner": owner, "table_name": table},
        )
    else:
        cursor.execute(
            """
            SELECT COLUMN_NAME, NULLABLE
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :table_name
            """,
            {"table_name": target_ref},
        )
    columns = {str(name).upper(): str(nullable).upper() for name, nullable in cursor.fetchall()}
    if not columns:
        raise ValueError(f"oracle.write target table has no columns: {target_ref}")
    return columns


def _delete_matching_keys(
    cursor,
    target_ref: str,
    rowset_source,
    key: list[str],
    *,
    batch_size: int,
    context: dict[str, Any] | None,
    after=None,
) -> None:
    if not key:
        return
    key_columns = [validate_sql_identifier(column, "oracle.write.key").upper() for column in key]
    predicate = " AND ".join(f"{column} = :{index + 1}" for index, column in enumerate(key_columns))
    sql = f"DELETE FROM {target_ref} WHERE {predicate}"
    deleted_batches = 0
    for item in rowset_source.iter_positioned_batches(batch_size=batch_size, columns=key, after=after):
        batch = item.batch
        if not batch.num_rows:
            continue
        cursor.executemany(sql, rows_from_batch(batch, key))
        deleted_batches += 1
        log_task_event(
            logger,
            "write.oracle.progress",
            context=context,
            target=target_ref,
            mode="upsert",
            delete_batches=deleted_batches,
        )
