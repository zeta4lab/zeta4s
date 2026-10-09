"""ClickHouse rowset write backend."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.backends.clickhouse.client import get_clickhouse_runtime_client
from zeta4s.runtime.backends.clickhouse.rowset_io import (
    DEFAULT_BATCH_SIZE,
    insert_rowset_batches,
    require_rowset_schema,
    validate_no_nulls,
)
from zeta4s.runtime.rowsets import ResolvedRowsetRef, rowset_column_specs_from_schema_metadata
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.task_result import log_task_event
from zeta4s.runtime.types import (
    clickhouse_type_from_arrow_field,
    clickhouse_type_from_column_spec,
    is_clickhouse_not_null_type,
    unwrap_clickhouse_type,
)

import logging

logger = logging.getLogger(__name__)


def write_clickhouse_rowset(
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
        raise ValueError("clickhouse.write mode must be replace, append or upsert")
    columns = [validate_sql_identifier(column, "clickhouse.write.columns[]") for column in columns]
    key = [validate_sql_identifier(column, "clickhouse.write.key[]") for column in (key or [])]
    if mode == "upsert" and not key:
        raise ValueError("clickhouse.write mode=upsert requires key[]")

    schema = rowset.schema
    require_rowset_schema(schema, rowset.source_ref)
    specs = _selected_column_specs(rowset, schema, columns)
    missing_key = [column for column in key if column not in columns]
    if missing_key:
        raise ValueError(f"clickhouse.write key columns must be included in columns: {missing_key}")
    order_by_columns = _column_list(options.get("order_by"), "clickhouse.write.order_by")
    missing_order_by = [column for column in order_by_columns if column not in columns]
    if missing_order_by:
        raise ValueError(f"clickhouse.write order_by columns must be included in columns: {missing_order_by}")
    not_null_columns = sorted(set(key) | set(order_by_columns))
    batch_size = int(options.get("batch_size") or DEFAULT_BATCH_SIZE)
    if batch_size <= 0:
        raise ValueError("clickhouse.write batch_size must be greater than zero")
    validate_no_nulls(
        rowset,
        not_null_columns,
        batch_size=batch_size,
        label="clickhouse.write key/order_by",
    )

    client = get_clickhouse_runtime_client(target_conn, connections=connections)
    target_ref = _target_ref(client, target_table=target_table, target_namespace=target_namespace)

    if mode == "replace" and after is None:
        _drop_table(client, target_ref)
        _create_table(client, target_ref, specs, options=options, not_null_columns=not_null_columns)
    else:
        _ensure_table(
            client,
            target_ref,
            specs,
            columns=columns,
            not_null_columns=not_null_columns,
            options=options,
        )
    if mode == "upsert":
        _delete_matching_keys(client, target_ref, rowset, key, batch_size=batch_size, context=context, after=after)

    loaded, batches = insert_rowset_batches(
        client=client,
        target_ref=target_ref,
        rowset=rowset,
        columns=columns,
        batch_size=batch_size,
        on_progress=lambda loaded_rows, _batches: log_task_event(
            logger,
            "write.clickhouse.progress",
            context=context,
            target=target_ref,
            mode=mode,
            loaded_rows=loaded_rows,
        ),
        after=after,
        on_checkpoint=on_checkpoint,
    )
    return {
        "target": target_ref,
        "input_rows": loaded,
        "output_rows": loaded,
        "success_rows": loaded,
        "failed_rows": 0,
        "skipped_rows": 0,
        "batches": batches,
    }


def _selected_column_specs(rowset: ResolvedRowsetRef, schema, columns: list[str]) -> list[ColumnSpec]:
    available = set(schema.names)
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(f"clickhouse.write columns not found in rowset: {missing}")
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
            validate_sql_identifier(field.name, "rowset.column"),
            clickhouse_type_from_arrow_field(field),
            field.nullable,
            source_backend="arrow",
        )
        for field in schema
    ]


def _target_ref(client, *, target_table: str, target_namespace: str | None) -> str:
    table = _quote_identifier(target_table, "clickhouse.write.target.table")
    if not target_namespace:
        return table
    namespace = _quote_identifier(target_namespace, "clickhouse.write.target.namespace")
    client.command(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    return f"{namespace}.{table}"


def _create_table(
    client,
    target_ref: str,
    specs: list[ColumnSpec],
    *,
    options: dict[str, Any],
    not_null_columns: list[str],
) -> None:
    not_null = set(not_null_columns)
    columns = [
        f"{_quote_identifier(spec.name, 'clickhouse.write.column')} {_clickhouse_type_for_write(spec, spec.name in not_null)}"
        for spec in specs
    ]
    if not columns:
        raise ValueError(f"rowset has no columns: {target_ref}")
    engine = str(options.get("engine") or "MergeTree").strip()
    if engine != "MergeTree":
        raise ValueError("clickhouse.write currently supports engine=MergeTree")
    order_by = _order_by(options.get("order_by"))
    partition_by = _partition_by(options.get("partition_by"))
    settings = _settings(options.get("settings"))
    client.command(
        f"CREATE TABLE {target_ref} ("
        + ", ".join(columns)
        + f") ENGINE = MergeTree{partition_by} ORDER BY {order_by}{settings}"
    )


def _ensure_table(
    client,
    target_ref: str,
    specs: list[ColumnSpec],
    *,
    columns: list[str],
    options: dict[str, Any],
    not_null_columns: list[str],
) -> None:
    if _table_exists(client, target_ref):
        _validate_existing_table(client, target_ref, columns=columns, not_null_columns=not_null_columns)
        return
    _create_table(client, target_ref, specs, options=options, not_null_columns=not_null_columns)


def _validate_existing_table(
    client,
    target_ref: str,
    *,
    columns: list[str],
    not_null_columns: list[str],
) -> None:
    target_columns = _target_columns(client, target_ref)
    missing = [column for column in columns if column not in target_columns]
    if missing:
        raise ValueError(f"clickhouse.write target table is missing columns: {missing}")
    nullable_keys = [column for column in not_null_columns if not is_clickhouse_not_null_type(target_columns[column])]
    if nullable_keys:
        raise ValueError(f"clickhouse.write target key/order_by columns must be non-null: {nullable_keys}")


def _target_columns(client, target_ref: str) -> dict[str, str]:
    result = client.query(f"DESCRIBE TABLE {target_ref}")
    columns: dict[str, str] = {}
    for row in result.result_rows:
        column = validate_sql_identifier(str(row[0]), "clickhouse.write.target.column")
        columns[column] = str(row[1])
    if not columns:
        raise ValueError(f"clickhouse.write target table has no columns: {target_ref}")
    return columns


def _clickhouse_type_for_write(spec: ColumnSpec, force_not_null: bool) -> str:
    if not force_not_null:
        return clickhouse_type_from_column_spec(spec)
    source_type = unwrap_clickhouse_type(spec.source_type) if spec.source_type else None
    return clickhouse_type_from_column_spec(replace(spec, nullable=False, source_type=source_type))


def _drop_table(client, target_ref: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {target_ref}")


def _table_exists(client, target_ref: str) -> bool:
    row = client.query(f"EXISTS TABLE {target_ref}").first_row
    return bool(row and int(row[0]) > 0)


def _delete_matching_keys(
    client,
    target_ref: str,
    rowset: ResolvedRowsetRef,
    key: list[str],
    *,
    batch_size: int,
    context: dict[str, Any] | None,
    after=None,
) -> None:
    deleted_batches = 0
    for item in rowset.iter_positioned_batches(batch_size=batch_size, columns=key, after=after):
        batch = item.batch
        if not batch.num_rows:
            continue
        predicate = _key_delete_predicate(batch, key)
        client.command(f"ALTER TABLE {target_ref} DELETE WHERE {predicate} SETTINGS mutations_sync = 1")
        deleted_batches += 1
        log_task_event(
            logger,
            "write.clickhouse.progress",
            context=context,
            target=target_ref,
            mode="upsert",
            delete_batches=deleted_batches,
        )


def _key_delete_predicate(batch, key: list[str]) -> str:
    values_by_column = {column: batch.column(column).to_pylist() for column in key}
    if len(key) == 1:
        column = key[0]
        values = [_clickhouse_literal(value) for value in values_by_column[column]]
        return f"{_quote_identifier(column, 'clickhouse.write.key')} IN ({', '.join(values)})"
    tuples = []
    for row_index in range(batch.num_rows):
        values = [_clickhouse_literal(values_by_column[column][row_index]) for column in key]
        tuples.append("(" + ", ".join(values) + ")")
    columns = ", ".join(_quote_identifier(column, "clickhouse.write.key") for column in key)
    return f"({columns}) IN ({', '.join(tuples)})"


def _order_by(value: Any) -> str:
    columns = _column_list(value, "clickhouse.write.order_by")
    if not columns:
        return "tuple()"
    return "(" + ", ".join(_quote_identifier(column, "clickhouse.write.order_by") for column in columns) + ")"


def _partition_by(value: Any) -> str:
    columns = _column_list(value, "clickhouse.write.partition_by")
    if not columns:
        return ""
    return (
        " PARTITION BY ("
        + ", ".join(_quote_identifier(column, "clickhouse.write.partition_by") for column in columns)
        + ")"
    )


def _column_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_columns = [value]
    elif isinstance(value, list):
        raw_columns = [str(item) for item in value]
    else:
        raise ValueError(f"{label} must be a string or list")
    return [validate_sql_identifier(column, label) for column in raw_columns]


def _settings(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, dict):
        raise ValueError("clickhouse.write settings must be a mapping")
    if not value:
        return ""
    items = []
    for key, setting_value in sorted(value.items()):
        name = validate_sql_identifier(str(key), "clickhouse.write.settings key")
        items.append(f"{name} = {_clickhouse_literal(setting_value)}")
    return " SETTINGS " + ", ".join(items)


def _quote_identifier(identifier: str, label: str) -> str:
    value = validate_sql_identifier(str(identifier), label)
    return "`" + value.replace("`", "``") + "`"


def _clickhouse_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (Decimal, int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"
