"""Oracle rowset IO helpers shared by stage and write backends."""

from __future__ import annotations

from typing import Any, Callable

from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.types import oracle_type_from_column_spec


DEFAULT_BATCH_SIZE = 10_000
DEFAULT_DELETE_BATCH_SIZE = 1_000


def require_rowset_schema(schema, source_ref: str) -> None:
    if len(schema) == 0:
        raise ValueError(f"rowset has no schema: {source_ref}")


def validate_no_nulls(rowset, columns: list[str], *, batch_size: int, label: str) -> None:
    if not columns:
        return
    for batch in rowset.iter_batches(batch_size=batch_size, columns=columns):
        for column in columns:
            if batch.column(column).null_count:
                raise ValueError(f"{label} column contains null values: {column}")


def insert_rowset_batches(
    *,
    cursor,
    sql: str,
    rowset,
    columns: list[str],
    batch_size: int,
    on_progress: Callable[[int, int], Any] | None = None,
    after=None,
    on_checkpoint=None,
) -> tuple[int, int]:
    loaded = 0
    batches = 0
    for item in rowset.iter_positioned_batches(batch_size=batch_size, columns=columns, after=after):
        batch = item.batch
        if not batch.num_rows:
            continue
        insert_record_batch(cursor, sql, batch, columns)
        loaded += int(batch.num_rows)
        batches += 1
        if on_progress is not None:
            on_progress(loaded, batches)
        if on_checkpoint is not None:
            on_checkpoint(loaded, batches, item.continuation)
    return loaded, batches


def insert_record_batch(cursor, sql: str, batch, columns: list[str]) -> None:
    import pyarrow as pa

    table = pa.Table.from_batches([batch]).select(columns)
    try:
        cursor.executemany(sql, table)
    except (AttributeError, TypeError, NotImplementedError):
        cursor.executemany(sql, rows_from_table(table, columns))


def rows_from_batch(batch, columns: list[str]) -> list[tuple]:
    import pyarrow as pa

    table = pa.Table.from_batches([batch]).select(columns)
    return rows_from_table(table, columns)


def rows_from_table(table, columns: list[str]) -> list[tuple]:
    values = {column: table.column(column).to_pylist() for column in columns}
    return [tuple(oracle_value(values[column][index]) for column in columns) for index in range(table.num_rows)]


def oracle_value(value):
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def set_oracle_input_sizes(cursor, specs: list[ColumnSpec]) -> None:
    setinputsizes = getattr(cursor, "setinputsizes", None)
    if setinputsizes is None:
        return
    try:
        import oracledb
    except ModuleNotFoundError:
        return
    input_sizes = [oracle_bind_type(spec, oracledb) for spec in specs]
    if input_sizes:
        setinputsizes(*input_sizes)


def oracle_bind_type(spec: ColumnSpec, oracledb_module):
    oracle_type = oracle_type_from_column_spec(spec).upper()
    if oracle_type == "DATE":
        return getattr(oracledb_module, "DB_TYPE_DATE", None)
    if oracle_type.startswith("TIMESTAMP"):
        return getattr(oracledb_module, "DB_TYPE_TIMESTAMP", None)
    if oracle_type.startswith("NUMBER"):
        return getattr(oracledb_module, "DB_TYPE_NUMBER", None)
    if oracle_type in {"BINARY_DOUBLE", "FLOAT"}:
        return getattr(oracledb_module, "DB_TYPE_BINARY_DOUBLE", None)
    if oracle_type == "BINARY_FLOAT":
        return getattr(oracledb_module, "DB_TYPE_BINARY_FLOAT", None)
    if "CLOB" in oracle_type:
        return getattr(oracledb_module, "DB_TYPE_CLOB", None)
    if "BLOB" in oracle_type or oracle_type in {"RAW", "BINARY"}:
        return getattr(oracledb_module, "DB_TYPE_BLOB", None)
    return getattr(oracledb_module, "DB_TYPE_VARCHAR", None)
