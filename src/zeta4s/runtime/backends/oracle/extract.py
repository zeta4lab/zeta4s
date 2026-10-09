"""Oracle rowset extract backend."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import oracledb

from zeta4s.common.runtime_limits import validate_native_oracle_batch_size
from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.backends.oracle.client import get_oracle_conn, oracle_db_types
from zeta4s.runtime.rowset_models import ResumeCapability
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch, reject_restart_only_continuation
from zeta4s.runtime.types import arrow_type_from_clickhouse_type, unwrap_clickhouse_type


def clickhouse_type_from_oracle_description(desc) -> str:
    """Map an Oracle cursor.description entry to the rowset ClickHouse type contract."""
    db_type = desc[1] if len(desc) > 1 else None
    precision = desc[4] if len(desc) > 4 else None
    scale = desc[5] if len(desc) > 5 else None

    if db_type == getattr(oracledb, "DB_TYPE_DATE", None):
        return "Nullable(DateTime)"
    if db_type in oracle_db_types("DB_TYPE_TIMESTAMP", "DB_TYPE_TIMESTAMP_LTZ", "DB_TYPE_TIMESTAMP_TZ"):
        timestamp_precision = oracle_timestamp_precision(scale)
        if timestamp_precision <= 0:
            return "Nullable(DateTime)"
        return f"Nullable(DateTime64({timestamp_precision}))"
    if db_type in oracle_db_types(
        "DB_TYPE_CHAR",
        "DB_TYPE_NCHAR",
        "DB_TYPE_NVARCHAR",
        "DB_TYPE_VARCHAR",
        "DB_TYPE_LONG",
        "DB_TYPE_CLOB",
        "DB_TYPE_NCLOB",
    ):
        return "Nullable(String)"
    if db_type in oracle_db_types("DB_TYPE_BINARY_DOUBLE", "DB_TYPE_BINARY_FLOAT", "DB_TYPE_DOUBLE"):
        return "Nullable(Float64)"
    if db_type == oracledb.DB_TYPE_NUMBER:
        if precision is None or scale is None or scale < 0:
            return "Nullable(String)"
        if scale and scale > 0:
            p = precision or 38
            s = scale
            if p > 76:
                return "Nullable(Float64)"
            return f"Nullable(Decimal({p}, {s}))"
        if precision and precision > 18:
            return f"Nullable(Decimal({precision}, 0))"
        return "Nullable(Int64)"
    if db_type in oracle_db_types("DB_TYPE_BLOB", "DB_TYPE_LONG_RAW", "DB_TYPE_RAW"):
        return "Nullable(String)"
    return "Nullable(String)"


def oracle_timestamp_precision(scale) -> int:
    try:
        precision = int(scale)
    except (TypeError, ValueError):
        return 6
    if 0 <= precision <= 9:
        return precision
    return 6


def source_type_label_from_oracle_description(desc) -> str | None:
    db_type = desc[1] if len(desc) > 1 else None
    precision = desc[4] if len(desc) > 4 else None
    scale = desc[5] if len(desc) > 5 else None
    if db_type is None:
        return None
    if db_type == getattr(oracledb, "DB_TYPE_NUMBER", None):
        if precision is not None and scale is not None:
            try:
                p = int(precision)
                s = int(scale)
            except (TypeError, ValueError):
                return "NUMBER"
            if 1 <= p <= 38 and s >= 0:
                return f"NUMBER({p},{s})"
        return "NUMBER"
    if db_type == getattr(oracledb, "DB_TYPE_DATE", None):
        return "DATE"
    if db_type in oracle_db_types("DB_TYPE_TIMESTAMP", "DB_TYPE_TIMESTAMP_LTZ", "DB_TYPE_TIMESTAMP_TZ"):
        return f"TIMESTAMP({oracle_timestamp_precision(scale)})"
    if db_type in oracle_db_types("DB_TYPE_BINARY_DOUBLE", "DB_TYPE_DOUBLE"):
        return "BINARY_DOUBLE"
    if db_type in oracle_db_types("DB_TYPE_BINARY_FLOAT"):
        return "BINARY_FLOAT"
    if db_type in oracle_db_types("DB_TYPE_CHAR", "DB_TYPE_NCHAR"):
        return "CHAR"
    if db_type in oracle_db_types("DB_TYPE_NVARCHAR", "DB_TYPE_VARCHAR"):
        return "VARCHAR2"
    if db_type in oracle_db_types("DB_TYPE_CLOB", "DB_TYPE_NCLOB"):
        return "CLOB"
    if db_type in oracle_db_types("DB_TYPE_BLOB", "DB_TYPE_LONG_RAW", "DB_TYPE_RAW"):
        return "BINARY"
    return str(db_type)


def column_spec_from_oracle_description(desc) -> ColumnSpec:
    column = validate_sql_identifier(desc[0].lower(), "oracle.extract.column")
    ch_type = clickhouse_type_from_oracle_description(desc)
    nullable = bool(desc[6]) if len(desc) > 6 and desc[6] is not None else True
    spec = ColumnSpec.from_type(
        column,
        ch_type,
        nullable,
        source_backend="oracle",
        source_type=source_type_label_from_oracle_description(desc),
    )
    if spec.source_type and spec.source_type.startswith("NUMBER("):
        precision = desc[4] if len(desc) > 4 else None
        scale = desc[5] if len(desc) > 5 else None
        return ColumnSpec(
            name=spec.name,
            type=spec.type,
            nullable=spec.nullable,
            logical_type="decimal" if scale and scale > 0 else "integer",
            precision=int(precision) if precision is not None else spec.precision,
            scale=int(scale) if scale is not None else spec.scale,
            source_backend=spec.source_backend,
            source_type=spec.source_type,
        )
    if spec.source_type == "DATE":
        return ColumnSpec(
            name=spec.name,
            type=spec.type,
            nullable=spec.nullable,
            logical_type="timestamp",
            datetime_precision=0,
            source_backend=spec.source_backend,
            source_type=spec.source_type,
        )
    return spec


def oracle_column_specs(description) -> list[ColumnSpec]:
    return [column_spec_from_oracle_description(desc) for desc in description]


def normalize_lob_policy(lob_policy: dict | None) -> dict[str, dict]:
    if not lob_policy:
        return {}
    columns = lob_policy.get("columns") if isinstance(lob_policy, dict) else None
    if not isinstance(columns, dict):
        raise ValueError("extract.lob_policy.columns must be a mapping")
    normalized = {}
    for column, policy in columns.items():
        column = validate_sql_identifier(column, "extract.lob_policy.columns").lower()
        if not isinstance(policy, dict):
            raise ValueError(f"extract.lob_policy.columns.{column} must be a mapping")
        mode = policy.get("mode")
        if mode != "read_text":
            raise ValueError(f"extract.lob_policy.columns.{column}.mode must be read_text")
        max_bytes = policy.get("max_bytes")
        if max_bytes is not None and (not isinstance(max_bytes, int) or max_bytes <= 0):
            raise ValueError(f"extract.lob_policy.columns.{column}.max_bytes must be null or >= 1")
        on_overflow = policy.get("on_overflow", "fail")
        if on_overflow != "fail":
            raise ValueError(f"extract.lob_policy.columns.{column}.on_overflow must be fail")
        normalized[column] = {"mode": mode, "max_bytes": max_bytes, "on_overflow": on_overflow}
    return normalized


def coerce_oracle_value(value, column: str, ch_type: str, has_source_type: bool, lob_policy: dict):
    if isinstance(value, oracledb.LOB):
        value = _coerce_lob_value(value, column, lob_policy.get(column))
    if value is None:
        return None
    if column in lob_policy and isinstance(value, str):
        _check_lob_text_policy(value, column, lob_policy[column])
    if unwrap_clickhouse_type(ch_type).startswith("Decimal"):
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if has_source_type and unwrap_clickhouse_type(ch_type) == "String":
        return str(value)
    return value


def _coerce_lob_value(value, column: str, policy: dict | None):
    if policy is None:
        raise ValueError(f"Oracle LOB value requires explicit extract.lob_policy for column: {column}")
    read_value = value.read()
    if isinstance(read_value, bytes):
        raise ValueError(f"extract.lob_policy mode=read_text cannot read binary LOB column: {column}")
    if read_value is None:
        return None
    if not isinstance(read_value, str):
        read_value = str(read_value)
    _check_lob_text_policy(read_value, column, policy)
    return read_value


def _check_lob_text_policy(value: str, column: str, policy: dict) -> None:
    if policy["max_bytes"] is None:
        return
    byte_len = len(value.encode("utf-8"))
    if byte_len > policy["max_bytes"]:
        raise ValueError(
            "Oracle LOB value exceeds extract.lob_policy max_bytes: "
            f"column={column}, bytes={byte_len}, max_bytes={policy['max_bytes']}"
        )


class OracleSelectReader:
    resume_capability = ResumeCapability.RESTART_ONLY
    """Oracle SELECT reader that yields bounded parquet-rowset-ready batches."""

    source_kind = "oracle"

    def __init__(
        self,
        *,
        source_conn: str,
        source_object: str,
        query: str,
        params: dict,
        fetch_batch_size: int,
        lob_policy: dict,
        arraysize: int | None = None,
        prefetchrows: int | None = None,
        connections: dict[str, Any] | None = None,
    ) -> None:
        self.source_conn = source_conn
        self.source_object = source_object
        self.query = query
        self.params = params
        self.fetch_batch_size = validate_native_oracle_batch_size(int(fetch_batch_size), "extract.fetch_batch_size")
        self.arraysize = (
            validate_native_oracle_batch_size(int(arraysize), "extract.arraysize") if arraysize is not None else None
        )
        self.prefetchrows = (
            validate_native_oracle_batch_size(int(prefetchrows), "extract.prefetchrows")
            if prefetchrows is not None
            else None
        )
        self.lob_policy = lob_policy
        self.connections = connections
        self.column_specs: list[ColumnSpec] = []
        self.columns: list[str] = []
        self._conn = None
        self._cursor = None
        self._use_dataframe_fetch = False

    def __enter__(self) -> "OracleSelectReader":
        self._conn = get_oracle_conn(self.source_conn, connections=self.connections)
        self._cursor = self._conn.cursor()
        self._cursor.arraysize = self.arraysize or self.fetch_batch_size
        if hasattr(self._cursor, "prefetchrows"):
            self._cursor.prefetchrows = self.prefetchrows or self.fetch_batch_size
        self._cursor.execute(self.query, self.params)
        self.column_specs = oracle_column_specs(self._cursor.description)
        self.columns = [column for column, _, _ in self.column_specs]
        self._use_dataframe_fetch = hasattr(self._conn, "fetch_df_batches") and not self.lob_policy
        if self._use_dataframe_fetch:
            self._cursor.close()
            self._cursor = None
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._cursor is not None:
            self._cursor.close()
        if self._conn is not None:
            self._conn.close()
        self._cursor = None
        self._conn = None

    def read_batches(self, *, after: dict[str, Any] | None = None):
        reject_restart_only_continuation(after, "oracle")
        if self._use_dataframe_fetch:
            yield from self._read_dataframe_batches()
            return
        if self._cursor is None:
            raise RuntimeError("OracleSelectReader must be opened before reading")
        while True:
            fetched = self._cursor.fetchmany(self.fetch_batch_size)
            if not fetched:
                break
            column_values = {column: [] for column in self.columns}
            for row in fetched:
                for value, spec in zip(row, self.column_specs, strict=True):
                    column_values[spec.name].append(
                        coerce_oracle_value(
                            value,
                            spec.name,
                            spec.type,
                            spec.source_type is not None,
                            self.lob_policy,
                        )
                    )
            yield SourceBatch(column_values=column_values)

    def _read_dataframe_batches(self):
        if self._conn is None:
            raise RuntimeError("OracleSelectReader must be opened before reading")
        import pyarrow as pa

        schema = oracle_dataframe_arrow_schema(self.column_specs)
        batches = self._conn.fetch_df_batches(
            self.query,
            self.params,
            size=self.fetch_batch_size,
            fetch_decimals=True,
        )
        for dataframe in batches:
            table = pa.table(dataframe)
            if table.num_rows == 0:
                continue
            if table.num_columns != len(self.columns):
                raise ValueError(
                    "Oracle DataFrame fetch returned unexpected column count: "
                    f"expected={len(self.columns)}, got={table.num_columns}"
                )
            table = table.rename_columns(self.columns)
            table = normalize_oracle_dataframe_table(table, schema, self.column_specs)
            yield SourceBatch(arrow_table=table.cast(schema))


def normalize_oracle_dataframe_table(table, schema, column_specs: list[ColumnSpec]):
    import pyarrow as pa

    arrays = []
    fields = {field.name: field for field in schema}
    for spec in column_specs:
        array = table.column(spec.name)
        field = fields[spec.name]
        if pa.types.is_string(field.type) and not pa.types.is_string(array.type):
            arrays.append(
                pa.array([None if value is None else str(value) for value in array.to_pylist()], type=pa.string())
            )
            continue
        arrays.append(array)
    return pa.Table.from_arrays(arrays, names=[spec.name for spec in column_specs])


def oracle_dataframe_arrow_schema(column_specs: list[ColumnSpec]):
    import pyarrow as pa

    return pa.schema(
        [pa.field(spec.name, oracle_dataframe_arrow_type(spec), nullable=spec.nullable) for spec in column_specs]
    )


def oracle_dataframe_arrow_type(spec: ColumnSpec):
    return arrow_type_from_clickhouse_type(spec.type)
