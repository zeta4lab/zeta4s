"""Runtime type mapping helpers shared by extract, rowset and stage paths."""

from __future__ import annotations

import re

from zeta4s.runtime.source_reader import ColumnSpec


def unwrap_clickhouse_type(ch_type: str) -> str:
    normalized = str(ch_type).strip()
    while normalized.startswith("Nullable(") and normalized.endswith(")"):
        normalized = normalized[len("Nullable(") : -1].strip()
    while normalized.startswith("LowCardinality(") and normalized.endswith(")"):
        normalized = normalized[len("LowCardinality(") : -1].strip()
    return normalized


def is_clickhouse_not_null_type(ch_type: str) -> bool:
    normalized = str(ch_type).strip()
    while normalized.startswith("LowCardinality(") and normalized.endswith(")"):
        normalized = normalized[len("LowCardinality(") : -1].strip()
    return not (normalized.startswith("Nullable(") and normalized.endswith(")"))


def nullable_clickhouse_type(ch_type: str, nullable: bool) -> str:
    if not nullable:
        return ch_type
    normalized = str(ch_type).strip()
    if normalized.startswith("Nullable(") and normalized.endswith(")"):
        return normalized
    return f"Nullable({normalized})"


def clickhouse_decimal_precision_scale(ch_type: str) -> tuple[int, int]:
    match = re.match(r"^(Decimal|Decimal32|Decimal64|Decimal128|Decimal256)\(([^)]*)\)$", ch_type.strip())
    if not match:
        return 38, 0
    family = match.group(1)
    parts = [part.strip() for part in match.group(2).split(",") if part.strip()]
    if family == "Decimal":
        if len(parts) != 2:
            return 38, 0
        return int(parts[0]), int(parts[1])
    if len(parts) != 1:
        return 38, 0
    precision = {
        "Decimal32": 9,
        "Decimal64": 18,
        "Decimal128": 38,
        "Decimal256": 76,
    }[family]
    return precision, int(parts[0])


def clickhouse_datetime_unit(ch_type: str) -> str:
    if ch_type.strip() == "DateTime":
        return "s"
    match = re.match(r"^DateTime64\((\d+)(?:\s*,[^)]*)?\)$", ch_type.strip())
    if not match:
        return "s"
    precision = int(match.group(1))
    if precision <= 0:
        return "s"
    if precision <= 3:
        return "ms"
    if precision <= 6:
        return "us"
    return "ns"


def arrow_type_from_column_spec(spec: ColumnSpec):
    import pyarrow as pa

    if spec.logical_type == "boolean":
        return pa.bool_()
    if spec.logical_type == "integer":
        type_name = unwrap_clickhouse_type(spec.source_type or spec.type)
        if type_name.startswith("UInt64"):
            return pa.uint64()
        if type_name.startswith("UInt32"):
            return pa.uint32()
        if type_name.startswith("UInt16"):
            return pa.uint16()
        if type_name.startswith("UInt8"):
            return pa.uint8()
        precision = spec.precision or 19
        if precision <= 3:
            return pa.int8()
        if precision <= 5:
            return pa.int16()
        if precision <= 10:
            return pa.int32()
        return pa.int64()
    if spec.logical_type == "float":
        return pa.float32() if spec.precision == 32 else pa.float64()
    if spec.logical_type == "decimal":
        precision = spec.precision or 38
        scale = spec.scale or 0
        if precision <= 38:
            return pa.decimal128(precision, scale)
        return pa.decimal256(precision, scale)
    if spec.logical_type == "date":
        return pa.date32()
    if spec.logical_type == "timestamp":
        precision = 0 if spec.datetime_precision is None else spec.datetime_precision
        if precision <= 0:
            return pa.timestamp("s")
        if precision <= 3:
            return pa.timestamp("ms")
        if precision <= 6:
            return pa.timestamp("us")
        return pa.timestamp("ns")
    return pa.string()


def arrow_type_from_clickhouse_type(ch_type: str):
    import pyarrow as pa

    normalized = unwrap_clickhouse_type(ch_type)
    if normalized.startswith("UInt64"):
        return pa.uint64()
    if normalized.startswith("UInt32"):
        return pa.uint32()
    if normalized.startswith("UInt16"):
        return pa.uint16()
    if normalized.startswith("UInt8"):
        return pa.uint8()
    if normalized.startswith("Int64"):
        return pa.int64()
    if normalized.startswith("Int32"):
        return pa.int32()
    if normalized.startswith("Int16"):
        return pa.int16()
    if normalized.startswith("Int8"):
        return pa.int8()
    if normalized.startswith("Int"):
        return pa.int64()
    if normalized.startswith("Float32"):
        return pa.float32()
    if normalized.startswith("Float"):
        return pa.float64()
    if normalized.startswith("Decimal"):
        precision, scale = clickhouse_decimal_precision_scale(normalized)
        if precision <= 38:
            return pa.decimal128(precision, scale)
        return pa.decimal256(precision, scale)
    if normalized.startswith("DateTime"):
        return pa.timestamp(clickhouse_datetime_unit(normalized))
    if normalized == "Date":
        return pa.date32()
    if normalized in {"Bool", "Boolean"}:
        return pa.bool_()
    return pa.string()


def clickhouse_type_from_arrow_field(field) -> str:
    import pyarrow as pa

    arrow_type = field.type
    if pa.types.is_uint8(arrow_type):
        base = "UInt8"
    elif pa.types.is_uint16(arrow_type):
        base = "UInt16"
    elif pa.types.is_uint32(arrow_type):
        base = "UInt32"
    elif pa.types.is_uint64(arrow_type):
        base = "UInt64"
    elif pa.types.is_int8(arrow_type):
        base = "Int8"
    elif pa.types.is_int16(arrow_type):
        base = "Int16"
    elif pa.types.is_int32(arrow_type):
        base = "Int32"
    elif pa.types.is_int64(arrow_type):
        base = "Int64"
    elif pa.types.is_integer(arrow_type):
        bit_width = getattr(arrow_type, "bit_width", 64)
        base = f"Int{bit_width}"
    elif pa.types.is_floating(arrow_type):
        base = "Float32" if pa.types.is_float32(arrow_type) else "Float64"
    elif pa.types.is_decimal(arrow_type):
        base = f"Decimal({arrow_type.precision}, {arrow_type.scale})"
    elif pa.types.is_date(arrow_type):
        base = "Date"
    elif pa.types.is_timestamp(arrow_type):
        precision = {"s": 0, "ms": 3, "us": 6, "ns": 9}.get(str(arrow_type.unit), 6)
        base = "DateTime" if precision == 0 else f"DateTime64({precision})"
    elif pa.types.is_boolean(arrow_type):
        base = "Bool"
    else:
        base = "String"
    return nullable_clickhouse_type(base, field.nullable)


def clickhouse_type_from_column_spec(spec: ColumnSpec) -> str:
    if spec.source_backend == "clickhouse" and spec.source_type:
        return nullable_clickhouse_type(spec.source_type, spec.nullable)
    if spec.logical_type == "boolean":
        return nullable_clickhouse_type("Bool", spec.nullable)
    if spec.logical_type == "integer":
        base_type = unwrap_clickhouse_type(spec.source_type or spec.type)
        if base_type.startswith("UInt"):
            return nullable_clickhouse_type(base_type, spec.nullable)
        precision = spec.precision or 19
        if precision <= 3:
            return nullable_clickhouse_type("Int8", spec.nullable)
        if precision <= 5:
            return nullable_clickhouse_type("Int16", spec.nullable)
        if precision <= 10:
            return nullable_clickhouse_type("Int32", spec.nullable)
        return nullable_clickhouse_type("Int64", spec.nullable)
    if spec.logical_type == "float":
        return nullable_clickhouse_type("Float32" if spec.precision == 32 else "Float64", spec.nullable)
    if spec.logical_type == "decimal":
        return nullable_clickhouse_type(f"Decimal({spec.precision or 38}, {spec.scale or 0})", spec.nullable)
    if spec.logical_type == "date":
        return nullable_clickhouse_type("Date", spec.nullable)
    if spec.logical_type == "timestamp":
        precision = 0 if spec.datetime_precision is None else spec.datetime_precision
        return nullable_clickhouse_type("DateTime" if precision <= 0 else f"DateTime64({precision})", spec.nullable)
    return nullable_clickhouse_type("String", spec.nullable)


def oracle_type_from_column_spec(spec: ColumnSpec) -> str:
    if spec.source_backend == "oracle" and spec.source_type:
        source_type = spec.source_type.upper()
        if source_type.startswith("NUMBER("):
            return source_type
        if source_type == "DATE":
            return "DATE"
        if source_type.startswith("TIMESTAMP"):
            return source_type
        if source_type in {"BINARY_DOUBLE", "DB_TYPE_BINARY_DOUBLE", "<DB_TYPE DB_TYPE_BINARY_DOUBLE>"}:
            return "BINARY_DOUBLE"
        if source_type in {"BINARY_FLOAT", "DB_TYPE_BINARY_FLOAT", "<DB_TYPE DB_TYPE_BINARY_FLOAT>"}:
            return "BINARY_FLOAT"
        if source_type in {"CLOB", "NCLOB"}:
            return "CLOB"
        if source_type in {"BLOB", "BINARY", "RAW", "LONG RAW"}:
            return "BLOB"
    if spec.logical_type == "boolean":
        return "NUMBER(1,0)"
    if spec.logical_type == "integer":
        return f"NUMBER({min(spec.precision or 19, 38)},0)"
    if spec.logical_type == "float":
        return "BINARY_DOUBLE"
    if spec.logical_type == "decimal":
        return f"NUMBER({min(spec.precision or 38, 38)},{spec.scale or 0})"
    if spec.logical_type == "date":
        return "DATE"
    if spec.logical_type == "timestamp":
        precision = 6 if spec.datetime_precision is None else spec.datetime_precision
        if precision <= 0 and spec.source_backend == "oracle" and spec.source_type == "DATE":
            return "DATE"
        return f"TIMESTAMP({precision})"
    return "VARCHAR2(4000)"
