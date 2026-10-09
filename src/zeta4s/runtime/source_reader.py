"""Bounded source reader contracts for extract runtimes."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import re
from typing import Any, Protocol

from zeta4s.runtime.rowset_models import ResumeCapability


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    nullable: bool
    logical_type: str
    precision: int | None = None
    scale: int | None = None
    datetime_precision: int | None = None
    source_backend: str | None = None
    source_type: str | None = None

    def __iter__(self):
        yield self.name
        yield self.type
        yield self.nullable

    def to_json(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "logical_type": self.logical_type,
        }
        for key in ("precision", "scale", "datetime_precision", "source_backend", "source_type"):
            value = getattr(self, key)
            if value is not None:
                result[key] = value
        return result

    @classmethod
    def from_value(cls, value: Any) -> "ColumnSpec":
        if isinstance(value, ColumnSpec):
            return value
        if isinstance(value, dict):
            name = str(value["name"])
            if not value.get("type"):
                raise ValueError("rowset column spec requires type")
            type_name = str(value["type"])
            return cls(
                name=name,
                type=type_name,
                nullable=bool(value.get("nullable", True)),
                logical_type=str(value.get("logical_type") or _logical_type_from_type(type_name)),
                precision=_optional_int(value.get("precision")),
                scale=_optional_int(value.get("scale")),
                datetime_precision=_optional_int(value.get("datetime_precision")),
                source_backend=str(value["source_backend"]) if value.get("source_backend") is not None else None,
                source_type=str(value["source_type"]) if value.get("source_type") is not None else None,
            )
        if isinstance(value, (list, tuple)) and len(value) >= 3:
            name, type_name, nullable = value[:3]
            return cls.from_type(str(name), str(type_name), bool(nullable))
        raise ValueError("rowset column spec must be a mapping or 3-item sequence")

    @classmethod
    def from_type(
        cls,
        name: str,
        type_name: str,
        nullable: bool,
        *,
        source_backend: str | None = None,
        source_type: str | None = None,
    ) -> "ColumnSpec":
        logical_type = _logical_type_from_type(type_name)
        precision, scale = _precision_scale_from_type(type_name)
        datetime_precision = _datetime_precision_from_type(type_name)
        return cls(
            name=name,
            type=type_name,
            nullable=nullable,
            logical_type=logical_type,
            precision=precision,
            scale=scale,
            datetime_precision=datetime_precision,
            source_backend=source_backend,
            source_type=source_type,
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _unwrap_type(type_name: str) -> str:
    current = str(type_name).strip()
    while current.startswith("Nullable(") and current.endswith(")"):
        current = current[len("Nullable(") : -1].strip()
    while current.startswith("LowCardinality(") and current.endswith(")"):
        current = current[len("LowCardinality(") : -1].strip()
    return current


def _logical_type_from_type(type_name: str) -> str:
    base = _unwrap_type(type_name)
    if base in {"Bool", "Boolean"}:
        return "boolean"
    if base.startswith(("Int", "UInt")):
        return "integer"
    if base.startswith("Float"):
        return "float"
    if base.startswith("Decimal"):
        return "decimal"
    if base in {"Date", "Date32"}:
        return "date"
    if base.startswith("DateTime"):
        return "timestamp"
    return "string"


def _precision_scale_from_type(type_name: str) -> tuple[int | None, int | None]:
    base = _unwrap_type(type_name)
    match = re.match(r"Decimal(?:32|64|128|256)?\((\d+)\s*,\s*(\d+)\)", base)
    if match:
        return int(match.group(1)), int(match.group(2))
    if base.startswith(("Int", "UInt")):
        return {"Int8": 3, "UInt8": 3, "Int16": 5, "UInt16": 5, "Int32": 10, "UInt32": 10}.get(base, 19), 0
    if base.startswith("Float32"):
        return 32, None
    if base.startswith("Float64") or base.startswith("Float"):
        return 64, None
    return None, None


def _datetime_precision_from_type(type_name: str) -> int | None:
    base = _unwrap_type(type_name)
    if base == "DateTime":
        return 0
    match = re.match(r"DateTime64\((\d+)(?:\s*,[^)]*)?\)", base)
    if match:
        return int(match.group(1))
    return None


@dataclass(frozen=True)
class SourceBatch:
    """A bounded extract batch.

    Reader implementations must not materialize the full source result set.
    Rows in one batch are limited by the source reader's configured batch size.
    Row batches serve row-oriented readers. Columnar batches avoid rebuilding
    row tuples before Arrow/ClickHouse ingestion.
    """

    rows: list[tuple] | None = None
    column_values: dict[str, list[Any]] | None = None
    arrow_table: Any | None = None
    continuation: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        payloads = sum(value is not None for value in (self.rows, self.column_values, self.arrow_table))
        if payloads != 1:
            raise ValueError("SourceBatch requires exactly one of rows, column_values or arrow_table")
        if self.column_values is not None:
            lengths = {len(values) for values in self.column_values.values()}
            if len(lengths) > 1:
                raise ValueError("SourceBatch column_values must have equal lengths")
        if self.arrow_table is not None and not hasattr(self.arrow_table, "num_rows"):
            raise ValueError("SourceBatch arrow_table must expose num_rows")

    @property
    def row_count(self) -> int:
        if self.rows is not None:
            return len(self.rows)
        if self.arrow_table is not None:
            return int(self.arrow_table.num_rows)
        assert self.column_values is not None
        if not self.column_values:
            return 0
        return len(next(iter(self.column_values.values())))


class SourceReader(Protocol):
    """Source-side extract reader.

    Implementations own source-specific pagination, cursor, PIT, offset, or
    watermark mechanics, and expose only bounded row batches to the sink.
    """

    source_kind: str
    source_object: str
    column_specs: list[ColumnSpec]
    columns: list[str]
    resume_capability: ResumeCapability

    def read_batches(self, *, after: dict[str, Any] | None = None) -> Iterator[SourceBatch]:
        """Yield bounded batches of source rows."""


def reject_restart_only_continuation(after: dict[str, Any] | None, source_kind: str) -> None:
    if after is not None:
        raise ValueError(f"{source_kind} source reader does not support continuation resume")
