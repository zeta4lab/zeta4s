"""Target-neutral schema contracts for write validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class TargetColumnContract:
    name: str
    ordinal: int
    target_type: str
    logical_family: str
    nullable: bool
    precision: int | None = None
    scale: int | None = None
    length: int | None = None
    length_semantics: str | None = None
    datetime_precision: int | None = None
    charset: str | None = None
    is_key: bool = False
    default_expr: str | None = None


@dataclass(frozen=True)
class TargetUniqueConstraint:
    name: str
    columns: tuple[str, ...]
    kind: str


@dataclass(frozen=True)
class TargetTableContract:
    target_type: str
    schema: str | None
    table: str
    columns: tuple[TargetColumnContract, ...]
    key_columns: tuple[str, ...] = ()
    unique_constraints: tuple[TargetUniqueConstraint, ...] = ()

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema else self.table

    def column_by_name(self) -> dict[str, TargetColumnContract]:
        return {column.name.lower(): column for column in self.columns}


class TargetSchemaInspector(Protocol):
    target_type: str

    def inspect_table(self, target_conn: str, target_table: str) -> TargetTableContract: ...
