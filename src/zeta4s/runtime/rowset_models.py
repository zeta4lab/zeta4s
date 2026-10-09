"""Storage-neutral rowset identities and descriptors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zeta4s.runtime.source_reader import ColumnSpec


class RowsetStorage(StrEnum):
    PARQUET = "parquet"
    ICEBERG = "iceberg"


class ResumeCapability(StrEnum):
    EXACT = "exact"
    RESTART_ONLY = "restart_only"


@dataclass(frozen=True)
class RowsetIdentity:
    project_id: str
    job_id: str
    run_id: str
    step_id: str
    attempt: int
    output_name: str


@dataclass(frozen=True)
class RowsetDescriptor:
    storage: RowsetStorage
    uri: str
    rows: int
    bytes: int
    columns: tuple[str, ...]
    column_specs: tuple[ColumnSpec, ...]
    schema_fingerprint: str
    snapshot_id: int | None = None
    table_identifier: str | None = None


__all__ = [
    "ResumeCapability",
    "RowsetDescriptor",
    "RowsetIdentity",
    "RowsetStorage",
]
