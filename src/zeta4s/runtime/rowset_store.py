"""Storage-neutral rowset reader and writer contracts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
import os
from typing import Any, Protocol

from zeta4s.runtime.rowset_models import (
    ResumeCapability,
    RowsetDescriptor,
    RowsetIdentity,
    RowsetStorage,
)
from zeta4s.runtime.source_reader import ColumnSpec


class CheckpointNotSupported(RuntimeError):
    pass


class RowsetReader(Protocol):
    descriptor: RowsetDescriptor

    def iter_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
    ) -> Iterator[Any]: ...

    def iter_positioned_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
        after: Mapping[str, Any] | None = None,
    ) -> Iterator["PositionedRowsetBatch"]: ...


class RowsetWriteSession(Protocol):
    identity: RowsetIdentity

    def append(self, batch: Any) -> None: ...
    def checkpoint(self, continuation: Mapping[str, Any]) -> RowsetDescriptor: ...
    def finish(self) -> RowsetDescriptor: ...
    def abort(self) -> None: ...


class RowsetStore(Protocol):
    resume_capability: ResumeCapability

    def begin(self, identity: RowsetIdentity, *, schema: Any) -> RowsetWriteSession: ...
    def resume(
        self,
        descriptor: RowsetDescriptor,
        *,
        identity: RowsetIdentity | None = None,
    ) -> RowsetWriteSession: ...
    def open_reader(self, descriptor: RowsetDescriptor) -> RowsetReader: ...


@dataclass(frozen=True)
class CheckpointPolicy:
    target_bytes: int = 128 * 1024 * 1024
    max_interval_seconds: int = 60

    @classmethod
    def from_environment(cls) -> "CheckpointPolicy":
        return cls(
            target_bytes=int(os.environ.get("ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES", cls.target_bytes)),
            max_interval_seconds=int(
                os.environ.get("ZETA4S_ROWSET_CHECKPOINT_MAX_INTERVAL_SECONDS", cls.max_interval_seconds)
            ),
        )


@dataclass(frozen=True)
class PositionedRowsetBatch:
    batch: Any
    continuation: dict[str, Any]


def rowset_descriptor_payload(descriptor: RowsetDescriptor) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": "rowset",
        "storage": descriptor.storage.value,
        "uri": descriptor.uri,
        "rows": descriptor.rows,
        "bytes": descriptor.bytes,
        "columns": list(descriptor.columns),
        "column_specs": [spec.to_json() for spec in descriptor.column_specs],
        "schema_fingerprint": descriptor.schema_fingerprint,
    }
    if descriptor.snapshot_id is not None:
        payload["snapshot_id"] = descriptor.snapshot_id
    if descriptor.table_identifier is not None:
        payload["table_identifier"] = descriptor.table_identifier
    return payload


def rowset_descriptor_from_payload(payload: Mapping[str, Any]) -> RowsetDescriptor:
    raw_storage = payload.get("storage")
    if not isinstance(raw_storage, str) or not raw_storage:
        raise ValueError("rowset descriptor requires storage")
    try:
        storage = RowsetStorage(raw_storage)
    except ValueError as exc:
        raise ValueError(f"unsupported rowset storage: {raw_storage}") from exc
    uri = payload.get("uri")
    if not isinstance(uri, str) or not uri:
        raise ValueError("rowset descriptor requires uri")
    fingerprint = payload.get("schema_fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint:
        raise ValueError("rowset descriptor requires schema_fingerprint")
    specs_value = payload.get("column_specs") or []
    if not isinstance(specs_value, list):
        raise ValueError("rowset descriptor column_specs must be a list")
    columns_value = payload.get("columns") or []
    if not isinstance(columns_value, list):
        raise ValueError("rowset descriptor columns must be a list")
    snapshot_id = _optional_int(payload.get("snapshot_id"))
    table_identifier = payload.get("table_identifier")
    if storage is RowsetStorage.ICEBERG and (snapshot_id is None or not table_identifier):
        raise ValueError("iceberg rowset descriptor requires snapshot_id and table_identifier")
    return RowsetDescriptor(
        storage=storage,
        uri=uri,
        rows=_non_negative_int(payload.get("rows"), "rows"),
        bytes=_non_negative_int(payload.get("bytes"), "bytes"),
        columns=tuple(str(column) for column in columns_value),
        column_specs=tuple(ColumnSpec.from_value(spec) for spec in specs_value),
        schema_fingerprint=fingerprint,
        snapshot_id=snapshot_id,
        table_identifier=str(table_identifier) if table_identifier is not None else None,
    )


def _non_negative_int(value: Any, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"rowset descriptor requires integer {field}") from exc
    if result < 0:
        raise ValueError(f"rowset descriptor {field} must be non-negative")
    return result


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


__all__ = [
    "CheckpointNotSupported",
    "CheckpointPolicy",
    "PositionedRowsetBatch",
    "RowsetReader",
    "RowsetStore",
    "RowsetWriteSession",
    "rowset_descriptor_from_payload",
    "rowset_descriptor_payload",
]
