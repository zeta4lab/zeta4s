"""Iceberg REST Catalog-backed operational rowset storage."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import hashlib
import json
import os
from typing import Any

from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_models import (
    ResumeCapability,
    RowsetDescriptor,
    RowsetIdentity,
    RowsetStorage,
)
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.rowset_store import PositionedRowsetBatch


_NAMESPACE = "zeta4s_checkpoint"


class IcebergRowsetStore:
    resume_capability = ResumeCapability.EXACT

    def __init__(self, catalog: Any, *, warehouse: str) -> None:
        self.catalog = catalog
        self.warehouse = warehouse

    @classmethod
    def from_environment(cls) -> "IcebergRowsetStore":
        from pyiceberg.catalog import load_catalog

        uri = os.environ.get("ZETA4S_ICEBERG_CATALOG_URI", "").strip()
        warehouse = os.environ.get("ZETA4S_ICEBERG_WAREHOUSE", "").strip()
        if not uri or not warehouse:
            raise ValueError(
                "scheduler rowset storage requires ZETA4S_ICEBERG_CATALOG_URI and ZETA4S_ICEBERG_WAREHOUSE"
            )
        properties: dict[str, str] = {
            "type": "rest",
            "uri": uri,
            "warehouse": warehouse,
            "snapshot-loading-mode": "all",
        }
        token = os.environ.get("ZETA4S_ICEBERG_CATALOG_TOKEN", "").strip()
        if token:
            properties["token"] = token
        return cls(load_catalog("zeta4s", **properties), warehouse=warehouse)

    def inspect(self) -> dict[str, str]:
        self.catalog.list_namespaces()
        return {"catalog_uri": str(self.catalog.uri), "warehouse": self.warehouse, "status": "ok"}

    def begin(self, identity: RowsetIdentity, *, schema: Any) -> "_IcebergWriteSession":
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        try:
            self.catalog.create_namespace((_NAMESPACE,))
        except NamespaceAlreadyExistsError:
            pass
        identifier = (_NAMESPACE, _table_name(identity))
        column_specs = _column_specs(schema)
        table = self.catalog.create_table(
            identifier,
            schema=schema,
            properties={
                **_identity_properties(identity),
                "zeta4s.column-specs": json.dumps(
                    [spec.to_json() for spec in column_specs],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
        return _IcebergWriteSession(
            self,
            table,
            identity,
            table.schema().as_arrow(),
            column_specs=column_specs,
            rows=0,
            bytes_=0,
            sequence=0,
        )

    def restart(self, identity: RowsetIdentity, *, schema: Any) -> "_IcebergWriteSession":
        from pyiceberg.exceptions import NoSuchTableError

        identifier = (_NAMESPACE, _table_name(identity))
        try:
            self.catalog.drop_table(identifier)
        except NoSuchTableError:
            pass
        return self.begin(identity, schema=schema)

    def resume(
        self,
        descriptor: RowsetDescriptor,
        *,
        identity: RowsetIdentity | None = None,
    ) -> "_IcebergWriteSession":
        if descriptor.storage is not RowsetStorage.ICEBERG or not descriptor.table_identifier:
            raise ValueError("iceberg resume requires an Iceberg rowset descriptor")
        table = self.catalog.load_table(tuple(descriptor.table_identifier.split(".")))
        _require_snapshot(table, descriptor)
        return _IcebergWriteSession(
            self,
            table,
            identity or _identity_from_properties(table.properties),
            table.schema().as_arrow(),
            column_specs=_column_specs_from_properties(table.properties),
            rows=descriptor.rows,
            bytes_=descriptor.bytes,
            sequence=_snapshot_sequence(table, descriptor.snapshot_id),
            descriptor=descriptor,
        )

    def descriptor_for_checkpoint(self, checkpoint: Any) -> RowsetDescriptor:
        table = self.catalog.load_table(tuple(checkpoint.table_identifier.split(".")))
        snapshot = next(
            (item for item in table.snapshots() if item.snapshot_id == checkpoint.snapshot_id),
            None,
        )
        if snapshot is None:
            raise ValueError(f"Iceberg checkpoint snapshot is missing: {checkpoint.snapshot_id}")
        schema = table.schema().as_arrow()
        return RowsetDescriptor(
            storage=RowsetStorage.ICEBERG,
            uri=checkpoint.storage_uri,
            rows=checkpoint.rows,
            bytes=checkpoint.bytes,
            columns=tuple(schema.names),
            column_specs=_column_specs_from_properties(table.properties),
            schema_fingerprint=checkpoint.schema_fingerprint,
            snapshot_id=checkpoint.snapshot_id,
            table_identifier=checkpoint.table_identifier,
        )

    def open_reader(self, descriptor: RowsetDescriptor) -> "_IcebergReader":
        if descriptor.storage is not RowsetStorage.ICEBERG or not descriptor.table_identifier:
            raise ValueError("iceberg store cannot read a non-Iceberg descriptor")
        table = self.catalog.load_table(tuple(descriptor.table_identifier.split(".")))
        _require_snapshot(table, descriptor)
        return _IcebergReader(descriptor, table)


class _IcebergWriteSession:
    def __init__(
        self,
        store: IcebergRowsetStore,
        table: Any,
        identity: RowsetIdentity,
        schema: Any,
        *,
        column_specs: tuple[ColumnSpec, ...],
        rows: int,
        bytes_: int,
        sequence: int,
        descriptor: RowsetDescriptor | None = None,
    ) -> None:
        self.store = store
        self.table = table
        self.identity = identity
        self.schema = schema
        self.column_specs = column_specs
        self.rows = rows
        self.bytes = bytes_
        self.sequence = sequence
        self.descriptor = descriptor
        self._pending: list[Any] = []
        self._closed = False

    def append(self, batch: Any) -> None:
        import pyarrow as pa

        if self._closed:
            raise RuntimeError("rowset write session is closed")
        if isinstance(batch, pa.RecordBatch):
            table = pa.Table.from_batches([batch])
        elif isinstance(batch, pa.Table):
            table = batch
        else:
            raise TypeError(f"unsupported rowset batch: {type(batch)!r}")
        self._pending.append(table.cast(self.schema))

    def checkpoint(self, continuation: Mapping[str, Any]) -> RowsetDescriptor:
        import pyarrow as pa

        if self._closed:
            raise RuntimeError("rowset write session is closed")
        if not self._pending:
            if self.descriptor is None:
                raise ValueError("cannot checkpoint an empty Iceberg rowset")
            return self.descriptor
        data = pa.concat_tables(self._pending)
        sequence = self.sequence + 1
        committed_rows = self.rows + data.num_rows
        committed_bytes = self.bytes + data.nbytes
        properties = {
            **_identity_properties(self.identity),
            "zeta4s.sequence": str(sequence),
            "zeta4s.rows": str(committed_rows),
            "zeta4s.bytes": str(committed_bytes),
            "zeta4s.continuation-sha256": hashlib.sha256(
                json.dumps(dict(continuation), sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        }
        from pyiceberg.exceptions import CommitStateUnknownException

        try:
            self.table.append(data, snapshot_properties=properties)
        except CommitStateUnknownException:
            reloaded = self.store.catalog.load_table(self.table.name())
            if not _snapshot_with_properties(reloaded, properties):
                raise
            self.table = reloaded
        reloaded = self.store.catalog.load_table(self.table.name())
        snapshot = reloaded.current_snapshot()
        if snapshot is None or snapshot.summary.additional_properties.get("zeta4s.sequence") != str(sequence):
            raise RuntimeError("Iceberg checkpoint commit could not be verified after reload")
        self.table = reloaded
        self.sequence = sequence
        self.rows = committed_rows
        self.bytes = committed_bytes
        self._pending.clear()
        self.descriptor = RowsetDescriptor(
            storage=RowsetStorage.ICEBERG,
            uri=f"iceberg://{self.store.warehouse}/{'.'.join(self.table.name())}@{snapshot.snapshot_id}",
            rows=self.rows,
            bytes=self.bytes,
            columns=tuple(self.schema.names),
            column_specs=self.column_specs,
            schema_fingerprint=_schema_fingerprint(self.schema),
            snapshot_id=snapshot.snapshot_id,
            table_identifier=".".join(self.table.name()),
        )
        return self.descriptor

    def finish(self) -> RowsetDescriptor:
        descriptor = self.checkpoint({"final": True}) if self._pending else self.descriptor
        if descriptor is None:
            raise ValueError("cannot finish an empty Iceberg rowset")
        self._closed = True
        return descriptor

    def abort(self) -> None:
        self._pending.clear()
        self._closed = True


class _IcebergReader:
    def __init__(self, descriptor: RowsetDescriptor, table: Any) -> None:
        self.descriptor = descriptor
        self.table = table

    def iter_batches(self, *, batch_size: int, columns: list[str] | None = None) -> Iterator[Any]:
        if batch_size < 1:
            raise ValueError("rowset batch_size must be greater than zero")
        scan = self.table.scan(
            snapshot_id=self.descriptor.snapshot_id, selected_fields=tuple(columns) if columns else ("*",)
        )
        reader = scan.to_arrow_batch_reader()
        for batch in reader:
            for offset in range(0, batch.num_rows, batch_size):
                yield batch.slice(offset, batch_size)

    def iter_positioned_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
        after: Mapping[str, Any] | None = None,
    ) -> Iterator[PositionedRowsetBatch]:
        from pyiceberg.io.pyarrow import ArrowScan

        scan = self.table.scan(
            snapshot_id=self.descriptor.snapshot_id,
            selected_fields=tuple(columns) if columns else ("*",),
        )
        tasks = sorted(
            scan.plan_files(),
            key=lambda task: str(task.file.file_path),
        )
        resume_position = _position_tuple(after) if after is not None else None
        if after is not None and int(after.get("snapshot_id", -1)) != self.descriptor.snapshot_id:
            raise ValueError("Iceberg rowset continuation snapshot does not match descriptor")
        resume_seen = resume_position is None
        arrow_scan = ArrowScan(
            scan.table_metadata,
            scan.io,
            scan.projection(),
            scan.row_filter,
            scan.case_sensitive,
            scan.limit,
        )
        for task in tasks:
            task_key = (
                str(task.file.file_path),
                0,
                int(task.file.file_size_in_bytes),
            )
            for batch_index, batch in enumerate(arrow_scan.to_record_batches([task])):
                for slice_index, offset in enumerate(range(0, batch.num_rows, batch_size)):
                    position = (*task_key, batch_index, slice_index)
                    if not resume_seen:
                        if position == resume_position:
                            resume_seen = True
                            continue
                        if position < resume_position:
                            continue
                        raise ValueError("Iceberg rowset continuation position is missing from snapshot")
                    continuation = {
                        "storage": "iceberg",
                        "snapshot_id": self.descriptor.snapshot_id,
                        "data_file": task_key[0],
                        "start": task_key[1],
                        "length": task_key[2],
                        "batch_index": batch_index,
                        "slice_index": slice_index,
                    }
                    yield PositionedRowsetBatch(batch.slice(offset, batch_size), continuation)
        if not resume_seen:
            raise ValueError("Iceberg rowset continuation position is missing from snapshot")


def _table_name(identity: RowsetIdentity) -> str:
    value = "\0".join((identity.project_id, identity.job_id, identity.run_id, identity.step_id, identity.output_name))
    return f"rowset_{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _identity_properties(identity: RowsetIdentity) -> dict[str, str]:
    return {
        "zeta4s.project-id": identity.project_id,
        "zeta4s.job-id": identity.job_id,
        "zeta4s.run-id": identity.run_id,
        "zeta4s.step-id": identity.step_id,
        "zeta4s.attempt": str(identity.attempt),
        "zeta4s.output-name": identity.output_name,
    }


def _identity_from_properties(properties: Mapping[str, str]) -> RowsetIdentity:
    try:
        return RowsetIdentity(
            project_id=properties["zeta4s.project-id"],
            job_id=properties["zeta4s.job-id"],
            run_id=properties["zeta4s.run-id"],
            step_id=properties["zeta4s.step-id"],
            attempt=int(properties["zeta4s.attempt"]),
            output_name=properties["zeta4s.output-name"],
        )
    except KeyError as exc:
        raise ValueError(f"Iceberg rowset table is missing identity property: {exc.args[0]}") from exc


def _require_snapshot(table: Any, descriptor: RowsetDescriptor) -> None:
    snapshot = next((item for item in table.snapshots() if item.snapshot_id == descriptor.snapshot_id), None)
    if snapshot is None:
        raise ValueError(f"Iceberg checkpoint snapshot is missing: {descriptor.snapshot_id}")
    if _schema_fingerprint(table.schema().as_arrow()) != descriptor.schema_fingerprint:
        raise ValueError("Iceberg checkpoint schema fingerprint mismatch")


def _snapshot_sequence(table: Any, snapshot_id: int | None) -> int:
    snapshot = next(item for item in table.snapshots() if item.snapshot_id == snapshot_id)
    return int(snapshot.summary.additional_properties["zeta4s.sequence"])


def _snapshot_with_properties(table: Any, properties: Mapping[str, str]) -> Any | None:
    return next(
        (
            snapshot
            for snapshot in table.snapshots()
            if all(snapshot.summary.additional_properties.get(key) == value for key, value in properties.items())
        ),
        None,
    )


def _schema_fingerprint(schema: Any) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _column_specs(schema: Any) -> tuple[ColumnSpec, ...]:
    raw = (schema.metadata or {}).get(ROWSET_COLUMN_SPECS_METADATA_KEY)
    if not raw:
        return ()
    value = json.loads(raw.decode("utf-8"))
    return tuple(ColumnSpec.from_value(spec) for spec in value)


def _column_specs_from_properties(properties: Mapping[str, str]) -> tuple[ColumnSpec, ...]:
    value = json.loads(properties.get("zeta4s.column-specs", "[]"))
    if not isinstance(value, list):
        raise ValueError("Iceberg rowset column specs property must be a list")
    return tuple(ColumnSpec.from_value(spec) for spec in value)


def _position_tuple(value: Mapping[str, Any]) -> tuple[str, int, int, int, int]:
    required = ("data_file", "start", "length", "batch_index", "slice_index")
    missing = [key for key in required if key not in value]
    if missing:
        raise ValueError(f"Iceberg rowset continuation is incomplete: {', '.join(missing)}")
    return (
        str(value["data_file"]),
        int(value["start"]),
        int(value["length"]),
        int(value["batch_index"]),
        int(value["slice_index"]),
    )


__all__ = ["IcebergRowsetStore"]
