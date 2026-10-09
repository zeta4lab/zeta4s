"""Verification-only local Parquet rowset storage."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_models import (
    ResumeCapability,
    RowsetDescriptor,
    RowsetIdentity,
    RowsetStorage,
)
from zeta4s.runtime.rowset_store import CheckpointNotSupported
from zeta4s.runtime.rowset_store import PositionedRowsetBatch
from zeta4s.runtime.source_reader import ColumnSpec


class ParquetRowsetStore:
    resume_capability = ResumeCapability.RESTART_ONLY

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def begin(self, identity: RowsetIdentity, *, schema: Any) -> "_ParquetWriteSession":
        return _ParquetWriteSession(self.root, identity, schema)

    def resume(
        self,
        descriptor: RowsetDescriptor,
        *,
        identity: RowsetIdentity | None = None,
    ) -> "_ParquetWriteSession":
        del descriptor, identity
        raise CheckpointNotSupported("local parquet rowsets do not support resume")

    def open_reader(self, descriptor: RowsetDescriptor) -> "_ParquetReader":
        if descriptor.storage is not RowsetStorage.PARQUET:
            raise ValueError(f"parquet store cannot read storage={descriptor.storage.value}")
        path = _file_uri_path(descriptor.uri)
        if not path.is_file():
            raise ValueError(f"rowset path does not exist: {path}")
        return _ParquetReader(descriptor, path)


class _ParquetWriteSession:
    def __init__(self, root: Path, identity: RowsetIdentity, schema: Any) -> None:
        import pyarrow.parquet as pq

        self.identity = identity
        self.schema = schema
        self.rows = 0
        self._finished: RowsetDescriptor | None = None
        digest = hashlib.sha256(
            "\0".join(
                (
                    identity.project_id,
                    identity.job_id,
                    identity.run_id,
                    identity.step_id,
                    str(identity.attempt),
                    identity.output_name,
                )
            ).encode("utf-8")
        ).hexdigest()
        self.path = root / "rowsets" / digest[:2] / f"{digest}.parquet"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp_path = self.path.with_suffix(".parquet.tmp")
        self.tmp_path.unlink(missing_ok=True)
        self._writer = pq.ParquetWriter(self.tmp_path, schema, compression="zstd")

    def append(self, batch: Any) -> None:
        import pyarrow as pa

        if self._finished is not None or self._writer is None:
            raise RuntimeError("rowset write session is closed")
        if isinstance(batch, pa.RecordBatch):
            table = pa.Table.from_batches([batch])
        elif isinstance(batch, pa.Table):
            table = batch
        else:
            raise TypeError(f"unsupported rowset batch: {type(batch)!r}")
        table = table.cast(self.schema)
        self._writer.write_table(table)
        self.rows += table.num_rows

    def checkpoint(self, continuation: Mapping[str, Any]) -> RowsetDescriptor:
        del continuation
        raise CheckpointNotSupported("local parquet rowsets do not support checkpoints")

    def finish(self) -> RowsetDescriptor:
        if self._finished is not None:
            return self._finished
        if self._writer is None:
            raise RuntimeError("rowset write session is closed")
        self._writer.close()
        self._writer = None
        self.tmp_path.replace(self.path)
        self._finished = RowsetDescriptor(
            storage=RowsetStorage.PARQUET,
            uri=self.path.resolve().as_uri(),
            rows=self.rows,
            bytes=self.path.stat().st_size,
            columns=tuple(str(name) for name in self.schema.names),
            column_specs=_column_specs(self.schema),
            schema_fingerprint=_schema_fingerprint(self.schema),
        )
        return self._finished

    def abort(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self.tmp_path.unlink(missing_ok=True)


class _ParquetReader:
    def __init__(self, descriptor: RowsetDescriptor, path: Path) -> None:
        self.descriptor = descriptor
        self.path = path

    def iter_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
    ) -> Iterator[Any]:
        import pyarrow.parquet as pq

        if batch_size < 1:
            raise ValueError("rowset batch_size must be greater than zero")
        parquet_file = pq.ParquetFile(self.path)
        yield from parquet_file.iter_batches(batch_size=batch_size, columns=columns)

    def iter_positioned_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
        after: Mapping[str, Any] | None = None,
    ) -> Iterator[PositionedRowsetBatch]:
        if after is not None:
            raise CheckpointNotSupported("local parquet rowsets do not support positioned resume")
        for batch_index, batch in enumerate(self.iter_batches(batch_size=batch_size, columns=columns)):
            yield PositionedRowsetBatch(
                batch=batch,
                continuation={
                    "storage": "parquet",
                    "file_uri": self.descriptor.uri,
                    "batch_index": batch_index,
                },
            )


def _schema_fingerprint(schema: Any) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _column_specs(schema: Any) -> tuple[ColumnSpec, ...]:
    metadata = schema.metadata or {}
    raw = metadata.get(ROWSET_COLUMN_SPECS_METADATA_KEY)
    if not raw:
        return ()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("rowset column_specs metadata is invalid") from exc
    if not isinstance(value, list):
        raise ValueError("rowset column_specs metadata must be a list")
    return tuple(ColumnSpec.from_value(spec) for spec in value)


def _file_uri_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise ValueError(f"parquet rowset requires local file URI: {uri}")
    return Path(unquote(parsed.path))


__all__ = ["ParquetRowsetStore"]
