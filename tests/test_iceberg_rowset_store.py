from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import uuid

import pyarrow as pa
import pytest

from zeta4s.metastore.contracts import StepCheckpoint
from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.checkpoints import (
    CheckpointCorruptionError,
    commit_step_checkpoint,
    load_verified_checkpoint,
)
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetIdentity
from zeta4s.runtime.rowsets import ResolvedRowsetRef
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.rowset_stores.iceberg import IcebergRowsetStore
from zeta4s.runtime.backends.clickhouse.stage import stage_clickhouse_rowset
from zeta4s.runtime.source_reader import ColumnSpec


pytestmark = pytest.mark.skipif(
    not os.environ.get("ZETA4S_TEST_ICEBERG_CATALOG_URI"),
    reason="requires the checkpoint Docker profile",
)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ZETA4S_ICEBERG_CATALOG_URI", os.environ["ZETA4S_TEST_ICEBERG_CATALOG_URI"])
    monkeypatch.setenv("ZETA4S_ICEBERG_WAREHOUSE", "warehouse")
    monkeypatch.setenv("ZETA4S_ICEBERG_CATALOG_TOKEN", "dummy")
    value = IcebergRowsetStore.from_environment()
    yield value


def _identity() -> RowsetIdentity:
    suffix = uuid.uuid4().hex
    return RowsetIdentity("project", "job", f"run-{suffix}", "extract", 1, "rows")


def test_append_checkpoint_reload_and_snapshot_reader(store: IcebergRowsetStore) -> None:
    identity = _identity()
    specs = (
        ColumnSpec.from_type("id", "int64", True, source_backend="oracle", source_type="NUMBER"),
        ColumnSpec.from_type("value", "string", True),
    )
    schema = pa.schema(
        [("id", pa.int64()), ("value", pa.string())],
        metadata={ROWSET_COLUMN_SPECS_METADATA_KEY: json.dumps([spec.to_json() for spec in specs]).encode("utf-8")},
    )
    session = store.begin(identity, schema=schema)
    try:
        session.append(pa.table({"id": [1, 2], "value": ["a", "b"]}, schema=schema))
        first = session.checkpoint({"cursor": "two"})
        session.append(pa.table({"id": [3], "value": ["c"]}, schema=schema))
        second = session.checkpoint({"cursor": "three"})

        assert first.snapshot_id != second.snapshot_id
        table = store.catalog.load_table(tuple(second.table_identifier.split(".")))
        assert {first.snapshot_id, second.snapshot_id}.issubset(
            {snapshot.snapshot_id for snapshot in table.snapshots()}
        )
        rows = [row for batch in store.open_reader(first).iter_batches(batch_size=1) for row in batch.to_pylist()]
        assert rows == [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        resumed = store.resume(first)
        assert resumed.identity == identity
        assert resumed.sequence == 1
        assert first.column_specs == specs
        assert resumed.column_specs == specs
    finally:
        _drop(store, identity)


def test_append_rejects_schema_mismatch(store: IcebergRowsetStore) -> None:
    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    session = store.begin(identity, schema=schema)
    try:
        with pytest.raises((ValueError, TypeError, pa.ArrowInvalid)):
            session.append(pa.table({"other": [1]}))
    finally:
        _drop(store, identity)


def test_missing_snapshot_fails_closed(store: IcebergRowsetStore) -> None:
    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    session = store.begin(identity, schema=schema)
    try:
        session.append(pa.table({"id": [1]}, schema=schema))
        descriptor = session.checkpoint({"cursor": "one"})
        missing = RowsetDescriptor(**{**descriptor.__dict__, "snapshot_id": descriptor.snapshot_id + 1})
        with pytest.raises(ValueError, match="snapshot is missing"):
            store.open_reader(missing)
    finally:
        _drop(store, identity)


def test_orphan_snapshot_is_ignored_by_checkpoint_selection(store: IcebergRowsetStore) -> None:
    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    repository = _CheckpointRepository()
    session = store.begin(identity, schema=schema)
    try:
        session.append(pa.table({"id": [1]}, schema=schema))
        committed = commit_step_checkpoint(session, {"cursor": "one"}, repository)
        session.append(pa.table({"id": [2]}, schema=schema))
        orphan = session.checkpoint({"cursor": "two"})

        verified = load_verified_checkpoint(identity, repository, store)
        assert verified == committed
        assert verified.snapshot_id != orphan.snapshot_id
    finally:
        _drop(store, identity)


def test_metastore_reference_to_missing_snapshot_fails_closed(store: IcebergRowsetStore) -> None:
    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    repository = _CheckpointRepository()
    session = store.begin(identity, schema=schema)
    try:
        session.append(pa.table({"id": [1]}, schema=schema))
        checkpoint = commit_step_checkpoint(session, {"cursor": "one"}, repository)
        repository.checkpoint = StepCheckpoint(**{**checkpoint.__dict__, "snapshot_id": checkpoint.snapshot_id + 1})
        with pytest.raises(CheckpointCorruptionError, match="snapshot is missing"):
            load_verified_checkpoint(identity, repository, store)
    finally:
        _drop(store, identity)


def test_stale_resumed_writer_rejects_optimistic_conflict(store: IcebergRowsetStore) -> None:
    from pyiceberg.exceptions import CommitFailedException

    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    initial = store.begin(identity, schema=schema)
    try:
        initial.append(pa.table({"id": [1]}, schema=schema))
        first = initial.checkpoint({"cursor": "one"})
        winner = store.resume(first)
        stale = store.resume(first)
        winner.append(pa.table({"id": [2]}, schema=schema))
        winner.checkpoint({"cursor": "two"})
        stale.append(pa.table({"id": [3]}, schema=schema))
        with pytest.raises(CommitFailedException):
            stale.checkpoint({"cursor": "three"})
    finally:
        _drop(store, identity)


def test_unknown_commit_is_reconciled_only_after_snapshot_reload(
    store: IcebergRowsetStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyiceberg.exceptions import CommitStateUnknownException

    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    session = store.begin(identity, schema=schema)
    try:
        original_append = session.table.append

        def commit_then_lose_response(*args: object, **kwargs: object) -> None:
            original_append(*args, **kwargs)
            raise CommitStateUnknownException("response lost after commit")

        monkeypatch.setattr(session.table, "append", commit_then_lose_response)
        session.append(pa.table({"id": [1]}, schema=schema))
        descriptor = session.checkpoint({"cursor": "one"})
        reloaded = store.catalog.load_table(tuple(descriptor.table_identifier.split(".")))
        assert descriptor.snapshot_id == reloaded.current_snapshot().snapshot_id
    finally:
        _drop(store, identity)


def test_positioned_reader_resumes_after_immutable_file_position(store: IcebergRowsetStore) -> None:
    identity = _identity()
    schema = pa.schema([("id", pa.int64())])
    session = store.begin(identity, schema=schema)
    try:
        session.append(pa.table({"id": [1, 2, 3]}, schema=schema))
        descriptor = session.finish()
        first_reader = store.open_reader(descriptor)
        first = next(first_reader.iter_positioned_batches(batch_size=1))
        resumed = list(
            store.open_reader(descriptor).iter_positioned_batches(
                batch_size=1,
                after=first.continuation,
            )
        )
        assert first.batch.to_pylist() == [{"id": 1}]
        assert [row for item in resumed for row in item.batch.to_pylist()] == [{"id": 2}, {"id": 3}]
        assert all(item.continuation["snapshot_id"] == descriptor.snapshot_id for item in resumed)
        invalid = {**first.continuation, "snapshot_id": descriptor.snapshot_id + 1}
        with pytest.raises(ValueError, match="snapshot does not match"):
            list(store.open_reader(descriptor).iter_positioned_batches(batch_size=1, after=invalid))
    finally:
        _drop(store, identity)


def test_clickhouse_stage_is_equivalent_for_parquet_and_iceberg(
    store: IcebergRowsetStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    specs = [ColumnSpec.from_type("id", "Int64", False)]
    schema = pa.schema(
        [("id", pa.int64())],
        metadata={ROWSET_COLUMN_SPECS_METADATA_KEY: json.dumps([spec.to_json() for spec in specs]).encode()},
    )
    identity = _identity()
    iceberg_session = store.begin(identity, schema=schema)
    iceberg_session.append(pa.table({"id": [1, 2, 3]}, schema=schema))
    iceberg_descriptor = iceberg_session.finish()
    try:
        with TemporaryDirectory() as tmp:
            parquet_store = ParquetRowsetStore(Path(tmp))
            parquet_session = parquet_store.begin(identity, schema=schema)
            parquet_session.append(pa.table({"id": [1, 2, 3]}, schema=schema))
            parquet_descriptor = parquet_session.finish()
            outcomes = []
            for descriptor, source_store in (
                (parquet_descriptor, parquet_store),
                (iceberg_descriptor, store),
            ):
                client = _StageClient()
                monkeypatch.setattr(
                    "zeta4s.runtime.backends.clickhouse.stage.get_clickhouse_runtime_client",
                    lambda *args, _client=client, **kwargs: _client,
                )
                rowset = ResolvedRowsetRef(
                    "extract.rows",
                    "extract",
                    "rows",
                    descriptor,
                    source_store.open_reader(descriptor),
                )
                loaded, target = stage_clickhouse_rowset(
                    rowset=rowset,
                    stage_conn="target",
                    target_table="rows",
                    target_namespace="stage",
                )
                outcomes.append((loaded, target, client.create_sql, len(client.inserts)))
            assert outcomes[0] == outcomes[1]
            assert outcomes[0][0] == 3
    finally:
        _drop(store, identity)


class _StageClient:
    def __init__(self) -> None:
        self.create_sql = ""
        self.inserts = []

    def command(self, sql: str) -> None:
        if sql.startswith("CREATE TABLE"):
            self.create_sql = sql

    def query(self, sql: str):
        return SimpleNamespace(first_row=(0,))

    def raw_insert(self, table, *, column_names, insert_block, fmt):
        self.inserts.append((table, tuple(column_names), len(insert_block), fmt))


class _CheckpointRepository:
    checkpoint: StepCheckpoint | None = None

    def append_checkpoint(self, checkpoint: StepCheckpoint) -> None:
        self.checkpoint = checkpoint

    def latest_checkpoint(self, **_: object) -> StepCheckpoint | None:
        return self.checkpoint


def _drop(store: IcebergRowsetStore, identity: RowsetIdentity) -> None:
    from zeta4s.runtime.rowset_stores.iceberg import _NAMESPACE, _table_name

    try:
        store.catalog.drop_table((_NAMESPACE, _table_name(identity)))
    except Exception:
        pass
