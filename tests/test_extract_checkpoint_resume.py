from __future__ import annotations

import os
from unittest.mock import patch

from types import SimpleNamespace

import pytest

from zeta4s.runtime.backends.clickhouse.extract import ClickHouseSelectReader
from zeta4s.runtime.backends.elasticsearch.extract import ElasticsearchSearchReader
from zeta4s.runtime.backends.oracle.extract import OracleSelectReader
from zeta4s.runtime.rowset_models import ResumeCapability
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetIdentity, RowsetStorage
from zeta4s.runtime.rowset_store import CheckpointPolicy
from zeta4s.runtime.rowset_extract import write_reader_to_rowset
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch
from zeta4s.runtime.input_checkpoints import append_input_checkpoint, load_input_position
from zeta4s.runtime.checkpoints import CheckpointCorruptionError


@pytest.mark.parametrize(
    ("reader", "source_kind"),
    [
        (ClickHouseSelectReader.__new__(ClickHouseSelectReader), "clickhouse"),
        (OracleSelectReader.__new__(OracleSelectReader), "oracle"),
    ],
)
def test_restart_only_reader_rejects_every_continuation(reader: object, source_kind: str) -> None:
    assert reader.resume_capability is ResumeCapability.RESTART_ONLY
    with pytest.raises(ValueError, match="does not support continuation"):
        list(reader.read_batches(after={"rows": 100}))
    assert source_kind in str(reader.source_kind if hasattr(reader, "source_kind") else source_kind)


def test_elasticsearch_exact_reader_emits_post_batch_pit_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        {"pit_id": "pit-next", "hits": {"hits": [{"_source": {"id": 1}, "sort": [1, "shard"]}]}},
        {"pit_id": "pit-next", "hits": {"hits": []}},
    ]
    monkeypatch.setattr(
        "zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_json_request",
        lambda *args, **kwargs: responses.pop(0),
    )
    reader = _elasticsearch_reader()
    reader._pit_id = "pit-original"

    batches = list(reader.read_batches())

    assert reader.resume_capability is ResumeCapability.EXACT
    assert batches[0].continuation == {"pit_id": "pit-next", "search_after": [1, "shard"]}


@pytest.mark.parametrize("after", [{"rows": 10}, {"pit_id": "pit"}, {"search_after": [1]}])
def test_elasticsearch_exact_reader_rejects_incomplete_or_row_count_token(after: dict) -> None:
    reader = _elasticsearch_reader()
    reader._pit_id = "new-pit"
    with pytest.raises(ValueError, match="requires pit_id"):
        list(reader.read_batches(after=after))


def test_elasticsearch_resume_uses_fresh_pit_with_checkpoint_search_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = []
    responses = [
        {"pit_id": "fresh-pit-next", "hits": {"hits": [{"_source": {"id": 2}, "sort": [2, "shard"]}]}},
        {"pit_id": "fresh-pit-next", "hits": {"hits": []}},
    ]

    monkeypatch.setattr(
        "zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_json_request",
        lambda *args, **kwargs: requests.append(kwargs["body"]) or responses.pop(0),
    )

    monkeypatch.setattr(
        "zeta4s.runtime.backends.elasticsearch.extract._close_pit",
        lambda *args, **kwargs: None,
    )
    reader = _elasticsearch_reader()
    reader._pit_id = "fresh-pit"

    batches = list(reader.read_batches(after={"pit_id": "expired-pit", "search_after": [1]}))

    assert batches[0].rows == [(2,)]
    assert requests[0]["pit"]["id"] == "fresh-pit"
    assert requests[0]["search_after"] == [1]


def _elasticsearch_reader() -> ElasticsearchSearchReader:
    field = {"name": "id", "path": "id", "type": "int"}
    return ElasticsearchSearchReader(
        conn=SimpleNamespace(base_url="http://elasticsearch", headers={}),
        index="items",
        fields=[field],
        raw_columns=[field],
        fields_by_column={"id": field},
        query={"match_all": {}},
        sort=[{"_shard_doc": "asc"}],
        batch_size=10,
        track_total_hits=False,
    )


def test_exact_writer_commits_at_batch_threshold_and_finalization() -> None:
    reader = _ExactReader(with_continuation=True)
    store = _FakeStore()
    repository = _FakeCheckpointRepository()
    result = write_reader_to_rowset(
        reader=reader,
        store=store,
        identity=RowsetIdentity("project", "job", "run", "step", 1, "rows"),
        checkpoint_repository=repository,
        checkpoint_policy=CheckpointPolicy(target_bytes=1, max_interval_seconds=60),
    )
    assert result.rows == 2
    assert [checkpoint.sequence for checkpoint in repository.items] == [1, 2]
    assert [checkpoint.continuation for checkpoint in repository.items] == [{"cursor": 1}, {"cursor": 2}]


def test_exact_writer_rejects_nonempty_batch_without_continuation() -> None:
    with pytest.raises(ValueError, match="without continuation"):
        write_reader_to_rowset(
            reader=_ExactReader(with_continuation=False),
            store=_FakeStore(),
            identity=RowsetIdentity("project", "job", "run", "step", 1, "rows"),
        )


class _ExactReader:
    source_kind = "test"
    source_object = "test.rows"
    resume_capability = ResumeCapability.EXACT
    column_specs = [ColumnSpec.from_type("id", "Int64", False)]
    columns = ["id"]

    def __init__(self, *, with_continuation: bool) -> None:
        self.with_continuation = with_continuation

    def read_batches(self, *, after=None):
        del after
        for value in (1, 2):
            continuation = {"cursor": value} if self.with_continuation else None
            yield SourceBatch(rows=[(value,)], continuation=continuation)


class _FakeSession:
    def __init__(self, identity: RowsetIdentity, schema) -> None:
        self.identity = identity
        self.schema = schema
        self.sequence = 0
        self.rows = 0
        self.bytes = 0
        self.pending = 0
        self.descriptor = None

    def append(self, table) -> None:
        self.pending += table.num_rows
        self.bytes += table.nbytes

    def checkpoint(self, continuation) -> RowsetDescriptor:
        del continuation
        self.sequence += 1
        self.rows += self.pending
        self.pending = 0
        self.descriptor = RowsetDescriptor(
            RowsetStorage.ICEBERG,
            f"iceberg://test/{self.sequence}",
            self.rows,
            self.bytes,
            tuple(self.schema.names),
            tuple(_ExactReader.column_specs),
            "fingerprint",
            snapshot_id=self.sequence,
            table_identifier="namespace.table",
        )
        return self.descriptor

    def finish(self) -> RowsetDescriptor:
        return self.descriptor or self.checkpoint({})

    def abort(self) -> None:
        self.pending = 0


class _FakeStore:
    def begin(self, identity, *, schema):
        return _FakeSession(identity, schema)


class _FakeCheckpointRepository:
    def __init__(self) -> None:
        self.items = []

    def latest_checkpoint(self, **kwargs):
        for checkpoint in reversed(self.items):
            if all(getattr(checkpoint, key) == value for key, value in kwargs.items()):
                return checkpoint
        return None

    def append_checkpoint(self, checkpoint) -> None:
        self.items.append(checkpoint)


def test_input_checkpoint_validates_snapshot_and_target_receipt() -> None:
    descriptor = RowsetDescriptor(
        RowsetStorage.ICEBERG,
        "iceberg://warehouse/table@1",
        10,
        100,
        ("id",),
        tuple(_ExactReader.column_specs),
        "fingerprint",
        snapshot_id=1,
        table_identifier="namespace.table",
    )
    rowset = SimpleNamespace(descriptor=descriptor)
    repository = _FakeCheckpointRepository()
    position = {"data_file": "s3://bucket/file.parquet", "start": 0, "length": 10, "batch_index": 0, "slice_index": 0}
    append_input_checkpoint(
        rowset=rowset,
        repository=repository,
        project_id="project",
        job_id="job",
        run_id="run",
        step_id="step",
        task_id="task",
        unit_id="source.rows",
        attempt=1,
        position=position,
        receipt={"target_type": "oracle", "target_table": "TARGET", "mode": "replace"},
    )
    loaded = load_input_position(
        rowset=rowset,
        repository=repository,
        project_id="project",
        job_id="job",
        run_id="run",
        step_id="step",
        task_id="task",
        unit_id="source.rows",
        expected_receipt={"target_type": "oracle", "target_table": "TARGET", "mode": "replace"},
    )
    assert loaded == position
    with pytest.raises(CheckpointCorruptionError, match="target receipt mismatch"):
        load_input_position(
            rowset=rowset,
            repository=repository,
            project_id="project",
            job_id="job",
            run_id="run",
            step_id="step",
            task_id="task",
            unit_id="source.rows",
            expected_receipt={"target_table": "OTHER"},
        )


def test_checkpoint_policy_reads_platform_environment() -> None:
    with patch.dict(
        os.environ,
        {
            "ZETA4S_ROWSET_CHECKPOINT_TARGET_BYTES": "64",
            "ZETA4S_ROWSET_CHECKPOINT_MAX_INTERVAL_SECONDS": "7",
        },
    ):
        policy = CheckpointPolicy.from_environment()

    assert policy == CheckpointPolicy(target_bytes=64, max_interval_seconds=7)
