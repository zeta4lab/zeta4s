from __future__ import annotations

from datetime import datetime
import unittest

from zeta4s.metastore.backends.clickhouse import ClickHouseStepCheckpointRepository
from zeta4s.metastore.backends.postgres import PostgresMetastoreAdapter
from zeta4s.metastore.contracts import StepCheckpoint

from test_postgres_metastore import FakeConnect


def _checkpoint(*, sequence: int = 1, attempt: int = 1, snapshot_id: int = 101) -> StepCheckpoint:
    return StepCheckpoint(
        project_id="retail",
        job_id="orders",
        run_id="run-1",
        step_id="extract_orders",
        task_id="extract_orders",
        attempt=attempt,
        sequence=sequence,
        unit_id="orders_rows",
        storage_uri="iceberg://zeta4s_checkpoint/r_abc",
        table_identifier="zeta4s_checkpoint.r_abc",
        snapshot_id=snapshot_id,
        continuation={"pit_id": "pit-1", "search_after": [sequence]},
        schema_fingerprint="schema-1",
        rows=sequence * 100,
        bytes=sequence * 1024,
        created_at="2026-07-11T00:00:00+00:00",
    )


def _postgres_row(checkpoint: StepCheckpoint) -> dict[str, object]:
    return {
        **checkpoint.__dict__,
        "created_at": datetime.fromisoformat(checkpoint.created_at),
    }


def _clickhouse_row(checkpoint: StepCheckpoint) -> tuple[object, ...]:
    import json

    return (
        checkpoint.project_id,
        checkpoint.job_id,
        checkpoint.run_id,
        checkpoint.step_id,
        checkpoint.task_id,
        checkpoint.attempt,
        checkpoint.sequence,
        checkpoint.unit_id,
        checkpoint.storage_uri,
        checkpoint.table_identifier,
        checkpoint.snapshot_id,
        json.dumps(checkpoint.continuation, sort_keys=True),
        checkpoint.schema_fingerprint,
        checkpoint.rows,
        checkpoint.bytes,
        datetime.fromisoformat(checkpoint.created_at),
        1,
    )


class PostgresStepCheckpointRepositoryTest(unittest.TestCase):
    def test_append_uses_immutable_business_key_and_returns_identical_record(self) -> None:
        checkpoint = _checkpoint()
        connect = FakeConnect(results=[[_postgres_row(checkpoint)]])
        repository = PostgresMetastoreAdapter(connect=connect).step_checkpoint_repository

        repository.append_checkpoint(checkpoint)

        sql, parameters = connect.connection.statements[0]
        normalized_sql = " ".join(sql.split())
        self.assertIn(
            "ON CONFLICT ( project_id, job_id, run_id, step_id, task_id, unit_id, sequence )",
            normalized_sql,
        )
        self.assertIn("RETURNING", normalized_sql)
        self.assertEqual(parameters["snapshot_id"], 101)

    def test_append_rejects_conflicting_record(self) -> None:
        repository = PostgresMetastoreAdapter(connect=FakeConnect(results=[[]])).step_checkpoint_repository

        with self.assertRaisesRegex(ValueError, "conflicting step checkpoint"):
            repository.append_checkpoint(_checkpoint())

    def test_latest_checkpoint_returns_contract_dataclass(self) -> None:
        checkpoint = _checkpoint(sequence=2, attempt=2, snapshot_id=202)
        connect = FakeConnect(results=[[_postgres_row(checkpoint)]])
        repository = PostgresMetastoreAdapter(connect=connect).step_checkpoint_repository

        result = repository.latest_checkpoint(
            project_id="retail",
            job_id="orders",
            run_id="run-1",
            step_id="extract_orders",
            task_id="extract_orders",
            unit_id="orders_rows",
        )

        self.assertEqual(result, checkpoint)
        self.assertIn("ORDER BY sequence DESC", connect.connection.statements[0][0])


class _ClickHouseResult:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.result_rows = rows


class _ClickHouseClient:
    def __init__(self, result_sets: list[list[tuple[object, ...]]]) -> None:
        self.result_sets = list(result_sets)
        self.queries: list[tuple[str, dict | None]] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def query(self, sql: str, parameters: dict | None = None) -> _ClickHouseResult:
        self.queries.append((sql, parameters))
        rows = self.result_sets.pop(0) if self.result_sets else []
        return _ClickHouseResult(rows)

    def insert(self, table: str, rows: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append((table, rows, column_names))


class _ClickHouseAdapter:
    def __init__(self, client: _ClickHouseClient) -> None:
        self._client = client

    def client(self) -> _ClickHouseClient:
        return self._client


class ClickHouseStepCheckpointRepositoryTest(unittest.TestCase):
    def test_identical_append_is_idempotent(self) -> None:
        checkpoint = _checkpoint()
        client = _ClickHouseClient([[_clickhouse_row(checkpoint)]])
        repository = ClickHouseStepCheckpointRepository(_ClickHouseAdapter(client))

        repository.append_checkpoint(checkpoint)

        self.assertEqual(client.inserts, [])

    def test_conflicting_append_is_rejected(self) -> None:
        existing = _checkpoint(snapshot_id=999)
        client = _ClickHouseClient([[_clickhouse_row(existing)]])
        repository = ClickHouseStepCheckpointRepository(_ClickHouseAdapter(client))

        with self.assertRaisesRegex(ValueError, "conflicting step checkpoint"):
            repository.append_checkpoint(_checkpoint())

    def test_new_append_and_list_preserve_sequence_and_attempt(self) -> None:
        checkpoint = _checkpoint(sequence=2, attempt=2, snapshot_id=202)
        client = _ClickHouseClient([[], [_clickhouse_row(checkpoint)]])
        repository = ClickHouseStepCheckpointRepository(_ClickHouseAdapter(client))

        repository.append_checkpoint(checkpoint)
        result = repository.list_checkpoints(
            project_id="retail",
            job_id="orders",
            run_id="run-1",
            step_id="extract_orders",
        )

        self.assertEqual(result, [checkpoint])
        self.assertEqual(client.inserts[0][1][0][5:8], [2, 2, "orders_rows"])


if __name__ == "__main__":
    unittest.main()
