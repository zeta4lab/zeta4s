from __future__ import annotations

import unittest

from zeta4s.core import StepOutputBinding, step_output_binding_key
from zeta4s.runtime.metastore_artifacts import MetastoreArtifactStore


class _FakeStepOutputBindingRepository:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []

    def get_binding(self, **kwargs):
        self.calls.append(kwargs)
        for row in reversed(self.rows):
            if all(row.get(key) == value for key, value in kwargs.items()):
                return row
        return None


class _InvalidStepOutputBindingRepository:
    def get_binding(self, **kwargs):
        return {"binding": {"value": 1}}


class MetastoreArtifactStoreTest(unittest.TestCase):
    def test_reads_step_output_binding_from_metastore_repository(self) -> None:
        repository = _FakeStepOutputBindingRepository(
            [
                {
                    "project_id": "retail",
                    "job_id": "daily",
                    "run_id": "run_1",
                    "step_id": "stage_orders",
                    "output_name": "mart.orders",
                    "output_kind": "table",
                    "binding": {
                        "step_id": "stage_orders",
                        "output_name": "mart.orders",
                        "kind": "table",
                        "value": {"kind": "table", "conn": "analytics", "table": "mart.orders"},
                        "table_ref": {"conn": "analytics", "table": "mart.orders"},
                        "ref": {"kind": "table"},
                    },
                }
            ]
        )
        store = MetastoreArtifactStore(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            repository=repository,
        )

        binding = store.get(step_output_binding_key("stage_orders", "mart.orders"))

        self.assertIsInstance(binding, StepOutputBinding)
        self.assertEqual(binding.step_id, "stage_orders")
        self.assertEqual(binding.output_name, "mart.orders")
        self.assertEqual(binding.kind, "table")
        self.assertEqual(binding.value, {"kind": "table", "conn": "analytics", "table": "mart.orders"})
        self.assertIsNotNone(binding.table_ref)
        assert binding.table_ref is not None
        self.assertEqual(binding.table_ref.conn, "analytics")
        self.assertEqual(binding.table_ref.table, "mart.orders")
        self.assertEqual(binding.ref, {"kind": "table"})
        self.assertEqual(
            repository.calls,
            [
                {
                    "project_id": "retail",
                    "job_id": "daily",
                    "run_id": "run_1",
                    "step_id": "stage_orders",
                    "output_name": "mart.orders",
                }
            ],
        )

    def test_caches_repository_binding_after_first_read(self) -> None:
        repository = _FakeStepOutputBindingRepository(
            [
                {
                    "project_id": "retail",
                    "job_id": "daily",
                    "run_id": "run_1",
                    "step_id": "count_orders",
                    "output_name": "row_count",
                    "output_kind": "scalar",
                    "binding": {
                        "step_id": "count_orders",
                        "output_name": "row_count",
                        "kind": "scalar",
                        "value": 7,
                        "ref": {"type": "int"},
                    },
                }
            ]
        )
        store = MetastoreArtifactStore(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            repository=repository,
        )
        key = step_output_binding_key("count_orders", "row_count")

        first = store.get(key)
        second = store.get(key)

        self.assertIs(first, second)
        self.assertEqual(first.kind, "scalar")
        self.assertEqual(first.value, 7)
        self.assertEqual(len(repository.calls), 1)

    def test_local_put_takes_precedence_over_repository_lookup(self) -> None:
        repository = _FakeStepOutputBindingRepository()
        store = MetastoreArtifactStore(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            repository=repository,
        )
        binding = StepOutputBinding(
            step_id="extract_orders",
            output_name="orders_rows",
            kind="rowset",
            value={"kind": "rowset", "path": "/tmp/orders.parquet"},
            ref={"format": "parquet"},
        )

        store.put(binding.key, binding)

        self.assertIs(store.get(binding.key), binding)
        self.assertEqual(repository.calls, [])

    def test_missing_or_invalid_binding_raises_key_error(self) -> None:
        missing_store = MetastoreArtifactStore(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            repository=_FakeStepOutputBindingRepository(),
        )
        invalid_store = MetastoreArtifactStore(
            project_id="retail",
            job_id="daily",
            run_id="run_1",
            repository=_InvalidStepOutputBindingRepository(),
        )

        with self.assertRaises(KeyError):
            missing_store.get(step_output_binding_key("unknown", "output"))
        with self.assertRaises(KeyError):
            missing_store.get("other-artifact/extract_orders/orders_rows")
        with self.assertRaises(KeyError):
            invalid_store.get(step_output_binding_key("unknown", "output"))


if __name__ == "__main__":
    unittest.main()
