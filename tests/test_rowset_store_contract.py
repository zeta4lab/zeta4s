from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa

from zeta4s.runtime.rowset_models import RowsetIdentity, RowsetStorage
from zeta4s.runtime.rowset_store import (
    CheckpointNotSupported,
    rowset_descriptor_from_payload,
    rowset_descriptor_payload,
)
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore


class RowsetStoreContractTest(unittest.TestCase):
    def test_parquet_store_finishes_and_reopens_storage_neutral_descriptor(self) -> None:
        with TemporaryDirectory() as tmp:
            store = ParquetRowsetStore(Path(tmp))
            identity = RowsetIdentity(
                project_id="retail",
                job_id="orders",
                run_id="run-1",
                step_id="extract_orders",
                attempt=1,
                output_name="orders_rows",
            )
            schema = pa.schema(
                [
                    pa.field("order_id", pa.int64(), nullable=False),
                    pa.field("amount", pa.decimal128(18, 2)),
                ]
            )
            session = store.begin(identity, schema=schema)
            session.append(
                pa.record_batch(
                    [
                        pa.array([1, 2]),
                        pa.array([Decimal("10.50"), Decimal("20.00")], type=pa.decimal128(18, 2)),
                    ],
                    schema=schema,
                )
            )
            session.append(
                pa.record_batch(
                    [pa.array([3]), pa.array([Decimal("30.25")], type=pa.decimal128(18, 2))],
                    schema=schema,
                )
            )

            with self.assertRaises(CheckpointNotSupported):
                session.checkpoint({"batch": 2})

            descriptor = session.finish()
            payload = rowset_descriptor_payload(descriptor)
            restored = rowset_descriptor_from_payload(payload)
            batches = list(store.open_reader(restored).iter_batches(batch_size=2))
            table = pa.Table.from_batches(batches)

            self.assertEqual(restored.storage, RowsetStorage.PARQUET)
            self.assertEqual(restored.rows, 3)
            self.assertGreater(restored.bytes, 0)
            self.assertEqual(restored.columns, ("order_id", "amount"))
            self.assertEqual(table.column("order_id").to_pylist(), [1, 2, 3])
            self.assertEqual([str(value) for value in table.column("amount").to_pylist()], ["10.50", "20.00", "30.25"])

    def test_parquet_store_rejects_resume(self) -> None:
        with TemporaryDirectory() as tmp:
            store = ParquetRowsetStore(Path(tmp))
            identity = RowsetIdentity("retail", "orders", "run-1", "extract_orders", 1, "orders_rows")
            session = store.begin(identity, schema=pa.schema([pa.field("order_id", pa.int64())]))
            session.append(pa.record_batch([pa.array([1])], names=["order_id"]))
            descriptor = session.finish()

            with self.assertRaises(CheckpointNotSupported):
                store.resume(descriptor)

    def test_descriptor_parser_rejects_missing_storage(self) -> None:
        with self.assertRaisesRegex(ValueError, "storage"):
            rowset_descriptor_from_payload({"uri": "file:///tmp/orders.parquet"})


if __name__ == "__main__":
    unittest.main()
