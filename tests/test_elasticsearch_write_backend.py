from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import types
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from zeta4s.runtime.backends.elasticsearch.write import bulk_payload, document_id_value, write_elasticsearch_rowset
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetStorage
from zeta4s.runtime.rowsets import ResolvedRowsetRef as RuntimeResolvedRowsetRef
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.source_reader import ColumnSpec


def ResolvedRowsetRef(
    *, path: Path, source_ref: str, step_id: str, output_name: str, rows: int, bytes: int, columns, column_specs, **_
):
    schema = pq.ParquetFile(path).schema_arrow
    descriptor = RowsetDescriptor(
        RowsetStorage.PARQUET,
        path.as_uri(),
        rows,
        bytes,
        tuple(columns),
        tuple(column_specs),
        hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
    )
    return RuntimeResolvedRowsetRef(
        source_ref, step_id, output_name, descriptor, ParquetRowsetStore(path.parent).open_reader(descriptor)
    )


class ElasticsearchWriteBackendTest(unittest.TestCase):
    def test_composite_document_id_uses_unambiguous_json_array_encoding(self) -> None:
        left = document_id_value(
            {"first": "a|b", "second": "c"},
            {"mode": "columns", "columns": ["first", "second"]},
        )
        right = document_id_value(
            {"first": "a", "second": "b|c"},
            {"mode": "columns", "columns": ["first", "second"]},
        )

        self.assertEqual(left, '["a|b","c"]')
        self.assertEqual(right, '["a","b|c"]')
        self.assertNotEqual(left, right)

    def test_append_uses_bulk_create_action(self) -> None:
        batch = pa.record_batch([pa.array([1], type=pa.int64())], names=["sale_id"])

        payload = bulk_payload(
            batch,
            ["sale_id"],
            mode="append",
            document_id={"mode": "columns", "columns": ["sale_id"]},
        )

        self.assertEqual(json.loads(payload.decode("utf-8").splitlines()[0]), {"create": {"_id": "1"}})

    def test_upsert_writes_rowset_as_bulk_update_payload(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, 2], type=pa.int64()),
                        "amount": pa.array([Decimal("10.50"), Decimal("20.75")], type=pa.decimal128(18, 2)),
                        "sold_at": pa.array(
                            [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 2, 0, 0)], type=pa.timestamp("us")
                        ),
                    }
                ),
                path,
            )
            rowset = ResolvedRowsetRef(
                source_ref="fetch_sales.sales_rows",
                step_id="fetch_sales",
                output_name="sales_rows",
                path=path,
                uri=path.as_uri(),
                rows=2,
                bytes=path.stat().st_size,
                columns=("sale_id", "amount", "sold_at"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", False),
                    ColumnSpec.from_type("amount", "Decimal(18, 2)", True),
                    ColumnSpec.from_type("sold_at", "DateTime64(6)", True),
                ),
            )
            fake_conn = types.SimpleNamespace(
                base_url="http://elasticsearch:9200", headers={"Authorization": "Basic x"}
            )
            calls = []

            def fake_request(url, body=None, method="POST", content_type="application/json", headers=None, timeout=60):
                calls.append(
                    {"url": url, "body": body, "method": method, "content_type": content_type, "headers": headers}
                )
                return json.dumps(
                    {"errors": False, "took": 3, "items": [{"update": {"status": 200}}, {"update": {"status": 200}}]}
                ).encode("utf-8")

            with (
                patch("zeta4s.runtime.backends.elasticsearch.write.elasticsearch_connection", return_value=fake_conn),
                patch("zeta4s.runtime.backends.elasticsearch.write.index_exists", return_value=True),
                patch("zeta4s.runtime.backends.elasticsearch.write.elasticsearch_request", side_effect=fake_request),
            ):
                result = write_elasticsearch_rowset(
                    rowset=rowset,
                    target_conn="elasticsearch_target",
                    mode="upsert",
                    columns=["sale_id", "amount", "sold_at"],
                    key=["sale_id"],
                    options={"index": "sales-target", "bulk_batch_size": 100, "refresh": True},
                    context={},
                )

            self.assertEqual(result["input_rows"], 2)
            self.assertEqual(result["batches"], 1)
            self.assertEqual(calls[0]["url"], "http://elasticsearch:9200/sales-target/_bulk?refresh=true")
            self.assertEqual(calls[0]["content_type"], "application/x-ndjson")
            lines = calls[0]["body"].decode("utf-8").strip().splitlines()
            self.assertEqual(json.loads(lines[0]), {"update": {"_id": "1"}})
            self.assertEqual(json.loads(lines[1])["doc"]["amount"], 10.5)
            self.assertTrue(json.loads(lines[1])["doc_as_upsert"])

    def test_upsert_requires_document_id_columns(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(pa.table({"sale_id": pa.array([1], type=pa.int64())}), path)
            rowset = ResolvedRowsetRef(
                source_ref="fetch_sales.sales_rows",
                step_id="fetch_sales",
                output_name="sales_rows",
                path=path,
                uri=path.as_uri(),
                rows=1,
                bytes=path.stat().st_size,
                columns=("sale_id",),
                column_specs=(ColumnSpec.from_type("sale_id", "Int64", False),),
            )

            with self.assertRaisesRegex(ValueError, "mode=upsert requires document_id columns"):
                write_elasticsearch_rowset(
                    rowset=rowset,
                    target_conn="elasticsearch_target",
                    mode="upsert",
                    columns=["sale_id"],
                    key=[],
                    options={"index": "sales-target", "document_id": {"mode": "auto"}},
                    context={},
                )


if __name__ == "__main__":
    unittest.main()
