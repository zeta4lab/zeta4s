from __future__ import annotations

from decimal import Decimal
import hashlib
from pathlib import Path
import sys
import types
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

sys.modules.setdefault("airflow", types.SimpleNamespace())
sys.modules.setdefault("airflow.sdk", types.SimpleNamespace(get_current_context=lambda: {}))

from zeta4s.runtime.backends.clickhouse.write import _key_delete_predicate, write_clickhouse_rowset
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


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.raw_inserts: list[dict] = []
        self.table_exists = False
        self.describe_rows = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str):
        self.commands.append(sql)
        if sql.startswith("EXISTS TABLE"):
            return types.SimpleNamespace(first_row=(1 if self.table_exists else 0,))
        if sql.startswith("DESCRIBE TABLE"):
            return types.SimpleNamespace(result_rows=self.describe_rows)
        raise AssertionError(f"unexpected query: {sql}")

    def raw_insert(self, table, *, column_names, insert_block, fmt):
        self.raw_inserts.append(
            {
                "table": table,
                "column_names": list(column_names),
                "insert_block": insert_block,
                "fmt": fmt,
            }
        )


class ClickHouseWriteBackendTest(unittest.TestCase):
    def test_replace_writes_rowset_to_clickhouse_table(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, 2], type=pa.int64()),
                        "amount": pa.array([100, 250], type=pa.int64()),
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
                columns=("sale_id", "amount"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", False),
                    ColumnSpec.from_type("amount", "Int64", False),
                ),
            )
            client = FakeClickHouseClient()

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client", return_value=client):
                result = write_clickhouse_rowset(
                    rowset=rowset,
                    target_conn="clickhouse_target",
                    target_table="sales",
                    target_namespace="mart",
                    mode="replace",
                    columns=["sale_id", "amount"],
                    key=[],
                    options={"settings": {"index_granularity": 8192}},
                )

            self.assertEqual(result["input_rows"], 2)
            self.assertEqual(result["output_rows"], 2)
            self.assertIn("DROP TABLE IF EXISTS `mart`.`sales`", client.commands)
            self.assertTrue(any(command.startswith("CREATE TABLE `mart`.`sales`") for command in client.commands))
            self.assertTrue(any("SETTINGS index_granularity = 8192" in command for command in client.commands))
            self.assertEqual(client.raw_inserts[0]["table"], "`mart`.`sales`")
            self.assertEqual(client.raw_inserts[0]["column_names"], ["sale_id", "amount"])
            self.assertEqual(client.raw_inserts[0]["fmt"], "Parquet")

    def test_upsert_delete_predicate_keeps_decimal_key_numeric(self) -> None:
        batch = pa.record_batch({"sale_id": pa.array([Decimal("100001")], type=pa.decimal128(19, 0))})

        predicate = _key_delete_predicate(batch, ["sale_id"])

        self.assertEqual(predicate, "`sale_id` IN (100001)")

    def test_key_and_order_by_columns_are_created_not_null(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, 2], type=pa.int64()),
                        "amount": pa.array([100, 250], type=pa.int64()),
                    },
                    schema=pa.schema(
                        [
                            pa.field("sale_id", pa.int64(), nullable=True),
                            pa.field("amount", pa.int64(), nullable=True),
                        ]
                    ),
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
                columns=("sale_id", "amount"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", True),
                    ColumnSpec.from_type("amount", "Int64", True),
                ),
            )
            client = FakeClickHouseClient()

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client", return_value=client):
                write_clickhouse_rowset(
                    rowset=rowset,
                    target_conn="clickhouse_target",
                    target_table="sales",
                    target_namespace="mart",
                    mode="replace",
                    columns=["sale_id", "amount"],
                    key=["sale_id"],
                    options={"order_by": ["sale_id"]},
                )

            create_table = next(command for command in client.commands if command.startswith("CREATE TABLE"))
            self.assertIn("`sale_id` Int64", create_table)
            self.assertIn("`amount` Nullable(Int64)", create_table)
            self.assertNotIn("allow_nullable_key", create_table)

    def test_key_columns_reject_actual_null_values(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, None], type=pa.int64()),
                        "amount": pa.array([100, 250], type=pa.int64()),
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
                columns=("sale_id", "amount"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", True),
                    ColumnSpec.from_type("amount", "Int64", True),
                ),
            )

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client"):
                with self.assertRaisesRegex(ValueError, "key/order_by column contains null values: sale_id"):
                    write_clickhouse_rowset(
                        rowset=rowset,
                        target_conn="clickhouse_target",
                        target_table="sales",
                        target_namespace="mart",
                        mode="replace",
                        columns=["sale_id", "amount"],
                        key=["sale_id"],
                        options={"order_by": ["sale_id"]},
                    )

    def test_existing_table_schema_is_validated_for_write_columns(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, 2], type=pa.int64()),
                        "amount": pa.array([100, 250], type=pa.int64()),
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
                columns=("sale_id", "amount"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", True),
                    ColumnSpec.from_type("amount", "Int64", True),
                ),
            )
            client = FakeClickHouseClient()
            client.table_exists = True
            client.describe_rows = [("sale_id", "Int64"), ("amount", "Nullable(Int64)")]

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client", return_value=client):
                write_clickhouse_rowset(
                    rowset=rowset,
                    target_conn="clickhouse_target",
                    target_table="sales",
                    target_namespace="mart",
                    mode="append",
                    columns=["sale_id", "amount"],
                    key=["sale_id"],
                    options={"order_by": ["sale_id"]},
                )

            self.assertFalse(any(command.startswith("CREATE TABLE") for command in client.commands))
            self.assertTrue(any(command.startswith("DESCRIBE TABLE") for command in client.commands))

    def test_existing_table_rejects_nullable_key_column(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            pq.write_table(
                pa.table(
                    {
                        "sale_id": pa.array([1, 2], type=pa.int64()),
                        "amount": pa.array([100, 250], type=pa.int64()),
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
                columns=("sale_id", "amount"),
                column_specs=(
                    ColumnSpec.from_type("sale_id", "Int64", True),
                    ColumnSpec.from_type("amount", "Int64", True),
                ),
            )
            client = FakeClickHouseClient()
            client.table_exists = True
            client.describe_rows = [("sale_id", "Nullable(Int64)"), ("amount", "Nullable(Int64)")]

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client", return_value=client):
                with self.assertRaisesRegex(ValueError, "target key/order_by columns must be non-null"):
                    write_clickhouse_rowset(
                        rowset=rowset,
                        target_conn="clickhouse_target",
                        target_table="sales",
                        target_namespace="mart",
                        mode="append",
                        columns=["sale_id", "amount"],
                        key=["sale_id"],
                        options={"order_by": ["sale_id"]},
                    )


if __name__ == "__main__":
    unittest.main()
