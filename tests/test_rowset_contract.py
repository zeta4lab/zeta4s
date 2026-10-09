from __future__ import annotations

from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import types
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

sys.modules.setdefault("airflow", types.SimpleNamespace())
sys.modules.setdefault("airflow.sdk", types.SimpleNamespace(get_current_context=lambda: {}))

from zeta4s.runtime.backends.clickhouse.stage import stage_clickhouse_rowset
from zeta4s.runtime.backends.clickhouse.write import write_clickhouse_rowset
from zeta4s.runtime.backends.oracle.write import write_oracle_rowset
from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetStorage
from zeta4s.runtime.rowset_store import rowset_descriptor_payload
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.rowsets import ResolvedRowsetRef, resolve_rowset_ref
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.types import (
    arrow_type_from_column_spec,
    clickhouse_type_from_column_spec,
    oracle_type_from_column_spec,
)


def _es_spec(
    name: str,
    type_name: str,
    logical_type: str,
    *,
    nullable: bool = True,
    precision: int | None = None,
    scale: int | None = None,
    datetime_precision: int | None = None,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        type=type_name,
        nullable=nullable,
        logical_type=logical_type,
        precision=precision,
        scale=scale,
        datetime_precision=datetime_precision,
        source_backend="elasticsearch",
        source_type=type_name.split("(", 1)[0],
    )


def _sales_specs() -> tuple[ColumnSpec, ...]:
    return (
        _es_spec("sale_id", "int", "integer", nullable=False, precision=10, scale=0),
        _es_spec("amount", "decimal(18,2)", "decimal", nullable=True, precision=18, scale=2),
        _es_spec("sold_at", "timestamp(6)", "timestamp", nullable=True, datetime_precision=6),
    )


def _write_sales_parquet(path: Path, specs: tuple[ColumnSpec, ...]) -> None:
    schema = pa.schema(
        [
            pa.field("sale_id", pa.int32(), nullable=False),
            pa.field("amount", pa.decimal128(18, 2), nullable=True),
            pa.field("sold_at", pa.timestamp("us"), nullable=True),
        ],
        metadata={ROWSET_COLUMN_SPECS_METADATA_KEY: json.dumps([spec.to_json() for spec in specs]).encode("utf-8")},
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int32()),
            pa.array([Decimal("10.50"), Decimal("25.00")], type=pa.decimal128(18, 2)),
            pa.array([None, None], type=pa.timestamp("us")),
        ],
        schema=schema,
    )
    pq.write_table(table, path)


def _resolved_rowset(path: Path, specs: tuple[ColumnSpec, ...]) -> ResolvedRowsetRef:
    schema = pq.ParquetFile(path).schema_arrow
    descriptor = RowsetDescriptor(
        storage=RowsetStorage.PARQUET,
        uri=path.as_uri(),
        rows=2,
        bytes=path.stat().st_size,
        columns=tuple(schema.names),
        column_specs=specs,
        schema_fingerprint=hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
    )
    return ResolvedRowsetRef(
        source_ref="extract_sales.sales_rows",
        step_id="extract_sales",
        output_name="sales_rows",
        descriptor=descriptor,
        reader=ParquetRowsetStore(path.parent).open_reader(descriptor),
    )


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.raw_inserts: list[dict] = []
        self.table_exists = False

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str):
        self.commands.append(sql)
        if sql.startswith("EXISTS TABLE"):
            return types.SimpleNamespace(first_row=(1 if self.table_exists else 0,))
        if sql.startswith("DESCRIBE TABLE"):
            return types.SimpleNamespace(result_rows=[])
        raise AssertionError(f"unexpected query: {sql}")

    def raw_insert(self, table, *, column_names, insert_block, fmt):
        self.raw_inserts.append({"table": table, "column_names": list(column_names), "fmt": fmt})


class FakeOracleCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, object]] = []

    def execute(self, sql: str, params=None) -> None:
        self.statements.append((sql, params))

    def executemany(self, sql: str, rows) -> None:
        self.executemany_calls.append((sql, rows))

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []

    def setinputsizes(self, *args) -> None:
        return None

    def close(self) -> None:
        return None


class FakeOracleConn:
    def __init__(self, cursor: FakeOracleCursor) -> None:
        self.cursor_obj = cursor
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class RowsetContractTest(unittest.TestCase):
    def test_resolve_rowset_ref_prefers_core_output_binding_context(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sales.parquet"
            specs = _sales_specs()
            _write_sales_parquet(path, specs)
            descriptor = _resolved_rowset(path, specs).descriptor

            rowset = resolve_rowset_ref(
                source_ref="extract_sales.sales_rows",
                context={
                    "step_output_bindings": {
                        "extract_sales.sales_rows": {
                            "kind": "rowset",
                            "value": rowset_descriptor_payload(descriptor),
                        }
                    }
                },
            )

        self.assertEqual(rowset.source_ref, "extract_sales.sales_rows")
        self.assertEqual(rowset.uri, path.as_uri())
        self.assertEqual(rowset.rows, 2)
        self.assertEqual([spec.name for spec in rowset.column_specs], ["sale_id", "amount", "sold_at"])

    def test_column_spec_requires_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires type"):
            ColumnSpec.from_value({"name": "sale_id", "nullable": False})

    def test_elasticsearch_rowset_specs_map_to_canonical_arrow_and_targets(self) -> None:
        sale_id, amount, sold_at = _sales_specs()

        self.assertEqual(str(arrow_type_from_column_spec(sale_id)), "int32")
        self.assertEqual(str(arrow_type_from_column_spec(amount)), "decimal128(18, 2)")
        self.assertEqual(str(arrow_type_from_column_spec(sold_at)), "timestamp[us]")

        self.assertEqual(clickhouse_type_from_column_spec(sale_id), "Int32")
        self.assertEqual(clickhouse_type_from_column_spec(amount), "Nullable(Decimal(18, 2))")
        self.assertEqual(clickhouse_type_from_column_spec(sold_at), "Nullable(DateTime64(6))")

        self.assertEqual(oracle_type_from_column_spec(sale_id), "NUMBER(10,0)")
        self.assertEqual(oracle_type_from_column_spec(amount), "NUMBER(18,2)")
        self.assertEqual(oracle_type_from_column_spec(sold_at), "TIMESTAMP(6)")

    def test_source_native_type_hint_is_preserved_only_for_matching_target_backend(self) -> None:
        clickhouse_spec = ColumnSpec.from_type(
            "id",
            "UInt64",
            False,
            source_backend="clickhouse",
            source_type="UInt64",
        )
        elasticsearch_spec = _es_spec("id", "int", "integer", nullable=False, precision=19, scale=0)
        oracle_spec = ColumnSpec(
            name="created_at",
            type="timestamp(3)",
            nullable=True,
            logical_type="timestamp",
            datetime_precision=3,
            source_backend="oracle",
            source_type="TIMESTAMP(3)",
        )

        self.assertEqual(clickhouse_type_from_column_spec(clickhouse_spec), "UInt64")
        self.assertEqual(clickhouse_type_from_column_spec(elasticsearch_spec), "Int64")
        self.assertEqual(oracle_type_from_column_spec(oracle_spec), "TIMESTAMP(3)")

    def test_clickhouse_stage_uses_rowset_metadata_for_elasticsearch_rowset(self) -> None:
        specs = _sales_specs()
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            _write_sales_parquet(path, specs)
            rowset = _resolved_rowset(path, specs)
            client = FakeClickHouseClient()
            with (
                patch(
                    "zeta4s.runtime.backends.clickhouse.stage.get_clickhouse_runtime_client",
                    return_value=client,
                ),
                patch("zeta4s.runtime.backends.clickhouse.stage.insert_rowset_batches", return_value=(2, 1)),
            ):
                loaded, target_ref = stage_clickhouse_rowset(
                    rowset=rowset,
                    stage_conn="analytics_clickhouse",
                    target_table="sales_stage",
                    target_namespace="mart",
                    kwargs={},
                )

        self.assertEqual(loaded, 2)
        self.assertEqual(target_ref, "`mart`.`sales_stage`")
        create_sql = next(command for command in client.commands if command.startswith("CREATE TABLE"))
        self.assertIn("sale_id Int32", create_sql)
        self.assertIn("amount Nullable(Decimal(18, 2))", create_sql)
        self.assertIn("sold_at Nullable(DateTime64(6))", create_sql)

    def test_clickhouse_write_uses_elasticsearch_rowset_logical_types(self) -> None:
        specs = _sales_specs()
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            _write_sales_parquet(path, specs)
            rowset = _resolved_rowset(path, specs)
            client = FakeClickHouseClient()

            with patch("zeta4s.runtime.backends.clickhouse.write.get_clickhouse_runtime_client", return_value=client):
                write_clickhouse_rowset(
                    rowset=rowset,
                    target_conn="clickhouse_target",
                    target_table="sales",
                    target_namespace="mart",
                    mode="replace",
                    columns=["sale_id", "amount", "sold_at"],
                    key=["sale_id"],
                    options={"order_by": ["sale_id"]},
                )

        create_sql = next(command for command in client.commands if command.startswith("CREATE TABLE"))
        self.assertIn("`sale_id` Int32", create_sql)
        self.assertIn("`amount` Nullable(Decimal(18, 2))", create_sql)
        self.assertIn("`sold_at` Nullable(DateTime64(6))", create_sql)

    def test_oracle_write_uses_elasticsearch_rowset_logical_types(self) -> None:
        specs = _sales_specs()
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_rows.parquet"
            _write_sales_parquet(path, specs)
            rowset = _resolved_rowset(path, specs)
            cursor = FakeOracleCursor()
            conn = FakeOracleConn(cursor)

            with patch("zeta4s.runtime.backends.oracle.client.get_oracle_conn", return_value=conn):
                write_oracle_rowset(
                    rowset=rowset,
                    target_conn="oracle_target",
                    target_table="sales",
                    target_namespace=None,
                    mode="replace",
                    columns=["sale_id", "amount", "sold_at"],
                    key=["sale_id"],
                )

        self.assertTrue(conn.committed)
        create_sql = next(sql for sql, _ in cursor.statements if sql.startswith("CREATE TABLE"))
        self.assertIn("SALE_ID NUMBER(10,0) NOT NULL", create_sql)
        self.assertIn("AMOUNT NUMBER(18,2)", create_sql)
        self.assertIn("SOLD_AT TIMESTAMP(6)", create_sql)


if __name__ == "__main__":
    unittest.main()
