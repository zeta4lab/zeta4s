from __future__ import annotations

from pathlib import Path
import hashlib
import sys
from tempfile import TemporaryDirectory
import types
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

sys.modules.setdefault("oracledb", types.SimpleNamespace(makedsn=lambda *args, **kwargs: "dsn"))

from zeta4s.runtime.backends.oracle.write import write_oracle_rowset
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


class FakeOracleCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, object]] = []
        self._fetchone = (0,)
        self._fetchall = []

    def execute(self, sql: str, params=None) -> None:
        self.statements.append((sql, params))

    def executemany(self, sql: str, rows) -> None:
        self.executemany_calls.append((sql, rows))

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall

    def setinputsizes(self, *args) -> None:
        return None

    def close(self) -> None:
        return None


class FakeOracleConn:
    def __init__(self, cursor: FakeOracleCursor) -> None:
        self.cursor_obj = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class OracleWriteBackendTest(unittest.TestCase):
    def test_replace_creates_key_column_not_null(self) -> None:
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
            cursor = FakeOracleCursor()
            conn = FakeOracleConn(cursor)

            with patch("zeta4s.runtime.backends.oracle.client.get_oracle_conn", return_value=conn):
                result = write_oracle_rowset(
                    rowset=rowset,
                    target_conn="oracle_target",
                    target_table="sales",
                    target_namespace=None,
                    mode="replace",
                    columns=["sale_id", "amount"],
                    key=["sale_id"],
                )

            self.assertEqual(result["input_rows"], 2)
            self.assertTrue(conn.committed)
            create_sql = next(sql for sql, _ in cursor.statements if sql.startswith("CREATE TABLE"))
            self.assertIn("SALE_ID NUMBER(19,0) NOT NULL", create_sql)
            self.assertIn("AMOUNT NUMBER(19,0)", create_sql)
            self.assertTrue(cursor.executemany_calls)

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

            with patch("zeta4s.runtime.backends.oracle.client.get_oracle_conn"):
                with self.assertRaisesRegex(ValueError, "key column contains null values: sale_id"):
                    write_oracle_rowset(
                        rowset=rowset,
                        target_conn="oracle_target",
                        target_table="sales",
                        target_namespace=None,
                        mode="replace",
                        columns=["sale_id", "amount"],
                        key=["sale_id"],
                    )


if __name__ == "__main__":
    unittest.main()
