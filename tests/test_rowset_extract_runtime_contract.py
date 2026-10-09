from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch
from zeta4s.runtime.rowset_extract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_extract import ClickHouseSelectReader
from zeta4s.runtime.rowset_extract import _run_extract_rowset_impl
from zeta4s.runtime.rowset_extract import _select_query
from zeta4s.runtime.rowset_extract import write_reader_to_rowset
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetIdentity, RowsetStorage
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.step_state import parse_watermark_value
from zeta4s.runtime.rowset_stage import _column_specs
from zeta4s.runtime.rowset_stage import _clickhouse_type_from_column_spec
from zeta4s.runtime.rowsets import ResolvedRowsetRef, resolve_rowset_ref
from zeta4s.runtime.rowsets import runtime_home


def _column_spec_tuples(specs):
    return [(spec.name, spec.type, spec.nullable) for spec in specs]


def write_reader_to_parquet_rowset(*, reader, path, watermark_column=None, context=None):
    """Exercise the storage-neutral writer with a local Parquet store."""
    import shutil
    from urllib.parse import unquote, urlparse

    context = dict(context or {})
    result = write_reader_to_rowset(
        reader=reader,
        store=ParquetRowsetStore(path.parent),
        identity=RowsetIdentity(
            project_id="test",
            job_id="test",
            run_id=str(context.get("run_id") or "run"),
            step_id=str(context.get("task_id") or "extract"),
            attempt=1,
            output_name=path.stem,
        ),
        watermark_column=watermark_column,
        context=context,
    )
    generated = Path(unquote(urlparse(result.uri).path))
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(generated, path)
    return result


def _resolved_test_parquet(path: Path, specs, source_ref: str) -> ResolvedRowsetRef:
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema_arrow
    step_id, output_name = source_ref.split(".", 1)
    descriptor = RowsetDescriptor(
        RowsetStorage.PARQUET,
        path.as_uri(),
        pq.read_metadata(path).num_rows,
        path.stat().st_size,
        tuple(schema.names),
        tuple(specs),
        hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
    )
    return ResolvedRowsetRef(
        source_ref, step_id, output_name, descriptor, ParquetRowsetStore(path.parent).open_reader(descriptor)
    )


class FakeReader:
    source_kind = "fake"
    source_object = "fake_source"
    column_specs = [("id", "Int64", False), ("updated_at", "DateTime", False)]
    columns = ["id", "updated_at"]

    def read_batches(self):
        from datetime import datetime

        yield SourceBatch(
            column_values={
                "id": [1, 2],
                "updated_at": [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 1, 0, 5)],
            }
        )


class _ContextReader:
    def __init__(self, reader):
        self.reader = reader

    def __enter__(self):
        return self.reader

    def __exit__(self, exc_type, exc, tb):
        return None


class _FakeStepStateAdapter:
    def __init__(self, states):
        self.step_state_repository = _FakeStepStateRepository(states)


class _FakeStepStateRepository:
    def __init__(self, states):
        self.states = states

    def get_state(self, *, project_id, job_id, step_id, state_key):
        return self.states.get((project_id, job_id, step_id, state_key))


class EmptySchemaReader:
    source_kind = "fake"
    source_object = "empty_source"
    column_specs = []
    columns = []

    def read_batches(self):
        return
        yield


class EmptyRowsReader:
    source_kind = "fake"
    source_object = "empty_rows_source"
    column_specs = [("id", "Int64", False), ("updated_at", "DateTime64(3)", True)]
    columns = ["id", "updated_at"]

    def read_batches(self):
        return
        yield


class DecimalDriftReader:
    source_kind = "fake"
    source_object = "decimal_drift_source"
    column_specs = [("id", "Int64", False), ("amount", "Decimal(18, 2)", False)]
    columns = ["id", "amount"]

    def read_batches(self):
        from decimal import Decimal

        yield SourceBatch(column_values={"id": [1, 2], "amount": [Decimal("1.23"), Decimal("12.34")]})
        yield SourceBatch(column_values={"id": [3, 4], "amount": [Decimal("12345.67"), Decimal("987654.32")]})


class NullFirstBatchReader:
    source_kind = "fake"
    source_object = "null_first_batch_source"
    column_specs = [
        ("id", "Int64", True),
        ("name", "String", True),
        ("updated_at", "DateTime64(6)", True),
    ]
    columns = ["id", "name", "updated_at"]

    def read_batches(self):
        from datetime import datetime

        yield SourceBatch(column_values={"id": [None], "name": [None], "updated_at": [None]})
        yield SourceBatch(
            column_values={
                "id": [1],
                "name": ["alpha"],
                "updated_at": [datetime(2026, 1, 1, 0, 0, 1, 123456)],
            }
        )


class PrimitiveSchemaReader:
    source_kind = "fake"
    source_object = "primitive_schema_source"
    column_specs = [
        ("int_col", "Int32", False),
        ("float_col", "Float32", False),
        ("bool_col", "Bool", False),
        ("date_col", "Date", False),
        ("timestamp_col", "DateTime64(3)", False),
        ("text_col", "LowCardinality(String)", True),
    ]
    columns = ["int_col", "float_col", "bool_col", "date_col", "timestamp_col", "text_col"]

    def read_batches(self):
        from datetime import date, datetime

        yield SourceBatch(
            column_values={
                "int_col": [1],
                "float_col": [1.5],
                "bool_col": [True],
                "date_col": [date(2026, 1, 1)],
                "timestamp_col": [datetime(2026, 1, 1, 0, 0, 1, 123000)],
                "text_col": ["x"],
            }
        )


class StringTemporalSchemaReader:
    source_kind = "fake"
    source_object = "string_temporal_source"
    column_specs = [
        ("sold_date", "Date", False),
        ("sold_at", "DateTime64(6)", False),
    ]
    columns = ["sold_date", "sold_at"]

    def read_batches(self):
        yield SourceBatch(
            column_values={
                "sold_date": ["2026-01-01"],
                "sold_at": ["2026-01-01T00:00:01.123456Z"],
            }
        )


class ArrowBatchReader:
    source_kind = "fake"
    source_object = "arrow_batch_source"
    column_specs = [("id", "Int64", False), ("name", "String", True)]
    columns = ["id", "name"]

    def read_batches(self):
        import pyarrow as pa

        yield SourceBatch(arrow_table=pa.table({"id": [1, 2], "name": ["alpha", None]}))


class RowsetExtractRuntimeContractTest(unittest.TestCase):
    def test_clickhouse_reader_prefers_describe_query_schema(self) -> None:
        class FakeClickHouseClient:
            def query(self, sql, parameters=None):
                self.sql = sql
                self.parameters = parameters
                return types.SimpleNamespace(
                    result_rows=[
                        ("is_priority", "Bool"),
                        ("discount_rate", "Float32"),
                        ("sold_date", "Date"),
                    ]
                )

        reader = ClickHouseSelectReader(
            source_conn="fake",
            source_object="fake_source",
            query="select is_priority, discount_rate, sold_date from source.sales where store_id = :store_id",
            params={"store_id": 1},
            batch_size=100,
        )
        client = FakeClickHouseClient()
        reader._client = client

        reader._set_schema_from_clickhouse_query()

        self.assertEqual(reader.columns, ["is_priority", "discount_rate", "sold_date"])
        self.assertEqual(
            _column_spec_tuples(reader.column_specs),
            [("is_priority", "Bool", False), ("discount_rate", "Float32", False), ("sold_date", "Date", False)],
        )
        self.assertEqual(reader.column_specs[0].logical_type, "boolean")
        self.assertEqual(reader.column_specs[1].precision, 32)
        self.assertEqual(client.parameters, {"store_id": 1})
        self.assertIn("DESCRIBE TABLE", client.sql)
        self.assertIn("where store_id = {store_id:Int64}", client.sql)

    def test_rowset_writer_creates_parquet_with_watermark(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "orders_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=FakeReader(),
                path=path,
                watermark_column="updated_at",
                context={"run_id": "run_1", "task_id": "fetch_orders"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(result.columns, ("id", "updated_at"))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(FakeReader.column_specs))
            metadata = table.schema.metadata or {}
            self.assertIn(ROWSET_COLUMN_SPECS_METADATA_KEY, metadata)
            self.assertEqual(result.new_watermark, datetime(2026, 1, 1, 0, 5))
            self.assertTrue(result.uri.startswith("file://"))

    def test_rowset_writer_creates_empty_parquet_with_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=EmptyRowsReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_empty"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 0)
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(result.columns, ("id", "updated_at"))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(EmptyRowsReader.column_specs))

    def test_rowset_writer_uses_column_specs_for_decimal_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "decimal_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=DecimalDriftReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_decimal"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(result.rows, 4)
            self.assertEqual(schema.field("amount").type, pa.decimal128(18, 2))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(DecimalDriftReader.column_specs))
            self.assertEqual(result.column_specs[1].logical_type, "decimal")
            self.assertEqual(result.column_specs[1].precision, 18)
            self.assertEqual(result.column_specs[1].scale, 2)

    def test_rowset_writer_accepts_float_values_for_decimal_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class FloatDecimalReader:
            source_kind = "oracle"
            source_object = "source.decimal_float"
            column_specs = [("amount", "Decimal(18, 2)", True)]
            columns = ["amount"]

            def read_batches(self):
                yield SourceBatch(column_values={"amount": [1.23, None]})

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "float_decimal_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=FloatDecimalReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_float_decimal"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("amount").type, pa.decimal128(18, 2))

    def test_rowset_writer_casts_null_first_batch_to_fixed_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "null_first_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=NullFirstBatchReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_null_first"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.schema.field("id").type, pa.int64())
            self.assertEqual(table.schema.field("updated_at").type, pa.timestamp("us"))
            self.assertEqual(table.column("id").to_pylist(), [None, 1])

    def test_rowset_writer_preserves_primitive_column_spec_types(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "primitive_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=PrimitiveSchemaReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_primitive"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("int_col").type, pa.int32())
            self.assertEqual(schema.field("float_col").type, pa.float32())
            self.assertEqual(schema.field("bool_col").type, pa.bool_())
            self.assertEqual(schema.field("date_col").type, pa.date32())
            self.assertEqual(schema.field("timestamp_col").type, pa.timestamp("ms"))
            self.assertEqual(schema.field("text_col").type, pa.string())

    def test_rowset_writer_coerces_string_temporal_values_to_schema(self) -> None:
        from datetime import date, datetime
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "string_temporal_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=StringTemporalSchemaReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_string_temporal"},
            )

            table = pq.read_table(path)
            self.assertEqual(table.schema.field("sold_date").type, pa.date32())
            self.assertEqual(table.schema.field("sold_at").type, pa.timestamp("us"))
            self.assertEqual(table.column("sold_date").to_pylist(), [date(2026, 1, 1)])
            self.assertEqual(table.column("sold_at").to_pylist(), [datetime(2026, 1, 1, 0, 0, 1, 123456)])

    def test_rowset_writer_accepts_arrow_source_batch(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "arrow_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=ArrowBatchReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_arrow"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.column("name").to_pylist(), ["alpha", None])

    def test_rowset_writer_preserves_unsigned_integer_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class UIntReader:
            source_kind = "clickhouse"
            source_object = "source.uints"
            column_specs = [("id", "UInt64", False)]
            columns = ["id"]

            def read_batches(self):
                yield SourceBatch(rows=[(9223372036854775808,)])

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "uint_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=UIntReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_uints"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("id").type, pa.uint64())

    def test_clickhouse_stage_preserves_nullable_column_specs(self) -> None:
        spec = ColumnSpec.from_type("optional_count", "Int64", True)

        self.assertEqual(_clickhouse_type_from_column_spec(spec), "Nullable(Int64)")

    def test_oracle_stage_uses_arrow_executemany_with_typed_binds(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class FakeCursor:
            def __init__(self):
                self.input_sizes = None
                self.executed = []
                self.executemany_calls = []

            def execute(self, sql, params=None):
                self.executed.append((sql, params))

            def fetchone(self):
                return (0,)

            def executemany(self, sql, payload):
                self.executemany_calls.append((sql, payload))

            def setinputsizes(self, *sizes):
                self.input_sizes = sizes

            def close(self):
                pass

        class FakeConn:
            def __init__(self):
                self.cursor_obj = FakeCursor()
                self.committed = False

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.committed = True

            def close(self):
                pass

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "oracle_stage.parquet"
            pq.write_table(
                pa.table(
                    {
                        "id": [1, 2],
                        "updated_at": [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 1, 0, 1)],
                    }
                ),
                path,
            )
            rowset = _resolved_test_parquet(
                path,
                (
                    ColumnSpec.from_type("id", "Int64", False, source_backend="oracle", source_type="NUMBER(19,0)"),
                    ColumnSpec.from_type(
                        "updated_at",
                        "DateTime64(6)",
                        True,
                        source_backend="oracle",
                        source_type="TIMESTAMP(6)",
                    ),
                ),
                "fetch.rows",
            )
            conn = FakeConn()
            fake_oracledb = types.ModuleType("oracledb")
            fake_oracledb.DB_TYPE_NUMBER = "NUMBER"
            fake_oracledb.DB_TYPE_TIMESTAMP = "TIMESTAMP"
            fake_oracledb.DB_TYPE_DATE = "DATE"
            fake_oracledb.DB_TYPE_BINARY_DOUBLE = "BINARY_DOUBLE"
            fake_oracledb.DB_TYPE_BINARY_FLOAT = "BINARY_FLOAT"
            fake_oracledb.DB_TYPE_CLOB = "CLOB"
            fake_oracledb.DB_TYPE_BLOB = "BLOB"
            fake_oracledb.DB_TYPE_VARCHAR = "VARCHAR"

            with (
                patch.dict(
                    sys.modules,
                    {"oracledb": fake_oracledb},
                ),
                patch("zeta4s.runtime.backends.oracle.stage.get_oracle_conn", return_value=conn),
            ):
                from zeta4s.runtime.backends.oracle.stage import stage_oracle_rowset

                loaded, target_ref = stage_oracle_rowset(
                    rowset=rowset,
                    stage_conn="oracle_default",
                    target_table="orders_stage",
                    target_namespace=None,
                )

            self.assertEqual(loaded, 2)
            self.assertEqual(target_ref, "ORDERS_STAGE")
            self.assertTrue(conn.committed)
            self.assertIsNotNone(conn.cursor_obj.input_sizes)
            self.assertEqual(len(conn.cursor_obj.input_sizes), 2)
            self.assertEqual(len(conn.cursor_obj.executemany_calls), 1)
            _, payload = conn.cursor_obj.executemany_calls[0]
            self.assertIsInstance(payload, pa.Table)

    def test_oracle_dataframe_fetch_keeps_clickhouse_integer_width(self) -> None:
        import pyarrow as pa

        fake_oracledb = types.ModuleType("oracledb")
        fake_oracledb.LOB = type("LOB", (), {})
        with patch.dict(sys.modules, {"oracledb": fake_oracledb}):
            from zeta4s.runtime.backends.oracle.extract import oracle_dataframe_arrow_type

        spec = ColumnSpec(
            name="amount",
            type="Nullable(Int64)",
            nullable=True,
            logical_type="integer",
            precision=5,
            scale=0,
            source_backend="oracle",
            source_type="NUMBER(5,0)",
        )

        self.assertEqual(oracle_dataframe_arrow_type(spec), pa.int64())

    def test_rowset_writer_rejects_empty_extract_without_schema(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"

            with self.assertRaisesRegex(ValueError, "no rows and no schema"):
                write_reader_to_parquet_rowset(
                    reader=EmptySchemaReader(),
                    path=path,
                    context={"run_id": "run_1", "task_id": "fetch_empty"},
                )

    def test_query_source_watermark_preserves_user_sql(self) -> None:
        sql = "select id, updated_at as watermark_ts from orders where updated_at > :select_from order by watermark_ts"

        adapter = _FakeStepStateAdapter(
            {
                ("retail", "orders", "extract_orders", "watermark:orders_rows:watermark_ts"): {
                    "state_value": {"value": "2026-01-01T00:00:00"}
                }
            }
        )

        with (
            patch("zeta4s.runtime.rowset_extract.load_extract_sql", return_value=sql),
            patch(
                "zeta4s.runtime.step_state.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            from datetime import datetime

            query, _, params = _select_query(
                source={"kind": "query", "query": "sql/orders.sql"},
                source_type="oracle",
                source_kind="query",
                project_root="/tmp/project",
                params={},
                watermark={"column": "watermark_ts"},
                time_window=None,
                loaded_at=datetime(2026, 1, 1, 1, 0),
                metadata_name="orders",
                job_id="orders",
                step_id="extract_orders",
                output_name="orders_rows",
                context={},
                kwargs={"project_id": "retail"},
            )

        self.assertEqual(query, sql)
        self.assertIn("select_from", params)
        self.assertIn("cur_wm", params)
        self.assertEqual(params["select_from"], datetime(2026, 1, 1, 0, 0))

    def test_query_source_watermark_rejects_missing_job_id(self) -> None:
        with patch("zeta4s.runtime.rowset_extract.load_extract_sql", return_value="select * from orders"):
            from datetime import datetime

            with self.assertRaisesRegex(ValueError, "project_id, job_id, and step_id"):
                _select_query(
                    source={"kind": "query", "query": "sql/orders.sql"},
                    source_type="oracle",
                    source_kind="query",
                    project_root="/tmp/project",
                    params={},
                    watermark={"column": "updated_at"},
                    time_window=None,
                    loaded_at=datetime(2026, 1, 1, 1, 0),
                    metadata_name="extract_orders",
                    job_id=None,
                    step_id="extract_orders",
                    output_name="orders_rows",
                    context={},
                    kwargs={"project_id": "retail"},
                )

    def test_extract_rowset_records_watermark_to_step_state(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        with (
            TemporaryDirectory() as tmp,
            patch.dict(os.environ, {"ZETA4S_API_HOME": tmp}),
            patch("zeta4s.runtime.rowset_extract.record_extract_history") as record_extract_history,
            patch("zeta4s.runtime.rowset_extract._open_reader", return_value=_ContextReader(FakeReader())),
            patch("zeta4s.runtime.rowset_extract.set_step_watermark") as set_step_watermark,
        ):
            result = _run_extract_rowset_impl(
                source_conn="orders_source",
                source={"kind": "table", "table": "orders"},
                output_name="orders_rows",
                source_type="oracle",
                project_root="/tmp/project",
                job_id="orders",
                step_id="extract_orders",
                params={},
                watermark={"column": "updated_at"},
                time_window=None,
                batch_size=None,
                context={"run_id": "run_1", "task_id": "extract_orders"},
                kwargs={"project_id": "retail"},
            )

        self.assertEqual(result.new_watermark, datetime(2026, 1, 1, 0, 5))
        set_step_watermark.assert_called_once_with(
            project_id="retail",
            job_id="orders",
            step_id="extract_orders",
            output_name="orders_rows",
            watermark_column="updated_at",
            watermark_value=datetime(2026, 1, 1, 0, 5),
            run_id="run_1",
        )
        self.assertEqual(record_extract_history.call_count, 2)
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].project_id, "retail")
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].job_id, "orders")
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].step_id, "extract_orders")

    def test_extract_rowset_rejects_watermark_state_without_job_id(self) -> None:
        from tempfile import TemporaryDirectory

        with (
            TemporaryDirectory() as tmp,
            patch.dict(os.environ, {"ZETA4S_API_HOME": tmp}),
            patch("zeta4s.runtime.rowset_extract.record_extract_history") as record_extract_history,
            patch("zeta4s.runtime.rowset_extract._open_reader", return_value=_ContextReader(FakeReader())),
            patch("zeta4s.runtime.rowset_extract.set_step_watermark") as set_step_watermark,
        ):
            with self.assertRaisesRegex(ValueError, "rowset extract requires runtime identity: job_id"):
                _run_extract_rowset_impl(
                    source_conn="orders_source",
                    source={"kind": "table", "table": "orders"},
                    output_name="orders_rows",
                    source_type="oracle",
                    project_root="/tmp/project",
                    job_id=None,
                    step_id="extract_orders",
                    params={},
                    watermark={"column": "updated_at"},
                    time_window=None,
                    batch_size=None,
                    context={"run_id": "run_1", "task_id": "extract_orders"},
                    kwargs={"project_id": "retail"},
                )

        record_extract_history.assert_not_called()
        set_step_watermark.assert_not_called()

    def test_watermark_state_parses_offset_as_utc_naive(self) -> None:
        from datetime import datetime

        self.assertEqual(
            parse_watermark_value("2026-01-01T09:00:00+09:00"),
            datetime(2026, 1, 1, 0, 0),
        )

    def test_resolve_rowset_ref_reads_task_result_artifact(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rowset_path = root / "runs" / "run_1" / "artifacts" / "rowsets" / "fetch_orders" / "orders_rows.parquet"
            rowset_path.parent.mkdir(parents=True)
            rowset_path.write_bytes(b"parquet")
            result_path = root / "runs" / "run_1" / "tasks" / "fetch_orders.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "task_id": "fetch_orders",
                        "details": {
                            "outputs": {
                                "orders_rows": {
                                    "kind": "rowset",
                                    "storage": "parquet",
                                    "uri": rowset_path.as_uri(),
                                    "rows": 3,
                                    "bytes": 7,
                                    "columns": ["id"],
                                    "column_specs": [{"name": "id", "type": "Int64", "nullable": False}],
                                    "schema_fingerprint": "test-fingerprint",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            resolved = resolve_rowset_ref(
                source_ref="fetch_orders.orders_rows",
                context={"run_id": "run_1"},
                home=root,
            )

        self.assertEqual(resolved.uri, rowset_path.as_uri())
        self.assertEqual(resolved.rows, 3)
        self.assertEqual(resolved.columns, ("id",))
        self.assertEqual(_column_spec_tuples(resolved.column_specs), [("id", "Int64", False)])

    def test_resolve_rowset_ref_uses_z4_run_id(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rowset_path = root / "runs" / "z4_run_1" / "artifacts" / "rowsets" / "fetch_orders" / "orders_rows.parquet"
            rowset_path.parent.mkdir(parents=True)
            rowset_path.write_bytes(b"parquet")
            result_path = root / "runs" / "z4_run_1" / "tasks" / "fetch_orders.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "task_id": "fetch_orders",
                        "details": {
                            "outputs": {
                                "orders_rows": {
                                    "kind": "rowset",
                                    "storage": "parquet",
                                    "uri": rowset_path.as_uri(),
                                    "rows": 0,
                                    "bytes": 7,
                                    "columns": ["id"],
                                    "column_specs": [{"name": "id", "type": "Int64", "nullable": False}],
                                    "schema_fingerprint": "test-fingerprint",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            resolved = resolve_rowset_ref(
                source_ref="fetch_orders.orders_rows",
                context={"run_id": "adapter_run_1", "z4_run_id": "z4_run_1"},
                home=root,
            )

        self.assertEqual(resolved.uri, rowset_path.as_uri())

    def test_runtime_home_uses_api_home_key_and_ignores_unknown_home_key(self) -> None:
        self.assertEqual(runtime_home({"zeta4s_api_home": "/tmp/api-home"}), Path("/tmp/api-home"))
        self.assertEqual(runtime_home({"runtime_home": "/tmp/runtime-home"}), Path("/tmp/runtime-home"))
        self.assertIsNone(runtime_home({"zeta4s_home": "/tmp/unused-home"}))

    def test_stage_column_specs_prefer_rowset_metadata(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty_rows.parquet"
            write_reader_to_parquet_rowset(
                reader=EmptyRowsReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_empty"},
            )
            parquet_file = pq.ParquetFile(path)
            specs = _column_specs(
                types.SimpleNamespace(column_specs=(), source_ref="fetch_empty.empty_rows"),
                parquet_file.schema_arrow,
            )

        self.assertEqual(_column_spec_tuples(specs), EmptyRowsReader.column_specs)


if __name__ == "__main__":
    unittest.main()
