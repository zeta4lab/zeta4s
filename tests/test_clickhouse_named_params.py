from __future__ import annotations

import datetime as dt
from decimal import Decimal
import os
import unittest
from unittest.mock import patch
import uuid

from zeta4s.runtime.backends.clickhouse import extract as clickhouse_extract
from zeta4s.runtime.backends.clickhouse import sql as clickhouse_sql
from zeta4s.runtime.backends.clickhouse.params import bind_clickhouse_named_params, clickhouse_param_type


class BindClickHouseNamedParamsTest(unittest.TestCase):
    def test_rewrites_named_placeholders_to_server_side_binds(self) -> None:
        sql, params = bind_clickhouse_named_params(
            "SELECT * FROM t WHERE id = :id AND name = :name AND id <> :id",
            {"id": 3, "name": "a"},
        )

        self.assertEqual(sql, "SELECT * FROM t WHERE id = {id:Int64} AND name = {name:String} AND id <> {id:Int64}")
        self.assertEqual(params, {"id": 3, "name": "a"})

    def test_leaves_string_literals_identifiers_and_comments_untouched(self) -> None:
        source = (
            "SELECT 'a:b', 'it''s :x', 'esc\\' :x', \"col:x\", `col:x`, $$ :x $$ FROM t /* :x */ WHERE c = :x -- :x\n"
        )

        sql, params = bind_clickhouse_named_params(source, {"x": "v"})

        self.assertEqual(
            sql,
            "SELECT 'a:b', 'it''s :x', 'esc\\' :x', \"col:x\", `col:x`, $$ :x $$ FROM t"
            " /* :x */ WHERE c = {x:String} -- :x\n",
        )
        self.assertEqual(params, {"x": "v"})

    def test_keeps_double_colon_cast_and_native_binds(self) -> None:
        sql, params = bind_clickhouse_named_params(
            "SELECT '1'::Int32, {native:UInt8}, json.a.:Int64, :v::String",
            {"v": 1, "Int32": 9, "UInt8": 9, "Int64": 9},
        )

        self.assertEqual(sql, "SELECT '1'::Int32, {native:UInt8}, json.a.:Int64, {v:Int64}::String")
        self.assertEqual(params, {"v": 1})

    def test_does_not_escape_percent_literals(self) -> None:
        sql, params = bind_clickhouse_named_params("SELECT count() FROM t WHERE s LIKE '%x%' AND id > :id", {"id": 1})

        self.assertEqual(sql, "SELECT count() FROM t WHERE s LIKE '%x%' AND id > {id:Int64}")
        self.assertEqual(params, {"id": 1})

    def test_missing_parameter_fails_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, r"without values: :window_start, :window_end"):
            bind_clickhouse_named_params(
                "SELECT 1 WHERE ts >= :window_start AND ts < :window_end AND ts <> :window_start", {}
            )

    def test_without_placeholders_returns_sql_unchanged_and_drops_unused_params(self) -> None:
        source = "SELECT count() FROM t WHERE s LIKE '%x%'"

        self.assertEqual(bind_clickhouse_named_params(source, {"select_from": 1}), (source, None))
        self.assertEqual(bind_clickhouse_named_params(source, None), (source, None))

    def test_unused_params_are_not_sent(self) -> None:
        _, params = bind_clickhouse_named_params("SELECT :a", {"a": 1, "cur_wm": 2})

        self.assertEqual(params, {"a": 1})

    def test_datetime_keeps_microseconds_and_normalizes_aware_values_to_utc(self) -> None:
        kst = dt.timezone(dt.timedelta(hours=9))
        sql, params = bind_clickhouse_named_params(
            "SELECT :naive, :aware",
            {
                "naive": dt.datetime(2026, 1, 1, 0, 0, 0, 500000),
                "aware": dt.datetime(2026, 1, 1, 9, 0, 0, 250000, tzinfo=kst),
            },
        )

        self.assertEqual(sql, "SELECT {naive:DateTime64(6)}, {aware:DateTime64(6, 'UTC')}")
        self.assertEqual(
            params,
            {"naive": "2026-01-01 00:00:00.500000", "aware": "2026-01-01 00:00:00.250000"},
        )

    def test_type_inference(self) -> None:
        self.assertEqual(clickhouse_param_type(None), "Nullable(Nothing)")
        self.assertEqual(clickhouse_param_type(True), "Bool")
        self.assertEqual(clickhouse_param_type(2**63), "Int256")
        self.assertEqual(clickhouse_param_type(1.5), "Float64")
        self.assertEqual(clickhouse_param_type(Decimal("1.25")), "Decimal(76, 2)")
        self.assertEqual(clickhouse_param_type(dt.date(2026, 1, 1)), "Date32")
        self.assertEqual(clickhouse_param_type(uuid.UUID(int=0)), "UUID")
        self.assertEqual(clickhouse_param_type([1, 2]), "Array(Int64)")
        self.assertEqual(clickhouse_param_type(["a", None]), "Array(Nullable(String))")
        self.assertEqual(clickhouse_param_type([]), "Array(Nothing)")

    def test_unsupported_value_fails_clearly(self) -> None:
        with self.assertRaisesRegex(ValueError, r":ids list elements must share one ClickHouse type"):
            bind_clickhouse_named_params("SELECT :ids", {"ids": [1, "a"]})
        with self.assertRaisesRegex(ValueError, r":obj has unsupported type object"):
            bind_clickhouse_named_params("SELECT :obj", {"obj": object()})


class _RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []

    def query(self, sql, parameters=None):
        self.calls.append(("query", sql, parameters))
        return type("Result", (), {"first_row": (1,), "result_rows": []})()

    def command(self, sql, parameters=None):
        self.calls.append(("command", sql, parameters))


class ClickHouseBackendBindingTest(unittest.TestCase):
    def test_sql_step_sends_server_side_binds(self) -> None:
        client = _RecordingClient()
        with patch.object(clickhouse_sql, "get_clickhouse_runtime_client", return_value=client):
            clickhouse_sql.run_clickhouse_step(
                project_root=".",
                step={"type": "sql", "sql": "insert into t select * from s where ds = :ds and s like '%x%'"},
                conn_id="ch",
                step_index=1,
                context={},
                result_stage="sql_transform",
                bind_params=lambda _step, _context, _sql: {"ds": "2026-01-01"},
            )

        self.assertEqual(
            client.calls,
            [
                (
                    "command",
                    "insert into t select * from s where ds = {ds:String} and s like '%x%'",
                    {"ds": "2026-01-01"},
                )
            ],
        )

    def test_extract_reader_binds_window_predicates(self) -> None:
        reader = clickhouse_extract.ClickHouseSelectReader(
            source_conn="ch",
            source_object="t",
            query="SELECT * FROM t WHERE ts >= :window_start AND ts < :window_end",
            params={
                "window_start": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                "window_end": dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc),
                "select_from": None,
            },
            batch_size=10,
        )

        self.assertEqual(
            reader.query,
            "SELECT * FROM t WHERE ts >= {window_start:DateTime64(6, 'UTC')} AND ts < {window_end:DateTime64(6, 'UTC')}",
        )
        self.assertEqual(
            reader.params,
            {"window_start": "2026-01-01 00:00:00.000000", "window_end": "2026-01-02 00:00:00.000000"},
        )


@unittest.skipUnless(
    os.environ.get("ZETA4S_TEST_CLICKHOUSE_DSN"),
    "ZETA4S_TEST_CLICKHOUSE_DSN is required",
)
class ClickHouseNamedParamsIntegrationTest(unittest.TestCase):
    """Run against a real server, e.g. ZETA4S_TEST_CLICKHOUSE_DSN=clickhouse://default:pw@127.0.0.1:8123/default."""

    table = "z4s_named_params_it"

    @classmethod
    def setUpClass(cls) -> None:
        import clickhouse_connect

        cls.client = clickhouse_connect.get_client(dsn=os.environ["ZETA4S_TEST_CLICKHOUSE_DSN"])
        cls.client.command(f"DROP TABLE IF EXISTS {cls.table}")
        cls.client.command(
            f"CREATE TABLE {cls.table} (id UInt32, d Date, ts DateTime, ts6 DateTime64(6, 'UTC'), s String)"
            " ENGINE = MergeTree ORDER BY id"
        )
        cls.client.command(
            f"INSERT INTO {cls.table} VALUES"
            " (1, '2026-01-01', '2026-01-01 00:00:00', '2026-01-01 00:00:00.000000', 'ax'),"
            " (2, '2026-01-02', '2026-01-01 00:00:01', '2026-01-01 00:00:00.500000', 'b:c'),"
            " (3, '2026-01-03', '2026-01-01 00:00:02', '2026-01-01 00:00:00.900000', '100%')"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.command(f"DROP TABLE IF EXISTS {cls.table}")

    def _count(self, where: str, params: dict) -> int:
        sql, bound = bind_clickhouse_named_params(f"SELECT count() FROM {self.table} WHERE {where}", params)
        return self.client.query(sql, parameters=bound).first_row[0]

    def test_named_params_bind_against_server(self) -> None:
        utc = dt.timezone.utc
        self.assertEqual(self._count("ts6 <= :wm", {"wm": dt.datetime(2026, 1, 1, 0, 0, 0, 500000, tzinfo=utc)}), 2)
        self.assertEqual(self._count("ts <= :wm", {"wm": dt.datetime(2026, 1, 1, 0, 0, 1, 500000)}), 2)
        self.assertEqual(self._count("d >= :start", {"start": dt.datetime(2026, 1, 2)}), 2)
        self.assertEqual(self._count("d >= :start", {"start": dt.date(2026, 1, 2)}), 2)
        self.assertEqual(self._count("ts >= :start", {"start": "2026-01-01 00:00:01"}), 2)
        self.assertEqual(self._count("s LIKE '%x%' AND id >= :id", {"id": 1}), 1)
        self.assertEqual(self._count("s = 'b:c' AND id IN :ids", {"ids": [1, 2]}), 1)
        self.assertEqual(self._count("s = :s AND coalesce(:missing, 1) = 1", {"s": "100%", "missing": None}), 1)

    def test_extract_reader_streams_with_named_params(self) -> None:
        reader = clickhouse_extract.ClickHouseSelectReader(
            source_conn="ch",
            source_object=self.table,
            query=f"SELECT id FROM {self.table} WHERE ts6 > :select_from AND ts6 <= :cur_wm ORDER BY id",
            params={
                "select_from": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                "cur_wm": dt.datetime(2026, 1, 1, 0, 0, 0, 900000, tzinfo=dt.timezone.utc),
            },
            batch_size=10,
        )
        with patch.object(clickhouse_extract, "get_clickhouse_source_client", return_value=self.client):
            with reader:
                ids = [value for batch in reader.read_batches() for value in batch.arrow_table.column("id").to_pylist()]

        self.assertEqual(reader.columns, ["id"])
        self.assertEqual(ids, [2, 3])


if __name__ == "__main__":
    unittest.main()
