from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import Mock, patch

import zeta4s.runtime.external_lookup as external_lookup
from zeta4s.runtime.external_lookup import _run_external_lookup_impl


class HttpLookupRuntimeContractTest(unittest.TestCase):
    def test_http_lookup_dispatches_by_runtime_connection(self) -> None:
        with patch(
            "zeta4s.runtime.backends.clickhouse.http_lookup.run_clickhouse_http_lookup",
            return_value={
                "input_rows": 1,
                "output_rows": 1,
                "success_rows": 1,
                "failed_rows": 0,
                "skipped_rows": 0,
                "error_rows": 0,
            },
        ) as run_backend:
            result = _run_external_lookup_impl(
                name="enrich_customer",
                mode="mock",
                conn="analytics_clickhouse",
                source_table="mart.customer_360",
                target_table="mart.customer_360_enriched",
                input_column="customer_name",
                output_columns=[{"name": "priority_label", "type": "String"}],
                concurrency=1,
                batch_size=1000,
                http=None,
                kwargs={"connections": {"analytics_clickhouse": {"type": "clickhouse"}}},
            )

        run_backend.assert_called_once()
        self.assertEqual(run_backend.call_args.kwargs["conn_id"], "analytics_clickhouse")
        self.assertEqual(result["success_rows"], 1)

    def test_http_lookup_dispatches_to_oracle_runtime_connection(self) -> None:
        run_backend = Mock(
            return_value={
                "input_rows": 1,
                "output_rows": 1,
                "success_rows": 1,
                "failed_rows": 0,
                "skipped_rows": 0,
                "error_rows": 0,
            }
        )
        fake_module = types.ModuleType("zeta4s.runtime.backends.oracle.http_lookup")
        fake_module.run_oracle_http_lookup = run_backend
        with patch.dict(sys.modules, {"zeta4s.runtime.backends.oracle.http_lookup": fake_module}):
            result = _run_external_lookup_impl(
                name="enrich_customer",
                mode="mock",
                conn="retail_oracle",
                source_table="customer_360",
                target_table="customer_360_enriched",
                input_column="customer_name",
                output_columns=[{"name": "priority_label", "type": "String"}],
                concurrency=1,
                batch_size=1000,
                http=None,
                kwargs={"connections": {"retail_oracle": {"type": "oracle"}}},
            )

        run_backend.assert_called_once()
        self.assertEqual(run_backend.call_args.kwargs["conn_id"], "retail_oracle")
        self.assertEqual(run_backend.call_args.kwargs["output_columns"][0]["nullable"], True)
        self.assertEqual(result["success_rows"], 1)

    def test_http_lookup_records_actual_http_request_metrics(self) -> None:
        def run_backend(**kwargs):
            enrich_row = kwargs["enrich_row"]
            enriched, error = enrich_row(("web",), 0)
            self.assertFalse(error)
            self.assertEqual(enriched, ("web", "medium"))
            return {
                "input_rows": 1,
                "output_rows": 1,
                "success_rows": 1,
                "failed_rows": 0,
                "skipped_rows": 0,
                "error_rows": 0,
            }

        http_connection = external_lookup._HttpConnection(
            base_url="http://priority-api:8099",
            headers={"Content-Type": "application/json"},
        )
        with (
            patch(
                "zeta4s.runtime.external_lookup._http_connection",
                return_value=http_connection,
            ) as get_http_connection,
            patch(
                "zeta4s.runtime.external_lookup._request_json",
                return_value={"priority_label": "medium"},
            ) as request_json,
            patch(
                "zeta4s.runtime.backends.clickhouse.http_lookup.run_clickhouse_http_lookup",
                side_effect=run_backend,
            ),
        ):
            result = _run_external_lookup_impl(
                name="enrich_customer",
                mode="http",
                conn="retail_clickhouse",
                source_table="customer_360",
                target_table="customer_360_enriched",
                input_column="channel",
                output_columns=[{"name": "priority_label", "type": "String"}],
                concurrency=1,
                batch_size=1000,
                http={
                    "conn": "retail_priority_api",
                    "path": "/classify",
                    "method": "GET",
                    "request_query_param": "description",
                    "response_json_paths": ["$.priority_label"],
                },
                kwargs={
                    "connections": {
                        "retail_clickhouse": {"type": "clickhouse"},
                        "retail_priority_api": {"type": "http"},
                    }
                },
            )

        get_http_connection.assert_called_once_with(
            "retail_priority_api",
            connections={
                "retail_clickhouse": {"type": "clickhouse"},
                "retail_priority_api": {"type": "http"},
            },
        )
        request_json.assert_called_once_with(
            url="http://priority-api:8099/classify",
            method="GET",
            payload={"description": "web"},
            timeout_seconds=30,
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(result["http_request_success_rows"], 1)
        self.assertEqual(result["http_request_failed_rows"], 0)

    def test_oracle_http_lookup_uses_separate_read_cursor_across_batches(self) -> None:
        sys.modules.setdefault("oracledb", types.ModuleType("oracledb"))
        module = importlib.import_module("zeta4s.runtime.backends.oracle.http_lookup")
        conn = _FakeOracleConnection(
            [
                (1, "web"),
                (2, "store"),
                (3, "marketplace"),
            ]
        )

        with (
            patch.object(module, "get_oracle_conn", return_value=conn),
            patch.object(module, "drop_oracle_table"),
            patch.object(
                module,
                "log_task_event",
            ),
        ):
            result = module.run_oracle_http_lookup(
                conn_id="retail_oracle",
                name="enrich_daily_revenue_channel_oracle",
                mode="mock",
                source_table="daily_revenue",
                target_table="daily_revenue_enriched",
                input_column="channel",
                output_columns=[{"name": "priority_label", "type": "String", "nullable": True}],
                concurrency=1,
                batch_size=2,
                enrich_row=lambda row, input_idx: (tuple(row) + ("medium",), False),
                context={},
                http=None,
            )

        self.assertEqual(result["success_rows"], 3)
        self.assertEqual(len(conn.cursors), 2)
        write_cursor, read_cursor = conn.cursors
        self.assertEqual(len(write_cursor.executemany_batches), 2)
        self.assertEqual([len(batch) for batch in write_cursor.executemany_batches], [2, 1])
        self.assertEqual(read_cursor.executemany_batches, [])


class _FakeOracleConnection:
    def __init__(self, rows):
        self.rows = rows
        self.cursors = []
        self.committed = False

    def cursor(self):
        cursor = _FakeOracleCursor(self.rows if self.cursors else [])
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass


class _FakeOracleCursor:
    def __init__(self, rows):
        self.rows = rows
        self.description = []
        self.executemany_batches = []

    def execute(self, sql, params=None):
        normalized = sql.strip().upper()
        if normalized.startswith("SELECT * FROM"):
            self.description = [("ID", object()), ("CHANNEL", object())]
        return self

    def executemany(self, sql, rows):
        self.executemany_batches.append(list(rows))

    def __iter__(self):
        return iter(self.rows)

    def close(self):
        pass


if __name__ == "__main__":
    unittest.main()
