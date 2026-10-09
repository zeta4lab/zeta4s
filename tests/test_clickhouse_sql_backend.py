from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "zeta4s_clickhouse_sql_backend_for_test",
    ROOT / "src/zeta4s/runtime/backends/clickhouse/sql.py",
)
assert SPEC is not None and SPEC.loader is not None
clickhouse_sql_backend = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = clickhouse_sql_backend
SPEC.loader.exec_module(clickhouse_sql_backend)
run_clickhouse_step = clickhouse_sql_backend.run_clickhouse_step
run_clickhouse_scalar_step = clickhouse_sql_backend.run_clickhouse_scalar_step


class _FakeQueryResult:
    def __init__(self, first_row):
        self.first_row = first_row


class _FakeClickHouseClient:
    def __init__(self, first_row):
        self.first_row = first_row
        self.commands: list[str] = []
        self.queries: list[str] = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def query(self, sql, parameters=None):
        self.queries.append(sql)
        return _FakeQueryResult(self.first_row)


class ClickHouseSqlBackendTest(unittest.TestCase):
    def test_check_step_passes_when_scalar_result_is_true(self) -> None:
        client = _FakeClickHouseClient((1,))
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ) as get_client:
            result = run_clickhouse_step(
                project_root=".",
                step={"type": "check", "name": "assert_sales", "sql": "select 1"},
                conn_id="analytics_clickhouse",
                step_index=1,
                context={},
                result_stage="sql_check",
                bind_params=lambda _step, _context, _sql: {},
            )

        self.assertEqual(result, 0)
        get_client.assert_called_once_with("analytics_clickhouse", connections=None)
        self.assertEqual(client.queries, ["select 1"])
        self.assertEqual(client.commands, [])

    def test_check_step_fails_when_scalar_result_is_false(self) -> None:
        client = _FakeClickHouseClient((0,))
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ) as get_client:
            with self.assertRaisesRegex(RuntimeError, "native check failed"):
                run_clickhouse_step(
                    project_root=".",
                    step={"type": "check", "name": "assert_sales", "sql": "select 0"},
                    conn_id="analytics_clickhouse",
                    step_index=1,
                    context={},
                    result_stage="sql_check",
                    bind_params=lambda _step, _context, _sql: {},
                )

        get_client.assert_called_once_with("analytics_clickhouse", connections=None)
        self.assertEqual(client.queries, ["select 0"])
        self.assertEqual(client.commands, [])

    def test_transform_step_uses_declared_connection_client(self) -> None:
        client = _FakeClickHouseClient(None)
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ) as get_client:
            result = run_clickhouse_step(
                project_root=".",
                step={"type": "sql", "name": "build_metrics", "sql": "insert into mart.metrics select 1"},
                conn_id="analytics_clickhouse",
                step_index=1,
                context={},
                result_stage="sql_transform",
                bind_params=lambda _step, _context, _sql: {},
            )

        self.assertEqual(result, 0)
        get_client.assert_called_once_with("analytics_clickhouse", connections=None)

    def test_transform_step_passes_runtime_connections_to_client(self) -> None:
        client = _FakeClickHouseClient(None)
        connections = {"analytics_clickhouse": {"type": "clickhouse", "host": "clickhouse"}}
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ) as get_client:
            run_clickhouse_step(
                project_root=".",
                step={"type": "sql", "name": "build_metrics", "sql": "insert into mart.metrics select 1"},
                conn_id="analytics_clickhouse",
                step_index=1,
                context={},
                result_stage="sql_transform",
                bind_params=lambda _step, _context, _sql: {},
                connections=connections,
            )

        get_client.assert_called_once_with("analytics_clickhouse", connections=connections)
        self.assertEqual(client.queries, [])
        self.assertEqual(client.commands, ["insert into mart.metrics select 1"])

    def test_scalar_step_reads_named_outputs(self) -> None:
        client = _FakeClickHouseClient(("42", "true"))
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ):
            result = run_clickhouse_scalar_step(
                project_root=".",
                step={
                    "type": "scalar",
                    "name": "count_sales",
                    "sql": "select 42, 'true'",
                    "outputs": {
                        "row_count": {"kind": "scalar", "type": "int", "column": 1},
                        "ready": {"kind": "scalar", "type": "bool", "column": 2},
                    },
                },
                conn_id="clickhouse_runtime",
                context={},
                bind_params=lambda _step, _context, _sql: {},
            )

        self.assertEqual(result, {"row_count": 42, "ready": True})
        self.assertEqual(client.queries, ["select 42, 'true'"])
        self.assertEqual(client.commands, [])

    def test_scalar_step_rejects_ambiguous_bool_output(self) -> None:
        client = _FakeClickHouseClient(("yes",))
        with patch(
            f"{clickhouse_sql_backend.__name__}.get_clickhouse_runtime_client",
            return_value=client,
        ):
            with self.assertRaisesRegex(ValueError, "sql.scalar bool output"):
                run_clickhouse_scalar_step(
                    project_root=".",
                    step={
                        "type": "scalar",
                        "name": "flag",
                        "sql": "select 'yes'",
                        "outputs": {
                            "ready": {"kind": "scalar", "type": "bool", "column": 1},
                        },
                    },
                    conn_id="clickhouse_runtime",
                    context={},
                    bind_params=lambda _step, _context, _sql: {},
                )


if __name__ == "__main__":
    unittest.main()
