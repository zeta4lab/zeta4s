from __future__ import annotations

import types
import unittest
from unittest.mock import Mock, patch

from zeta4s.runtime.dbt import run_dbt_node


class RuntimeDbtConnectionsTest(unittest.TestCase):
    def test_dbt_router_uses_explicit_clickhouse_connection_mapping(self):
        run_backend = Mock(return_value={"status": "success"})
        fake_module = types.ModuleType("zeta4s.runtime.backends.clickhouse.dbt")
        fake_module.run_clickhouse_dbt_node = run_backend

        with patch.dict("sys.modules", {"zeta4s.runtime.backends.clickhouse.dbt": fake_module}):
            result = run_dbt_node(
                conn_id="analytics",
                dbt_project_path="/tmp/dbt",
                unique_id="model.analytics.orders",
                resource_type="model",
                node_name="orders",
                command="run",
                connections={"analytics": {"type": "clickhouse"}},
            )

        self.assertEqual(result, {"status": "success"})
        run_backend.assert_called_once()
        self.assertEqual(run_backend.call_args.kwargs["conn_id"], "analytics")

    def test_dbt_router_uses_explicit_oracle_connection_mapping(self):
        run_backend = Mock(return_value={"status": "success"})
        fake_module = types.ModuleType("zeta4s.runtime.backends.oracle.dbt")
        fake_module.run_oracle_dbt_node = run_backend

        with patch.dict("sys.modules", {"zeta4s.runtime.backends.oracle.dbt": fake_module}):
            result = run_dbt_node(
                conn_id="warehouse",
                dbt_project_path="/tmp/dbt",
                unique_id="model.warehouse.orders",
                resource_type="model",
                node_name="orders",
                command="run",
                connections={"warehouse": {"type": "oracle"}},
            )

        self.assertEqual(result, {"status": "success"})
        run_backend.assert_called_once()
        self.assertEqual(run_backend.call_args.kwargs["conn_id"], "warehouse")


if __name__ == "__main__":
    unittest.main()
