from __future__ import annotations

import sys
import unittest

from zeta4s.runtime import native


class RuntimeNativeContextTest(unittest.TestCase):
    def test_current_context_uses_explicit_runtime_context_without_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            context = native._current_context(
                {
                    "runtime_context": {"dag_id": "core", "task_id": "scalar"},
                    "project_id": "demo",
                    "connection_types": {"analytics": "clickhouse"},
                    "connections": {"analytics": {"type": "clickhouse"}},
                }
            )
        finally:
            sys.modules.update(airflow_modules)

        self.assertEqual(context["dag_id"], "core")
        self.assertEqual(context["task_id"], "scalar")
        self.assertEqual(context["project_id"], "demo")
        self.assertNotIn("connection_types", context)
        self.assertNotIn("connections", context)

    def test_resolve_native_engine_uses_connection_type_map_before_airflow(self):
        engine = native._resolve_native_engine(
            step={"name": "count_rows"},
            default_engine="auto",
            conn="analytics",
            connection_types={"analytics": "clickhouse"},
        )

        self.assertEqual(engine, "clickhouse")

    def test_resolve_native_engine_uses_runtime_connections_without_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            engine = native._resolve_native_engine(
                step={"name": "count_rows"},
                default_engine="auto",
                conn="analytics",
                connections={"analytics": {"type": "clickhouse", "host": "clickhouse"}},
            )
        finally:
            sys.modules.update(airflow_modules)

        self.assertEqual(engine, "clickhouse")

    def test_current_context_rejects_non_mapping_runtime_context(self):
        with self.assertRaisesRegex(ValueError, "runtime_context must be a mapping"):
            native._current_context({"runtime_context": ["not", "a", "mapping"]})

    def test_resolve_native_engine_rejects_unsupported_connection_type_map_value(self):
        with self.assertRaisesRegex(ValueError, "unsupported native step connection type"):
            native._resolve_native_engine(
                step={"name": "count_rows"},
                default_engine="auto",
                conn="analytics",
                connection_types={"analytics": "postgres"},
            )


if __name__ == "__main__":
    unittest.main()
