from __future__ import annotations

import sys
import unittest

from zeta4s.runtime.context import current_context_from_kwargs


class RuntimeContextTest(unittest.TestCase):
    def test_explicit_runtime_context_does_not_require_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            context = current_context_from_kwargs(
                {
                    "runtime_context": {"dag_id": "core", "task_id": "step"},
                    "project_id": "demo",
                    "connection_types": {"analytics": "clickhouse"},
                },
                exclude_keys={"connection_types"},
            )
        finally:
            sys.modules.update(airflow_modules)

        self.assertEqual(context["dag_id"], "core")
        self.assertEqual(context["task_id"], "step")
        self.assertEqual(context["project_id"], "demo")
        self.assertNotIn("connection_types", context)

    def test_rejects_non_mapping_runtime_context(self):
        with self.assertRaisesRegex(ValueError, "runtime_context must be a mapping"):
            current_context_from_kwargs({"runtime_context": ["not", "mapping"]})

    def test_sets_run_date_when_requested(self):
        context = current_context_from_kwargs({"runtime_context": {}}, set_run_date=True)

        self.assertIn("run_date", context)

    def test_core_execution_kwargs_do_not_leak_into_runtime_context(self):
        context = current_context_from_kwargs(
            {
                "runtime_context": {"task_id": "stage_orders"},
                "step_execution": object(),
                "step_execution_payload": {"step_id": "stage_orders"},
                "project_id": "demo",
            }
        )

        self.assertEqual(context["task_id"], "stage_orders")
        self.assertEqual(context["project_id"], "demo")
        self.assertNotIn("step_execution", context)
        self.assertNotIn("step_execution_payload", context)


if __name__ == "__main__":
    unittest.main()
