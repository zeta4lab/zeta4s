from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from zeta4s.runtime import dbt_runner


class RuntimeDbtRunnerContextTest(unittest.TestCase):
    def test_current_context_uses_explicit_runtime_context_without_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            context = dbt_runner._current_context(
                {
                    "runtime_context": {"dag_id": "core", "task_id": "dbt_node"},
                    "project_id": "demo",
                    "connections": {"analytics": {"type": "clickhouse"}},
                }
            )
        finally:
            sys.modules.update(airflow_modules)

        self.assertEqual(context["dag_id"], "core")
        self.assertEqual(context["task_id"], "dbt_node")
        self.assertEqual(context["project_id"], "demo")
        self.assertNotIn("connections", context)

    def test_local_run_id_and_task_id_are_used_for_target_path_context(self):
        context = {"run_id": "local__retail__daily__001", "task_id": "dbt_models.orders"}

        self.assertEqual(dbt_runner._run_id(context), "local__retail__daily__001")
        self.assertEqual(dbt_runner._task_id(context), "dbt_models.orders")

    def test_z4_run_id_takes_precedence_for_target_path_context(self):
        context = {"z4_run_id": "z4__001", "run_id": "airflow__001", "task_id": "dbt_models.orders"}

        self.assertEqual(dbt_runner._run_id(context), "z4__001")

    def test_current_context_rejects_non_mapping_runtime_context(self):
        with self.assertRaisesRegex(ValueError, "runtime_context must be a mapping"):
            dbt_runner._current_context({"runtime_context": ["not", "a", "mapping"]})

    def test_dbt_command_selects_the_projected_node_name(self):
        completed = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        with TemporaryDirectory() as tmp:
            target = Path(tmp)
            with (
                patch.object(dbt_runner, "_current_context", return_value={"task_id": "dbt_test"}),
                patch.object(dbt_runner, "_target_path", return_value=target),
                patch.object(dbt_runner, "result_context", return_value=nullcontext(("started", 0.0))),
                patch.object(dbt_runner, "dbt_executable", return_value="dbt"),
                patch.object(dbt_runner.subprocess, "run", return_value=completed) as run,
                patch.object(
                    dbt_runner,
                    "_parse_run_results",
                    return_value={"results": [{"status": "pass", "failures": 0}]},
                ),
                patch.object(dbt_runner, "record_success", return_value={"status": "success"}),
            ):
                dbt_runner.run_dbt_node_with_profile(
                    conn_id="analytics",
                    conn_type="clickhouse",
                    dbt_project_path=tmp,
                    unique_id="test.analytics.not_null_products.abc123",
                    resource_type="test",
                    node_name="not_null_products",
                    command="test",
                    profiles_yml="analytics: {}",
                )

        command = run.call_args.args[0]
        selector_index = command.index("--select") + 1
        self.assertEqual(command[selector_index], "not_null_products")

    def test_dbt_target_id_groups_nodes_from_the_same_step(self):
        context = {
            "task_id": "test_products.not_null_products",
            "dbt_target_id": "test_products",
        }

        self.assertEqual(dbt_runner._target_id(context), "test_products")


if __name__ == "__main__":
    unittest.main()
