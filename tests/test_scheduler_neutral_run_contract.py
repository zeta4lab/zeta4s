from __future__ import annotations

import json
import unittest

from click.testing import CliRunner

from zeta4s.api.app import create_app
from zeta4s.cli.main import cli


class SchedulerNeutralRunContractTest(unittest.TestCase):
    def test_cli_exposes_run_group_without_dag_group(self) -> None:
        api = cli.commands["api"]
        self.assertIn("run", api.commands)
        self.assertNotIn("dag", api.commands)
        self.assertEqual(
            set(api.commands["run"].commands),
            {"create", "list", "status", "tasks", "summary", "logs", "artifacts", "cancel"},
        )
        self.assertIn("run", cli.commands)

        result = CliRunner().invoke(cli, ["api", "run", "create", "--help"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("--parameters", result.output)
        self.assertNotIn("--conf", result.output)

    def test_openapi_common_surface_has_no_scheduler_native_vocabulary(self) -> None:
        schema = create_app().openapi()
        common = {
            "paths": {
                path: value
                for path, value in schema["paths"].items()
                if path.startswith("/api/") or path.startswith("/internal/v1/runtime/")
            },
            "schemas": schema.get("components", {}).get("schemas", {}),
        }
        rendered = json.dumps(common, ensure_ascii=False, sort_keys=True).lower()
        for native_word in ("airflow", "prefect", "dag", "flow"):
            self.assertNotIn(native_word, rendered)

    def test_run_create_accepts_parameters_schema(self) -> None:
        schemas = create_app().openapi()["components"]["schemas"]
        request = schemas["RunCreateRequest"]
        self.assertEqual(set(request["properties"]), {"parameters"})

    def test_internal_routes_are_runtime_scoped(self) -> None:
        paths = set(create_app().openapi()["paths"])
        self.assertIn("/internal/v1/runtime/connections/{conn_id}", paths)
        self.assertIn("/internal/v1/runtime/steps/execute", paths)
        self.assertIn("/internal/v1/runtime/runs/finalize", paths)
        self.assertFalse(any(path.startswith("/internal/v1/airflow") for path in paths))


if __name__ == "__main__":
    unittest.main()
