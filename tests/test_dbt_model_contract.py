from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import yaml

from zeta4s.dbt.model_contract import validate_dbt_model_contract


class DbtModelContractTest(unittest.TestCase):
    def test_validates_project_level_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {"+materialized": "table"}})
            _write_model(project, "orders", "select 1 as id\n")

            result = validate_dbt_model_contract(project, run_models=["orders"])

            self.assertEqual(result.checked, 1)
            self.assertEqual(result.materialized, 1)

    def test_validates_properties_yaml_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {}})
            _write_model(project, "orders", "select 1 as id\n")
            _write_properties(project, {"models": [{"name": "orders", "config": {"materialized": "table"}}]})

            result = validate_dbt_model_contract(project, run_models=["orders"])

            self.assertEqual(result.checked, 1)

    def test_validates_sql_config_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {}})
            _write_model(project, "orders", "{{ config(materialized='table') }}\nselect 1 as id\n")

            result = validate_dbt_model_contract(project, run_models=["orders"])

            self.assertEqual(result.checked, 1)

    def test_rejects_non_table_run_model_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {"+materialized": "view"}})
            _write_model(project, "orders", "select 1 as id\n")

            with self.assertRaisesRegex(ValueError, "materialized=view"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_scoped_model_config_over_project_level_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(
                Path(tmp),
                {
                    "analytics": {
                        "+materialized": "table",
                        "orders": {"+materialized": "view"},
                    }
                },
            )
            _write_model(project, "orders", "select 1 as id\n")

            with self.assertRaisesRegex(ValueError, "materialized=view"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_scoped_folder_config_over_project_level_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(
                Path(tmp),
                {
                    "analytics": {
                        "+materialized": "table",
                        "mart": {"+materialized": "view"},
                    }
                },
            )
            _write_model(project, "orders", "select 1 as id\n", folder="mart")

            with self.assertRaisesRegex(ValueError, "materialized=view"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_dbt_project_model_config_outside_project_name(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(
                Path(tmp),
                {
                    "analytics": {"+materialized": "table"},
                    "orders": {"+materialized": "view"},
                },
            )
            _write_model(project, "orders", "select 1 as id\n")

            with self.assertRaisesRegex(ValueError, "unsupported=orders"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_dbt_project_model_config_outside_project_name_before_sql_config(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(
                Path(tmp),
                {
                    "analytics": {"+materialized": "table"},
                    "orders": {"+materialized": "view"},
                },
            )
            _write_model(project, "orders", "{{ config(materialized='table') }}\nselect 1 as id\n")

            with self.assertRaisesRegex(ValueError, "unsupported=orders"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_dbt_project_model_config_outside_project_name_before_properties_config(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(
                Path(tmp),
                {
                    "analytics": {"+materialized": "table"},
                    "orders": {"+materialized": "view"},
                },
            )
            _write_model(project, "orders", "select 1 as id\n")
            _write_properties(project, {"models": [{"name": "orders", "config": {"materialized": "table"}}]})

            with self.assertRaisesRegex(ValueError, "unsupported=orders"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_dbt_project_name_that_does_not_match_conn(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"other": {"+materialized": "table"}}, name="other")
            _write_model(project, "orders", "select 1 as id\n")

            with self.assertRaisesRegex(ValueError, "name must match conn id"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_rejects_dbt_project_profile_that_does_not_match_conn(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {"+materialized": "table"}}, profile="other")
            _write_model(project, "orders", "select 1 as id\n")

            with self.assertRaisesRegex(ValueError, "profile must match conn id"):
                validate_dbt_model_contract(project, run_models=["orders"])

    def test_test_models_require_sql_but_not_table_materialization(self) -> None:
        with TemporaryDirectory() as tmp:
            project = _dbt_project(Path(tmp), {"analytics": {}})
            _write_model(project, "orders", "select 1 as id\n")

            result = validate_dbt_model_contract(project, run_models=[], test_models=["orders"])

            self.assertEqual(result.checked, 1)
            self.assertEqual(result.materialized, 0)


def _dbt_project(root: Path, models_config: dict, *, name: str = "analytics", profile: str = "analytics") -> Path:
    project = root / "analytics"
    (project / "models").mkdir(parents=True)
    (project / "tests").mkdir()
    (project / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "version": "0.1.0",
                "config-version": 2,
                "profile": profile,
                "model-paths": ["models"],
                "test-paths": ["tests"],
                "models": models_config,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return project


def _write_model(project: Path, name: str, sql: str, *, folder: str | None = None) -> None:
    models_dir = project / "models"
    target_dir = models_dir / folder if folder else models_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"{name}.sql").write_text(sql, encoding="utf-8")


def _write_properties(project: Path, payload: dict) -> None:
    (project / "models" / "schema.yml").write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
