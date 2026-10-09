from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from zeta4s.airflow.dag_source import render_airflow_dag_source
from zeta4s.core import RuntimeCallableStepExecutor
from zeta4s.core.step_executors import built_in_step_executor, supported_builtin_step_types
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.pools import project_pool_name
from zeta4s.project.step_graph import ScheduleConfig, validate_step_graph_config

TASK_SPEC_KEYS = {
    "step_id",
    "pool",
    "retries",
    "retry_delay_seconds",
    "execution_timeout_seconds",
    "trigger_rule",
    "upstream_ids",
}


def _render(schedule: ScheduleConfig | None = None, *, project_timezone: str = "Asia/Seoul") -> tuple[str, str]:
    step = SimpleNamespace(
        id="extract",
        flow=SimpleNamespace(retry={"max_attempts": 2, "delay_seconds": 3}, timeout={"seconds": 30}),
        step=SimpleNamespace(when=None, join=None),
    )
    plan = SimpleNamespace(
        schedule=schedule,
        steps=[step],
        terminal_step_ids={"extract"},
        upstream_ids_by_step={"extract": ()},
    )
    project = SimpleNamespace(registered_at=None, timezone=project_timezone)
    registration = {
        "project_id": "retail",
        "artifact_id": "sha256:abc",
        "profile_id": "prod",
    }
    dag_spec = {"dag_id": "retail__daily", "job_id": "daily"}

    with (
        patch("zeta4s.airflow.dag_source.load_scheduled_plan", return_value=(plan, project)),
        patch("zeta4s.airflow.dag_source.execution_step_pool_name", return_value="retail.extract"),
    ):
        return render_airflow_dag_source(registration, dag_spec, home=Path("/tmp/runtime"))


def _project() -> ProjectContext:
    root = Path("/tmp/zeta4s-dag-source-test")
    return ProjectContext(
        project_id="retail",
        root=root,
        jobs_dir=root / "jobs",
        dbt_dir=root / "dbt",
        timezone="Asia/Seoul",
    )


def _plan(config: dict):
    return build_step_graph_execution_plan(validate_step_graph_config(Path("job.yml"), config))


def _render_job(config: dict, *, profile_id: str = "prod") -> tuple[str, dict]:
    plan = _plan(config)
    registration = {"project_id": "retail", "artifact_id": "sha256:abc", "profile_id": profile_id}
    dag_spec = {"job_id": config["job_id"]}
    with patch("zeta4s.airflow.dag_source.load_scheduled_plan", return_value=(plan, _project())):
        source, _ = render_airflow_dag_source(registration, dag_spec, home=Path("/tmp/runtime"))
    return source, _spec(source)


def _task(spec: dict, step_id: str) -> dict:
    return next(task for task in spec["tasks"] if task["step_id"] == step_id)


def _core_projection_contracts() -> dict[str, dict]:
    return {
        "noop": {
            "job_id": "noop_job",
            "steps": [{"step_id": "step_under_test", "type": "noop"}],
        },
        "oracle.extract": {
            "job_id": "oracle_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {"kind": "query", "query": "select id from source.orders"},
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "clickhouse.extract": {
            "job_id": "clickhouse_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "clickhouse.extract",
                    "conn": "clickhouse_source",
                    "source": {"kind": "query", "query": "select id from source.orders"},
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "elasticsearch.extract": {
            "job_id": "elasticsearch_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "elasticsearch.extract",
                    "conn": "search_source",
                    "source": {
                        "kind": "search",
                        "index": "orders",
                        "fields": [{"column": "id", "path": "id", "type": "int", "precision": 19}],
                    },
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "clickhouse.stage": _mapped_step_contract("clickhouse.stage", "clickhouse_stage_job", "mart.stg_orders"),
        "oracle.stage": _mapped_step_contract("oracle.stage", "oracle_stage_job", "mart.stg_orders"),
        "http.lookup": {
            "job_id": "http_lookup_job",
            "steps": [
                {
                    "step_id": "build_customer",
                    "type": "dbt.run",
                    "conn": "analytics",
                    "models": ["customer"],
                },
                {
                    "step_id": "step_under_test",
                    "type": "http.lookup",
                    "conn": "analytics",
                    "depends_on": ["build_customer"],
                    "lookup": {"table": "build_customer.customer", "column": "name"},
                    "target": {"table": "mart.customer_enriched"},
                    "api": {
                        "conn": "customer_api",
                        "method": "GET",
                        "response": {"columns": [{"name": "priority", "type": "String"}]},
                    },
                },
            ],
        },
        "dbt.run": _dbt_contract("dbt.run", "dbt_run_job"),
        "dbt.test": _dbt_contract("dbt.test", "dbt_test_job"),
        "sql.scalar": {
            "job_id": "sql_scalar_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "sql.scalar",
                    "conn": "analytics",
                    "sql": "select 1 as row_count",
                    "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                }
            ],
        },
        "clickhouse.write": _write_contract(
            "clickhouse.write",
            "clickhouse_write_job",
            {"table": "mart.orders", "mode": "replace", "columns": ["id"]},
        ),
        "oracle.write": _write_contract(
            "oracle.write",
            "oracle_write_job",
            {"table": "mart.orders", "mode": "upsert", "key": ["id"], "columns": ["id"]},
        ),
        "elasticsearch.write": _write_contract(
            "elasticsearch.write",
            "elasticsearch_write_job",
            {"index": "orders", "mode": "upsert", "document_id": {"columns": ["id"]}, "columns": ["id"]},
        ),
        "elasticsearch.command": {
            "job_id": "elasticsearch_command_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "elasticsearch.command",
                    "conn": "search_admin",
                    "operation": "bulk",
                    "target": {"index": "orders"},
                    "source": {"file": "elasticsearch/orders.ndjson", "format": "ndjson"},
                }
            ],
        },
        "sql.check": _sql_contract("sql.check", "sql_check_job"),
        "oracle.sql": _sql_contract("oracle.sql", "oracle_sql_job"),
        "clickhouse.sql": _sql_contract("clickhouse.sql", "clickhouse_sql_job"),
        "oracle.call": {
            "job_id": "oracle_call_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "oracle.call",
                    "conn": "analytics",
                    "call": "pkg.refresh_orders",
                }
            ],
        },
    }


def _mapped_step_contract(step_type: str, job_id: str, target: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "fetch_orders",
                "type": "oracle.extract",
                "conn": "oracle_source",
                "source": {"kind": "query", "query": "select id from orders"},
                "output": {"orders_rows": {"kind": "rowset"}},
            },
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "depends_on": ["fetch_orders"],
                "map": {"fetch_orders.orders_rows": target},
            },
        ],
    }


def _write_contract(step_type: str, job_id: str, target: dict) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "fetch_orders",
                "type": "oracle.extract",
                "conn": "oracle_source",
                "source": {"kind": "query", "query": "select id from orders"},
                "output": {"orders_rows": {"kind": "rowset"}},
            },
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "target",
                "depends_on": ["fetch_orders"],
                "map": {"fetch_orders.orders_rows": target},
            },
        ],
    }


def _dbt_contract(step_type: str, job_id: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "models": ["orders"],
            }
        ],
    }


def _sql_contract(step_type: str, job_id: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "sql": "select 1",
            }
        ],
    }


def _uses_unsupported_runtime_callable(executor: object) -> bool:
    if isinstance(executor, RuntimeCallableStepExecutor):
        return executor.runtime_callable == "zeta4s.core:run_unsupported_step"
    nested = getattr(executor, "executors", ())
    return any(_uses_unsupported_runtime_callable(item) for item in nested)


def _spec(source: str) -> dict:
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "SPEC":
            return json.loads(ast.literal_eval(node.value.args[0]))
    raise AssertionError("SPEC assignment not found")


class AirflowDagSourceTest(unittest.TestCase):
    def test_generated_source_compiles_without_zeta4s_import(self) -> None:
        source, dag_id = _render()

        self.assertEqual(dag_id, "retail__daily")
        compile(source, "retail__daily.py", "exec")
        self.assertNotIn("from zeta4s", source)
        self.assertNotIn("import zeta4s", source)
        self.assertIn('"artifact:" + SPEC["artifact_id"]', source)
        self.assertIn("/internal/v1/runtime/steps/execute", source)
        self.assertIn("/internal/v1/runtime/runs/finalize", source)

    def test_schedule_timezone_falls_back_to_project_timezone(self) -> None:
        cases = [
            (None, "Asia/Seoul", None, "Asia/Seoul"),
            (ScheduleConfig(cron="0 2 * * *"), "Asia/Seoul", "0 2 * * *", "Asia/Seoul"),
            (ScheduleConfig(cron="0 2 * * *", timezone="Europe/Berlin"), "Asia/Seoul", "0 2 * * *", "Europe/Berlin"),
            (ScheduleConfig(interval_seconds=300), "America/New_York", "@continuous:300", "America/New_York"),
        ]
        for schedule, project_timezone, expected_schedule, expected_timezone in cases:
            with self.subTest(schedule=schedule, project_timezone=project_timezone):
                spec = _spec(_render(schedule, project_timezone=project_timezone)[0])
                self.assertEqual(spec["schedule"], expected_schedule)
                self.assertEqual(spec["timezone"], expected_timezone)

    def test_generated_dag_start_date_carries_schedule_timezone(self) -> None:
        source, _ = _render(ScheduleConfig(cron="0 2 * * *"))

        self.assertIn('.astimezone(ZoneInfo(SPEC["timezone"]))', source)
        self.assertIn('"start_date": start_date', source)

    def test_generated_dag_runs_one_active_run_without_asset_outlets(self) -> None:
        source, _ = _render()

        self.assertIn("max_active_runs=1", source)
        self.assertIn("catchup=False", source)
        self.assertNotIn("outlets", source)
        self.assertNotIn("Asset", source)
        self.assertNotIn("Dataset", source)


class AirflowDagSourceStepProjectionTest(unittest.TestCase):
    """Generated DAG 의 step task 는 step id 와 scheduler 정책만 싣고 실행은 zeta4s-api 에 위임한다."""

    def test_core_executor_factory_supports_every_builtin_step_type(self) -> None:
        contracts = _core_projection_contracts()

        self.assertEqual(set(contracts), set(supported_builtin_step_types()))
        for step_type, config in contracts.items():
            with self.subTest(step_type=step_type):
                plan = _plan(config)

                executor = built_in_step_executor(
                    project=_project(),
                    plan=plan,
                    plan_step=plan.step_by_id["step_under_test"],
                    runtime_home="/tmp/zeta4s",
                )

                self.assertFalse(_uses_unsupported_runtime_callable(executor))

    def test_every_builtin_step_type_projects_to_one_delegated_task(self) -> None:
        contracts = _core_projection_contracts()

        self.assertEqual(set(contracts), set(supported_builtin_step_types()))
        for step_type, config in contracts.items():
            with self.subTest(step_type=step_type):
                source, spec = _render_job(config)

                plan = _plan(config)
                self.assertEqual([task["step_id"] for task in spec["tasks"]], [step.id for step in plan.steps])
                for task in spec["tasks"]:
                    self.assertEqual(set(task), TASK_SPEC_KEYS)
                self.assertEqual(spec["job_id"], config["job_id"])
                self.assertEqual(spec["artifact_id"], "sha256:abc")
                # step 설정은 DAG source 에 싣지 않는다. zeta4s-api 가 active artifact 에서 읽는다.
                self.assertNotIn('"type"', source)
                self.assertNotIn('"conn"', source)
                self.assertNotIn("from zeta4s", source)
                self.assertIn("python_callable=_run_step", source)

    def test_generated_task_carries_deployment_profile(self) -> None:
        _, spec = _render_job(
            {"job_id": "noop_job", "schedule": None, "steps": [{"step_id": "start", "type": "noop"}]},
            profile_id="prod",
        )

        self.assertEqual(spec["profile_id"], "prod")

    def test_generated_task_uses_execution_plan_default_pool(self) -> None:
        _, spec = _render_job(
            {
                "job_id": "pooled_job",
                "schedule": None,
                "defaults": {"pools": {"transform": "analytics_transform_pool"}},
                "steps": [{"step_id": "select_one", "type": "clickhouse.sql", "conn": "analytics", "sql": "select 1"}],
            }
        )

        self.assertEqual(_task(spec, "select_one")["pool"], "analytics_transform_pool")

    def test_generated_task_uses_project_scoped_pool_without_default_pool(self) -> None:
        _, spec = _render_job(
            {
                "job_id": "auto_pooled_job",
                "schedule": None,
                "steps": [{"step_id": "select_one", "type": "clickhouse.sql", "conn": "analytics", "sql": "select 1"}],
            }
        )

        self.assertEqual(_task(spec, "select_one")["pool"], project_pool_name("retail", "transform"))

    def test_retry_and_timeout_map_to_scheduler_task_policy(self) -> None:
        _, spec = _render_job(
            {
                "job_id": "policy_job",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "work",
                        "type": "noop",
                        "retry": {"max_attempts": 3, "delay_seconds": 0},
                        "timeout": {"seconds": 5},
                    },
                ],
            }
        )

        task = _task(spec, "work")
        self.assertEqual(task["retries"], 2)
        self.assertEqual(task["retry_delay_seconds"], 0)
        self.assertEqual(task["execution_timeout_seconds"], 5)

    def test_when_expr_remains_core_flow_control(self) -> None:
        _, spec = _render_job(
            {
                "job_id": "expr_job",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "count_rows",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select count(*) as row_count from orders",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "publish",
                        "type": "noop",
                        "depends_on": ["count_rows"],
                        "when": {"expr": "$steps.count_rows.outputs.row_count >= 4"},
                    },
                ],
            }
        )

        task = _task(spec, "publish")
        self.assertEqual(task["trigger_rule"], "all_success")
        self.assertEqual(task["upstream_ids"], ["count_rows"])
        self.assertEqual(spec["terminal_step_ids"], ["publish"])


if __name__ == "__main__":
    unittest.main()
