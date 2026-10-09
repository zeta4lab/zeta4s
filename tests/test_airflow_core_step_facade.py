from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from zeta4s.core import StaticConnectionResolver, StepExecutionState, StepResult
from zeta4s.core import RuntimeCallableStepExecutor, step_output_binding_key
from zeta4s.airflow.operators import run_core_step


class _FailedResultThenSuccessExecutor:
    def __init__(self):
        self.calls = 0

    def execute(self, step, context):
        self.calls += 1
        if self.calls == 1:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.FAILED,
            )
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED)


class _FakeStepOutputBindingRepository:
    def __init__(self):
        self.bindings = []

    def upsert_binding(self, **kwargs):
        self.bindings.append(kwargs)

    def get_binding(self, **kwargs):
        for binding in reversed(self.bindings):
            if all(binding.get(key) == value for key, value in kwargs.items()):
                return binding
        return None


class _FakeStepExecutionRepository:
    def __init__(self):
        self.records = []

    def record_execution(self, **kwargs):
        self.records.append(kwargs)

    def list_executions(self, **kwargs):
        return [
            record
            for record in self.records
            if all(record.get(key) == value for key, value in kwargs.items() if value is not None)
        ]


class _FakeStepEventRepository:
    def __init__(self):
        self.events = []

    def record_event(self, **kwargs):
        self.events.append(kwargs)


class _FakeMetastoreAdapter:
    def __init__(self):
        self.step_execution_repository = _FakeStepExecutionRepository()
        self.step_event_repository = _FakeStepEventRepository()
        self.step_output_binding_repository = _FakeStepOutputBindingRepository()
        self.step_checkpoint_repository = object()


def _project_config() -> dict:
    root = Path("/tmp/core-runner-test")
    return {
        "project_id": "core_runner_test",
        "root": str(root),
        "jobs_dir": str(root / "jobs"),
        "assets_dir": str(root / "assets"),
        "dbt_dir": str(root / "dbt"),
        "timezone": "Asia/Seoul",
    }


def _runtime_success(*, value, **kwargs):
    return {"status": "success", "details": {"outputs": {"value": value}}}


def _runtime_failure(**kwargs):
    return {"status": "failed", "error": {"message": "runtime failed", "type": "RuntimeFailure"}}


def _runtime_capture_connections(**kwargs):
    return {
        "status": "success",
        "details": {
            "outputs": {
                "connections": kwargs["connections"],
                "connection_types": kwargs["connection_types"],
            }
        },
    }


def _runtime_success_without_outputs(**kwargs):
    return {"status": "success", "details": {"target": "mart.stg_orders"}}


def _runtime_wrong_step(**kwargs):
    return StepResult(
        step_id="wrong_step",
        step_type="noop",
        state=StepExecutionState.SUCCEEDED,
    )


class AirflowCoreStepFacadeTest(unittest.TestCase):
    def setUp(self):
        self.rowset_store = object()
        self.rowset_store_patch = patch(
            "zeta4s.runtime.rowset_stores.iceberg.IcebergRowsetStore.from_environment",
            return_value=self.rowset_store,
        )
        self.rowset_store_patch.start()
        self.addCleanup(self.rowset_store_patch.stop)

    def test_airflow_projects_common_checkpoint_context_into_core(self):
        captured = {}
        captured_kwargs = {}
        rowset_store = object()
        adapter = _FakeMetastoreAdapter()

        def runtime_with_context(**kwargs):
            captured_kwargs.update(kwargs)
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(runtime_with_context),
            ),
            patch(
                "zeta4s.runtime.rowset_stores.iceberg.IcebergRowsetStore.from_environment",
                return_value=rowset_store,
            ),
        ):
            run_core_step(
                step_config={"step_id": "checkpointed", "type": "noop"},
                job_config={
                    "job_id": "airflow_checkpoint_job",
                    "steps": [{"step_id": "checkpointed", "type": "noop"}],
                },
                project_config=_project_config(),
                task_id="airflow_checkpointed",
                run_id="run_1",
                try_number=3,
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(captured["attempt"], 3)
        self.assertEqual(captured["adapter_attempt"], 1)
        self.assertIs(captured["rowset_store"], rowset_store)
        self.assertIs(captured["step_checkpoint_repository"], adapter.step_checkpoint_repository)
        self.assertIs(captured_kwargs["rowset_store"], rowset_store)
        self.assertIs(captured_kwargs["step_checkpoint_repository"], adapter.step_checkpoint_repository)

    def _connection_resolver(self, connection: dict | None = None) -> StaticConnectionResolver:
        return StaticConnectionResolver({"analytics": connection or {"type": "clickhouse"}})

    def test_airflow_core_step_facade_returns_runtime_result_payload(self):
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_success, {"value": 11}),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "value_step",
                    "type": "sql.scalar",
                    "conn": "analytics",
                    "sql": "select 11",
                    "outputs": {"value": {"kind": "scalar", "type": "int"}},
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "value_step",
                            "type": "sql.scalar",
                            "conn": "analytics",
                            "sql": "select 11",
                            "outputs": {"value": {"kind": "scalar", "type": "int"}},
                        }
                    ],
                },
                project_config=_project_config(),
                task_id="value_step",
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["details"]["outputs"], {"value": 11})
        self.assertEqual(adapter.step_output_binding_repository.bindings[0]["project_id"], "core_runner_test")
        self.assertEqual(adapter.step_output_binding_repository.bindings[0]["job_id"], "airflow_core_job")
        self.assertEqual(adapter.step_output_binding_repository.bindings[0]["run_id"], "run_1")
        self.assertEqual(adapter.step_output_binding_repository.bindings[0]["step_id"], "value_step")
        self.assertEqual(adapter.step_output_binding_repository.bindings[0]["output_name"], "value")
        self.assertEqual(
            adapter.step_output_binding_repository.bindings[0]["binding"],
            {
                "step_id": "value_step",
                "output_name": "value",
                "kind": "scalar",
                "value": 11,
                "ref": {"kind": "scalar", "type": "int"},
            },
        )
        self.assertEqual(
            [record["status"] for record in adapter.step_execution_repository.records], ["running", "success"]
        )
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["step_started", "step_output_produced", "step_succeeded"],
        )

    def test_airflow_core_step_facade_records_failed_step_lifecycle(self):
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_failure),
            ),
        ):
            with self.assertRaises(RuntimeError):
                run_core_step(
                    step_config={
                        "step_id": "check",
                        "type": "sql.check",
                        "conn": "analytics",
                        "sql": "select 0",
                    },
                    job_config={
                        "job_id": "airflow_core_job",
                        "steps": [
                            {
                                "step_id": "check",
                                "type": "sql.check",
                                "conn": "analytics",
                                "sql": "select 0",
                            }
                        ],
                    },
                    project_config=_project_config(),
                    task_id="check",
                    run_id="run_1",
                    connection_resolver=self._connection_resolver(),
                )

        self.assertEqual(
            [record["status"] for record in adapter.step_execution_repository.records], ["running", "failed"]
        )
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["step_started", "step_failed"],
        )

    def test_airflow_core_step_facade_executes_one_engine_attempt(self):
        adapter = _FakeMetastoreAdapter()
        executor = _FailedResultThenSuccessExecutor()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch("zeta4s.airflow.operators.built_in_step_executor", return_value=executor),
        ):
            with self.assertRaisesRegex(RuntimeError, "step failed"):
                run_core_step(
                    step_config={
                        "step_id": "retry_step",
                        "type": "noop",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                    },
                    job_config={
                        "job_id": "airflow_retry_job",
                        "steps": [
                            {
                                "step_id": "retry_step",
                                "type": "noop",
                                "retry": {"max_attempts": 2, "delay_seconds": 0},
                            }
                        ],
                    },
                    project_config=_project_config(),
                    task_id="retry_step",
                    run_id="run_1",
                    try_number=2,
                    connection_resolver=self._connection_resolver(),
                )

        self.assertEqual(executor.calls, 1)
        self.assertEqual(
            [record["status"] for record in adapter.step_execution_repository.records], ["running", "failed"]
        )
        self.assertEqual([record["attempt"] for record in adapter.step_execution_repository.records], [2, 2])
        self.assertEqual(
            [record["metadata"]["adapter_attempt"] for record in adapter.step_execution_repository.records],
            [1, 1],
        )
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["step_started", "step_failed"],
        )

    def test_airflow_core_step_facade_rejects_wrong_step_result(self):
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_wrong_step),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "wrong step"):
                run_core_step(
                    step_config={"step_id": "expected_step", "type": "noop"},
                    job_config={"job_id": "airflow_core_job", "steps": [{"step_id": "expected_step", "type": "noop"}]},
                    project_config=_project_config(),
                    task_id="expected_step",
                    run_id="run_1",
                    connection_resolver=self._connection_resolver(),
                )

        self.assertEqual(
            [record["status"] for record in adapter.step_execution_repository.records], ["running", "failed"]
        )
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["step_started", "step_failed"],
        )

    def test_airflow_core_step_facade_resolves_step_connections(self):
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_capture_connections, connection_ids=("analytics",)),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "select_one",
                    "type": "clickhouse.sql",
                    "conn": "analytics",
                    "sql": "select 1",
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "select_one",
                            "type": "clickhouse.sql",
                            "conn": "analytics",
                            "sql": "select 1",
                        }
                    ],
                },
                project_config=_project_config(),
                task_id="select_one",
                run_id="run_1",
                connection_resolver=self._connection_resolver({"type": "clickhouse", "host": "clickhouse"}),
            )

        self.assertEqual(
            result["details"]["outputs"]["connections"], {"analytics": {"type": "clickhouse", "host": "clickhouse"}}
        )
        self.assertEqual(result["details"]["outputs"]["connection_types"], {"analytics": "clickhouse"})

    def test_airflow_core_step_facade_records_declared_table_outputs(self):
        adapter = _FakeMetastoreAdapter()
        adapter.step_output_binding_repository.upsert_binding(
            project_id="core_runner_test",
            job_id="airflow_core_job",
            run_id="run_1",
            step_id="fetch_orders",
            output_name="orders_rows",
            output_kind="rowset",
            binding={
                "key": step_output_binding_key("fetch_orders", "orders_rows"),
                "kind": "rowset",
                "value": {"kind": "rowset", "path": "/tmp/orders.parquet"},
                "table_ref": None,
                "ref": {"kind": "rowset"},
            },
        )
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_success_without_outputs),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "stage_orders",
                    "type": "clickhouse.stage",
                    "conn": "analytics",
                    "depends_on": ["fetch_orders"],
                    "map": {"fetch_orders.orders_rows": "mart.stg_orders"},
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "fetch_orders",
                            "type": "oracle.extract",
                            "conn": "orders",
                            "source": {"kind": "table", "table": "orders.raw_orders"},
                            "output": {"orders_rows": {"kind": "rowset"}},
                        },
                        {
                            "step_id": "stage_orders",
                            "type": "clickhouse.stage",
                            "conn": "analytics",
                            "depends_on": ["fetch_orders"],
                            "map": {"fetch_orders.orders_rows": "mart.stg_orders"},
                        },
                    ],
                },
                project_config=_project_config(),
                task_id="stage_orders",
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        stage_binding = next(
            binding
            for binding in adapter.step_output_binding_repository.bindings
            if binding["step_id"] == "stage_orders"
        )
        self.assertEqual(stage_binding["output_name"], "mart.stg_orders")
        self.assertEqual(
            stage_binding["binding"]["value"],
            {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"},
        )

    def test_airflow_core_step_facade_loads_upstream_output_bindings_from_metastore(self):
        captured = {}

        def runtime_with_context(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        adapter = _FakeMetastoreAdapter()
        adapter.step_output_binding_repository.upsert_binding(
            project_id="core_runner_test",
            job_id="airflow_core_job",
            run_id="run_1",
            step_id="fetch_count",
            output_name="count",
            output_kind="scalar",
            binding={
                "key": step_output_binding_key("fetch_count", "count"),
                "kind": "scalar",
                "value": 7,
                "table_ref": None,
                "ref": {},
            },
        )
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(runtime_with_context),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "check_count",
                    "type": "sql.check",
                    "conn": "analytics",
                    "depends_on": ["fetch_count"],
                    "sql": "select 1",
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "fetch_count",
                            "type": "sql.scalar",
                            "conn": "analytics",
                            "sql": "select count(*) from orders",
                            "outputs": {"count": {"kind": "scalar", "type": "int"}},
                        },
                        {
                            "step_id": "check_count",
                            "type": "sql.check",
                            "conn": "analytics",
                            "depends_on": ["fetch_count"],
                            "sql": "select 1",
                        },
                    ],
                },
                project_config=_project_config(),
                task_id="check_count",
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(captured["step_output_bindings"]["fetch_count.count"]["value"], 7)
        self.assertEqual(captured["step_output_bindings"]["fetch_count.count"]["kind"], "scalar")

    def test_airflow_core_step_facade_loads_upstream_success_without_outputs_from_metastore(self):
        adapter = _FakeMetastoreAdapter()
        adapter.step_execution_repository.record_execution(
            project_id="core_runner_test",
            job_id="airflow_core_job",
            run_id="run_1",
            step_id="check_ready",
            step_type="sql.check",
            attempt=1,
            status="success",
            task_id="check_ready",
        )
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "select_after_check",
                    "type": "clickhouse.sql",
                    "conn": "analytics",
                    "depends_on": ["check_ready"],
                    "sql": "select 1",
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "check_ready",
                            "type": "sql.check",
                            "conn": "analytics",
                            "sql": "select 1",
                        },
                        {
                            "step_id": "select_after_check",
                            "type": "clickhouse.sql",
                            "conn": "analytics",
                            "depends_on": ["check_ready"],
                            "sql": "select 1",
                        },
                    ],
                },
                project_config=_project_config(),
                task_id="select_after_check",
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")

    def test_airflow_core_step_facade_uses_latest_upstream_attempt_from_metastore(self):
        adapter = _FakeMetastoreAdapter()
        adapter.step_execution_repository.records.extend(
            [
                {
                    "project_id": "core_runner_test",
                    "job_id": "airflow_core_job",
                    "run_id": "run_1",
                    "step_id": "check_ready",
                    "step_type": "sql.check",
                    "attempt": 2,
                    "revision": 20,
                    "updated_at": "2026-07-10T00:00:20+00:00",
                    "status": "success",
                    "task_id": "check_ready",
                },
                {
                    "project_id": "core_runner_test",
                    "job_id": "airflow_core_job",
                    "run_id": "run_1",
                    "step_id": "check_ready",
                    "step_type": "sql.check",
                    "attempt": 1,
                    "revision": 10,
                    "updated_at": "2026-07-10T00:00:10+00:00",
                    "status": "failed",
                    "task_id": "check_ready",
                },
            ]
        )
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
            ),
        ):
            result = run_core_step(
                step_config={
                    "step_id": "select_after_check",
                    "type": "clickhouse.sql",
                    "conn": "analytics",
                    "depends_on": ["check_ready"],
                    "sql": "select 1",
                },
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "check_ready",
                            "type": "sql.check",
                            "conn": "analytics",
                            "sql": "select 1",
                        },
                        {
                            "step_id": "select_after_check",
                            "type": "clickhouse.sql",
                            "conn": "analytics",
                            "depends_on": ["check_ready"],
                            "sql": "select 1",
                        },
                    ],
                },
                project_config=_project_config(),
                task_id="select_after_check",
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")

    def test_airflow_core_step_facade_projects_core_skip_to_airflow_skip(self):
        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "upstream result missing: check_ready"):
                run_core_step(
                    step_config={
                        "step_id": "select_after_check",
                        "type": "clickhouse.sql",
                        "conn": "analytics",
                        "depends_on": ["check_ready"],
                        "sql": "select 1",
                    },
                    job_config={
                        "job_id": "airflow_core_job",
                        "steps": [
                            {
                                "step_id": "check_ready",
                                "type": "sql.check",
                                "conn": "analytics",
                                "sql": "select 1",
                            },
                            {
                                "step_id": "select_after_check",
                                "type": "clickhouse.sql",
                                "conn": "analytics",
                                "depends_on": ["check_ready"],
                                "sql": "select 1",
                            },
                        ],
                    },
                    project_config=_project_config(),
                    task_id="select_after_check",
                    run_id="run_1",
                    connection_resolver=self._connection_resolver(),
                )

        self.assertEqual([record["status"] for record in adapter.step_execution_repository.records], ["skipped"])
        self.assertEqual(
            [event["event_type"] for event in adapter.step_event_repository.events],
            ["step_skipped"],
        )

    def test_airflow_core_step_facade_prefers_airflow_task_id_in_runtime_context(self):
        captured = {}

        def runtime_with_context(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        adapter = _FakeMetastoreAdapter()
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(runtime_with_context),
            ),
        ):
            result = run_core_step(
                step_config={"step_id": "dbt_step", "type": "dbt.run", "conn": "analytics", "models": ["orders"]},
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [
                        {
                            "step_id": "dbt_step",
                            "type": "dbt.run",
                            "conn": "analytics",
                            "models": ["orders"],
                        }
                    ],
                },
                project_config=_project_config(),
                task=SimpleNamespace(task_id="dbt_step__orders"),
                run_id="run_1",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(captured["task_id"], "dbt_step__orders")
        self.assertEqual(
            [record["task_id"] for record in adapter.step_execution_repository.records],
            ["dbt_step__orders", "dbt_step__orders"],
        )
        self.assertEqual(
            [event["task_id"] for event in adapter.step_event_repository.events],
            ["dbt_step__orders", "dbt_step__orders", "dbt_step__orders"],
        )

    def test_airflow_core_step_facade_normalizes_scheduler_context_before_core_boundary(self):
        captured = {}

        def runtime_with_context(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        adapter = _FakeMetastoreAdapter()
        dag_run = SimpleNamespace(
            dag_id="zeta4s_retail_daily",
            run_id="scheduled__001",
            conf={"z4_run_id": "z4__001"},
        )
        task_instance = SimpleNamespace(task_id="check_orders", try_number=3)
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(runtime_with_context),
            ),
        ):
            result = run_core_step(
                step_config={"step_id": "check_orders", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [{"step_id": "check_orders", "type": "sql.check", "conn": "analytics", "sql": "select 1"}],
                },
                project_config=_project_config(),
                dag_run=dag_run,
                task_instance=task_instance,
                logical_date="2026-07-10T00:00:00+00:00",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(captured["adapter"], "airflow")
        self.assertEqual(captured["dag_id"], "zeta4s_retail_daily")
        self.assertEqual(captured["task_id"], "check_orders")
        self.assertEqual(captured["run_id"], "z4__001")
        self.assertEqual(captured["z4_run_id"], "z4__001")
        self.assertEqual(captured["try_number"], 3)
        self.assertNotIn("dag_run", captured)
        self.assertNotIn("task_instance", captured)
        self.assertNotIn("ti", captured)
        self.assertNotIn("task", captured)

    def test_airflow_core_step_facade_uses_dag_run_profile_for_core_context(self):
        captured = {}

        def runtime_with_context(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        adapter = _FakeMetastoreAdapter()
        dag_run = SimpleNamespace(
            dag_id="zeta4s_retail_daily",
            run_id="scheduled__001",
            conf={"z4_run_id": "z4__001", "profile_id": "prod"},
        )
        with (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=adapter),
            patch(
                "zeta4s.airflow.operators.built_in_step_executor",
                return_value=RuntimeCallableStepExecutor(runtime_with_context),
            ),
        ):
            result = run_core_step(
                step_config={"step_id": "check_orders", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                job_config={
                    "job_id": "airflow_core_job",
                    "steps": [{"step_id": "check_orders", "type": "sql.check", "conn": "analytics", "sql": "select 1"}],
                },
                project_config=_project_config(),
                dag_run=dag_run,
                task_id="check_orders",
                connection_resolver=self._connection_resolver(),
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(captured["profile"], "prod")
        self.assertEqual(adapter.step_execution_repository.records[0]["metadata"]["profile"], "prod")


if __name__ == "__main__":
    unittest.main()
