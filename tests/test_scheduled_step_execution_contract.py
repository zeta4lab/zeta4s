"""`/internal/v1/runtime/steps/execute` 가 위임하는 scheduled step 실행 계약.

Airflow generated DAG 와 Prefect worker 는 step 하나를 zeta4s-api 에 맡긴다. API 는
`run_scheduled_step` 으로 core runner 에 step 한 번을 실행시키고 결과를 metastore 에 남긴다.
"""

from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

from zeta4s.core import RuntimeCallableStepExecutor, StepExecutionState, StepResult, step_output_binding_key
from zeta4s.prefect.runtime import run_scheduled_step
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import validate_step_graph_config

PROJECT_ID = "core_runner_test"
JOB_ID = "scheduled_core_job"
RUN_ID = "run_1"
PROFILE = {"connections": {"analytics": {"type": "clickhouse", "host": "clickhouse"}}}


class _FailedResultThenSuccessExecutor:
    def __init__(self):
        self.calls = 0

    def execute(self, step, context):
        self.calls += 1
        if self.calls == 1:
            return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.FAILED)
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


def _project() -> ProjectContext:
    root = Path("/tmp/core-runner-test")
    return ProjectContext(
        project_id=PROJECT_ID,
        root=root,
        jobs_dir=root / "jobs",
        dbt_dir=root / "dbt",
        timezone="Asia/Seoul",
    )


def _plan(steps: list[dict]):
    return build_step_graph_execution_plan(
        validate_step_graph_config(Path("job.yml"), {"job_id": JOB_ID, "steps": steps})
    )


def _runtime_success(*, value, **kwargs):
    return {"status": "success", "details": {"outputs": {"value": value}}}


def _runtime_failure(**kwargs):
    return {"status": "failed", "error": {"message": "runtime failed", "type": "RuntimeFailure"}}


def _runtime_capture_connections(**kwargs):
    return {
        "status": "success",
        "details": {"outputs": {"connections": kwargs["connections"], "connection_types": kwargs["connection_types"]}},
    }


def _runtime_success_without_outputs(**kwargs):
    return {"status": "success", "details": {"target": "mart.stg_orders"}}


def _runtime_wrong_step(**kwargs):
    return StepResult(step_id="wrong_step", step_type="noop", state=StepExecutionState.SUCCEEDED)


CHECK_READY = {"step_id": "check_ready", "type": "sql.check", "conn": "analytics", "sql": "select 1"}
SELECT_AFTER_CHECK = {
    "step_id": "select_after_check",
    "type": "clickhouse.sql",
    "conn": "analytics",
    "depends_on": ["check_ready"],
    "sql": "select 1",
}


class ScheduledStepExecutionContractTest(unittest.TestCase):
    def setUp(self):
        self.adapter = _FakeMetastoreAdapter()
        self.rowset_store = object()
        for patcher in (
            patch("zeta4s.runtime.metastore_reporter.metastore_adapter_factory", return_value=self.adapter),
            patch(
                "zeta4s.runtime.rowset_stores.iceberg.IcebergRowsetStore.from_environment",
                return_value=self.rowset_store,
            ),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _run(self, steps: list[dict], step_id: str, executor, **kwargs):
        with patch("zeta4s.prefect.runtime.built_in_step_executor", return_value=executor):
            return run_scheduled_step(
                project=_project(),
                plan=_plan(steps),
                step_id=step_id,
                run_id=RUN_ID,
                profile=kwargs.pop("profile", "dev"),
                profile_data=kwargs.pop("profile_data", PROFILE),
                **kwargs,
            )

    def _execution_statuses(self) -> list[str]:
        return [record["status"] for record in self.adapter.step_execution_repository.records]

    def _event_types(self) -> list[str]:
        return [event["event_type"] for event in self.adapter.step_event_repository.events]

    def test_projects_scheduler_context_into_core_runtime_context(self):
        captured = {}
        captured_kwargs = {}

        def runtime_with_context(**kwargs):
            captured_kwargs.update(kwargs)
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        result = self._run(
            [{"step_id": "checkpointed", "type": "noop"}],
            "checkpointed",
            RuntimeCallableStepExecutor(runtime_with_context),
            attempt=3,
            adapter="airflow",
            parameters={"window_start": "2026-07-10"},
            profile="prod",
        )

        self.assertEqual(result["status"], StepExecutionState.SUCCEEDED.value)
        self.assertEqual(captured["attempt"], 3)
        self.assertEqual(captured["adapter_attempt"], 1)
        self.assertEqual(captured["adapter"], "airflow")
        self.assertEqual(captured["task_id"], "checkpointed")
        self.assertEqual(captured["window_start"], "2026-07-10")
        self.assertEqual(captured["profile"], "prod")
        self.assertIs(captured["rowset_store"], self.rowset_store)
        self.assertIs(captured["step_checkpoint_repository"], self.adapter.step_checkpoint_repository)
        self.assertIs(captured_kwargs["rowset_store"], self.rowset_store)
        self.assertIs(captured_kwargs["step_checkpoint_repository"], self.adapter.step_checkpoint_repository)
        self.assertEqual(self.adapter.step_execution_repository.records[0]["metadata"]["profile"], "prod")

    def test_records_step_outputs_and_lifecycle(self):
        step = {
            "step_id": "value_step",
            "type": "sql.scalar",
            "conn": "analytics",
            "sql": "select 11",
            "outputs": {"value": {"kind": "scalar", "type": "int"}},
        }

        result = self._run([step], "value_step", RuntimeCallableStepExecutor(_runtime_success, {"value": 11}))

        self.assertEqual(result["details"]["outputs"], {"value": 11})
        binding = self.adapter.step_output_binding_repository.bindings[0]
        self.assertEqual(
            {key: binding[key] for key in ("project_id", "job_id", "run_id", "step_id", "output_name")},
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "run_id": RUN_ID,
                "step_id": "value_step",
                "output_name": "value",
            },
        )
        self.assertEqual(
            binding["binding"],
            {
                "step_id": "value_step",
                "output_name": "value",
                "kind": "scalar",
                "value": 11,
                "ref": {"kind": "scalar", "type": "int"},
            },
        )
        self.assertEqual(self._execution_statuses(), ["running", "success"])
        self.assertEqual(self._event_types(), ["step_started", "step_output_produced", "step_succeeded"])

    def test_failed_step_raises_and_records_failed_lifecycle(self):
        with self.assertRaises(RuntimeError):
            self._run([CHECK_READY], "check_ready", RuntimeCallableStepExecutor(_runtime_failure))

        self.assertEqual(self._execution_statuses(), ["running", "failed"])
        self.assertEqual(self._event_types(), ["step_started", "step_failed"])

    def test_executes_one_engine_attempt_per_scheduler_attempt(self):
        executor = _FailedResultThenSuccessExecutor()

        with self.assertRaisesRegex(RuntimeError, "step failed"):
            self._run(
                [{"step_id": "retry_step", "type": "noop", "retry": {"max_attempts": 2, "delay_seconds": 0}}],
                "retry_step",
                executor,
                attempt=2,
            )

        self.assertEqual(executor.calls, 1)
        self.assertEqual(self._execution_statuses(), ["running", "failed"])
        records = self.adapter.step_execution_repository.records
        self.assertEqual([record["attempt"] for record in records], [2, 2])
        self.assertEqual([record["metadata"]["adapter_attempt"] for record in records], [1, 1])
        self.assertEqual(self._event_types(), ["step_started", "step_failed"])

    def test_rejects_wrong_step_result(self):
        with self.assertRaisesRegex(RuntimeError, "wrong step"):
            self._run(
                [{"step_id": "expected_step", "type": "noop"}],
                "expected_step",
                RuntimeCallableStepExecutor(_runtime_wrong_step),
            )

        self.assertEqual(self._execution_statuses(), ["running", "failed"])
        self.assertEqual(self._event_types(), ["step_started", "step_failed"])

    def test_resolves_step_connections_from_profile(self):
        result = self._run(
            [{"step_id": "select_one", "type": "clickhouse.sql", "conn": "analytics", "sql": "select 1"}],
            "select_one",
            RuntimeCallableStepExecutor(_runtime_capture_connections, connection_ids=("analytics",)),
        )

        outputs = result["details"]["outputs"]
        self.assertEqual(outputs["connection_types"], {"analytics": "clickhouse"})
        self.assertEqual(set(outputs["connections"]), {"analytics"})
        self.assertEqual(outputs["connections"]["analytics"].host, "clickhouse")

    def test_records_declared_table_outputs(self):
        self.adapter.step_output_binding_repository.upsert_binding(
            project_id=PROJECT_ID,
            job_id=JOB_ID,
            run_id=RUN_ID,
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
        self.adapter.step_execution_repository.record_execution(
            project_id=PROJECT_ID,
            job_id=JOB_ID,
            run_id=RUN_ID,
            step_id="fetch_orders",
            attempt=1,
            status="success",
        )
        steps = [
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
        ]

        result = self._run(steps, "stage_orders", RuntimeCallableStepExecutor(_runtime_success_without_outputs))

        self.assertEqual(result["status"], StepExecutionState.SUCCEEDED.value)
        stage_binding = next(
            binding
            for binding in self.adapter.step_output_binding_repository.bindings
            if binding["step_id"] == "stage_orders"
        )
        self.assertEqual(stage_binding["output_name"], "mart.stg_orders")
        self.assertEqual(
            stage_binding["binding"]["value"],
            {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"},
        )

    def test_loads_upstream_output_bindings_from_metastore(self):
        captured = {}

        def runtime_with_context(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {}}}

        self.adapter.step_output_binding_repository.upsert_binding(
            project_id=PROJECT_ID,
            job_id=JOB_ID,
            run_id=RUN_ID,
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
        self.adapter.step_execution_repository.record_execution(
            project_id=PROJECT_ID,
            job_id=JOB_ID,
            run_id=RUN_ID,
            step_id="fetch_count",
            attempt=1,
            status="success",
        )
        steps = [
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
        ]

        result = self._run(steps, "check_count", RuntimeCallableStepExecutor(runtime_with_context))

        self.assertEqual(result["status"], StepExecutionState.SUCCEEDED.value)
        self.assertEqual(captured["step_output_bindings"]["fetch_count.count"]["value"], 7)
        self.assertEqual(captured["step_output_bindings"]["fetch_count.count"]["kind"], "scalar")

    def test_loads_upstream_success_without_outputs_from_metastore(self):
        self.adapter.step_execution_repository.record_execution(
            project_id=PROJECT_ID,
            job_id=JOB_ID,
            run_id=RUN_ID,
            step_id="check_ready",
            step_type="sql.check",
            attempt=1,
            status="success",
            task_id="check_ready",
        )

        result = self._run(
            [CHECK_READY, SELECT_AFTER_CHECK],
            "select_after_check",
            RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
        )

        self.assertEqual(result["status"], StepExecutionState.SUCCEEDED.value)

    def test_uses_latest_upstream_attempt_from_metastore(self):
        common = {"project_id": PROJECT_ID, "job_id": JOB_ID, "run_id": RUN_ID, "step_id": "check_ready"}
        self.adapter.step_execution_repository.records.extend(
            [
                {
                    **common,
                    "attempt": 2,
                    "revision": 20,
                    "updated_at": "2026-07-10T00:00:20+00:00",
                    "status": "success",
                },
                {**common, "attempt": 1, "revision": 10, "updated_at": "2026-07-10T00:00:10+00:00", "status": "failed"},
            ]
        )

        result = self._run(
            [CHECK_READY, SELECT_AFTER_CHECK],
            "select_after_check",
            RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
        )

        self.assertEqual(result["status"], StepExecutionState.SUCCEEDED.value)

    def test_missing_upstream_result_skips_step(self):
        result = self._run(
            [CHECK_READY, SELECT_AFTER_CHECK],
            "select_after_check",
            RuntimeCallableStepExecutor(_runtime_success, {"value": 1}),
        )

        self.assertEqual(result["status"], StepExecutionState.SKIPPED.value)
        self.assertTrue(result.get("reason"))
        self.assertEqual(self._execution_statuses(), ["skipped"])
        self.assertEqual(self._event_types(), ["step_skipped"])


if __name__ == "__main__":
    unittest.main()
