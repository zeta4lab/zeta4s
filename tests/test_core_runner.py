from __future__ import annotations

import concurrent.futures
from dataclasses import replace
import importlib
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

from zeta4s.core import (
    ExecutionContext,
    LocalRunReporter,
    LocalRunner,
    RuntimeCallableStepExecutor,
    StaticConnectionResolver,
    StepExecutionState,
    StepFailure,
    StepOutputBinding,
    StepResult,
    build_step_execution_input,
    evaluate_step_expression,
    step_output_binding_key,
)
from zeta4s.core.step_executors import built_in_step_executor
from zeta4s.dbt.graph import DbtGraph, DbtNode
from zeta4s.project.execution_plan import ExecutionEdge, build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import StepGraphJob


class _Executor:
    def __init__(self, *, outputs=None, fail: bool = False):
        self.outputs = outputs or {}
        self.fail = fail
        self.calls: list[str] = []

    def execute(self, step, context):
        self.calls.append(step.id)
        if self.fail:
            raise RuntimeError(f"failed {step.id}")
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.SUCCEEDED,
            outputs=dict(self.outputs.get(step.id, {})),
        )


class _Reporter:
    def __init__(self):
        self.events: list[tuple[str, str]] = []

    def run_started(self, context):
        self.events.append(("run_started", context.job_id))

    def run_succeeded(self, context, result):
        self.events.append(("run_succeeded", result.state))

    def run_failed(self, context, result):
        self.events.append(("run_failed", result.state))

    def run_skipped(self, context, result):
        self.events.append(("run_skipped", result.state))

    def step_started(self, context, step):
        self.events.append(("step_started", step.id))

    def step_succeeded(self, context, result):
        self.events.append(("step_succeeded", result.step_id))

    def step_failed(self, context, result):
        self.events.append(("step_failed", result.step_id))

    def step_skipped(self, context, result):
        self.events.append(("step_skipped", result.step_id))

    def step_output_produced(self, context, binding):
        self.events.append(("step_output_produced", binding.key))


class _WrongStepExecutor:
    def execute(self, step, context):
        return StepResult(
            step_id=f"{step.id}_wrong",
            step_type=step.type,
            state=StepExecutionState.SUCCEEDED,
        )


class _FlakyExecutor:
    def __init__(self, failures: int):
        self.failures = failures
        self.calls = 0

    def execute(self, step, context):
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError(f"attempt {self.calls} failed")
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED)


class _SlowExecutor:
    def __init__(self, seconds: float):
        self.seconds = seconds

    def execute(self, step, context):
        time.sleep(self.seconds)
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED)


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
                failure=StepFailure("failed once", type="RuntimeFailure"),
            )
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED)


class _CaptureAttemptRuntime:
    def __init__(self):
        self.attempts: list[int] = []

    def __call__(self, **kwargs):
        self.attempts.append(kwargs["runtime_context"]["attempt"])
        if len(self.attempts) == 1:
            return {"status": "failed", "error": {"message": "failed once", "type": "RuntimeFailure"}}
        return {"status": "success", "details": {"outputs": {"attempt": kwargs["runtime_context"]["attempt"]}}}


class _ReadsUpstreamOutputExecutor:
    def __init__(self, source_step_id: str, source_output_name: str):
        self.source_step_id = source_step_id
        self.source_output_name = source_output_name
        self.seen_binding: StepOutputBinding | None = None

    def execute(self, step, context):
        self.seen_binding = context.step_output_binding(self.source_step_id, self.source_output_name)
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.SUCCEEDED,
            outputs={"copied": context.step_output(self.source_step_id, self.source_output_name)},
        )


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


def _runtime_capture_context(**kwargs):
    return {
        "status": "success",
        "details": {"outputs": {"runtime_context": kwargs["runtime_context"]}},
    }


def _runtime_capture_step_execution(**kwargs):
    return {
        "status": "success",
        "details": {
            "outputs": {
                "step_execution": kwargs["step_execution"].to_runtime_context_payload(),
                "step_execution_payload": kwargs["step_execution_payload"],
                "runtime_context_step_execution": kwargs["runtime_context"]["step_execution"],
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


def _plan(config: dict):
    return build_step_graph_execution_plan(StepGraphJob.model_validate(config))


def _context(job_id: str, reporter=None):
    return ExecutionContext(
        project_id="core_runner_test",
        job_id=job_id,
        run_id=f"{job_id}__test",
        reporter=reporter or _Reporter(),
    )


class CoreRunnerTest(unittest.TestCase):
    def test_core_import_does_not_require_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            module = importlib.import_module("zeta4s.core")
            self.assertTrue(hasattr(module, "LocalRunner"))
        finally:
            sys.modules.update(airflow_modules)

    def test_runs_execution_plan_in_topological_order(self):
        plan = _plan(
            {
                "job_id": "ordered_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                    {"step_id": "second", "type": "noop", "depends_on": ["first"]},
                ],
            }
        )
        executor = _Executor()

        result = LocalRunner({"noop": executor}).run(plan, _context("ordered_job"))

        self.assertEqual(executor.calls, ["first", "second"])
        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)

    def test_tracks_downstream_futures_submitted_after_wait_snapshot(self):
        plan = _plan(
            {
                "job_id": "future_tracking_job",
                "steps": [
                    {"step_id": "root", "type": "noop"},
                    {"step_id": "left", "type": "noop", "depends_on": ["root"]},
                    {"step_id": "right", "type": "noop", "depends_on": ["root"]},
                    {
                        "step_id": "joined",
                        "type": "noop",
                        "depends_on": ["left", "right"],
                    },
                ],
            }
        )
        release_root = threading.Event()
        original_wait = concurrent.futures.wait
        first_wait = True

        def wait_after_snapshot(fs, *, return_when):
            nonlocal first_wait
            if first_wait:
                first_wait = False
                snapshot = set(fs)
                release_root.set()
                return original_wait(snapshot, return_when=return_when)
            return original_wait(fs, return_when=return_when)

        class RootBlockedExecutor(_Executor):
            def execute(self, step, context):
                if step.id == "root" and not release_root.wait(timeout=2):
                    raise RuntimeError("runner did not start waiting for root")
                return super().execute(step, context)

        executor = RootBlockedExecutor()
        with patch("zeta4s.core.runner.concurrent.futures.wait", side_effect=wait_after_snapshot):
            result = LocalRunner({"noop": executor}).run(plan, _context("future_tracking_job"))

        self.assertEqual(
            [step.step_id for step in result.steps],
            ["root", "left", "right", "joined"],
        )
        self.assertCountEqual(executor.calls, ["root", "left", "right", "joined"])
        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)

    def test_runner_rejects_execution_plan_with_unknown_control_edge(self):
        plan = _plan(
            {
                "job_id": "unknown_edge_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                ],
            }
        )
        invalid_plan = replace(
            plan,
            edges=(ExecutionEdge(upstream_id="missing", downstream_id="first", kind="control"),),
        )

        with self.assertRaisesRegex(ValueError, "unknown upstream step: missing -> first"):
            LocalRunner({"noop": _Executor()}).run(invalid_plan, _context("unknown_edge_job"))

    def test_runner_rejects_execution_plan_without_steps(self):
        plan = _plan(
            {
                "job_id": "empty_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                ],
            }
        )
        invalid_plan = replace(plan, steps=(), edges=())

        with self.assertRaisesRegex(ValueError, "execution plan has no step: empty_job"):
            LocalRunner({"noop": _Executor()}).run(invalid_plan, _context("empty_job"))

    def test_runner_rejects_execution_plan_with_duplicated_step_ids(self):
        plan = _plan(
            {
                "job_id": "duplicate_step_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                ],
            }
        )
        invalid_plan = replace(plan, steps=(plan.steps[0], plan.steps[0]), edges=())

        with self.assertRaisesRegex(ValueError, "duplicated step ids: first"):
            LocalRunner({"noop": _Executor()}).run(invalid_plan, _context("duplicate_step_job"))

    def test_runner_rejects_execution_plan_with_control_cycle(self):
        plan = _plan(
            {
                "job_id": "cycle_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                    {"step_id": "second", "type": "noop"},
                ],
            }
        )
        invalid_plan = replace(
            plan,
            edges=(
                ExecutionEdge(upstream_id="first", downstream_id="second", kind="control"),
                ExecutionEdge(upstream_id="second", downstream_id="first", kind="control"),
            ),
        )

        with self.assertRaisesRegex(ValueError, "execution plan cycle"):
            LocalRunner({"noop": _Executor()}).run(invalid_plan, _context("cycle_job"))

    def test_failure_skips_downstream_all_success_step(self):
        plan = _plan(
            {
                "job_id": "failure_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                    {"step_id": "second", "type": "noop", "depends_on": ["first"]},
                ],
            }
        )
        result = LocalRunner({"noop": _Executor(fail=True)}).run(plan, _context("failure_job"))

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["first"].state, StepExecutionState.FAILED)
        self.assertEqual(results["second"].state, StepExecutionState.SKIPPED)
        self.assertEqual(result.state, StepExecutionState.SKIPPED)

    def test_when_failed_runs_after_failed_upstream(self):
        plan = _plan(
            {
                "job_id": "failure_handler_job",
                "steps": [
                    {"step_id": "first", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                    {"step_id": "handler", "type": "noop", "depends_on": ["first"], "when": {"failed": "first"}},
                ],
            }
        )
        noop = _Executor()

        result = LocalRunner({"sql.check": _Executor(fail=True), "noop": noop}).run(
            plan, _context("failure_handler_job")
        )

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["first"].state, StepExecutionState.FAILED)
        self.assertEqual(results["handler"].state, StepExecutionState.SUCCEEDED)
        self.assertEqual(noop.calls, ["handler"])
        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)

    def test_run_step_skips_when_upstream_result_is_missing(self):
        plan = _plan(
            {
                "job_id": "single_step_missing_upstream_job",
                "steps": [
                    {"step_id": "first", "type": "noop"},
                    {"step_id": "second", "type": "noop", "depends_on": ["first"]},
                ],
            }
        )
        executor = _Executor()

        result = LocalRunner({"noop": executor}).run_step(
            plan,
            plan.step_by_id["second"],
            _context("single_step_missing_upstream_job"),
        )

        self.assertEqual(result.state, StepExecutionState.SKIPPED)
        self.assertEqual(result.skipped_reason, "upstream result missing: first")
        self.assertEqual(executor.calls, [])

    def test_run_step_uses_core_when_failed_policy(self):
        plan = _plan(
            {
                "job_id": "single_step_when_failed_job",
                "steps": [
                    {"step_id": "first", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                    {"step_id": "handler", "type": "noop", "depends_on": ["first"], "when": {"failed": "first"}},
                ],
            }
        )
        context = _context("single_step_when_failed_job")
        context.step_results["first"] = StepResult(
            step_id="first",
            step_type="sql.check",
            state=StepExecutionState.FAILED,
            failure=StepFailure("failed", type="RuntimeFailure"),
        )
        executor = _Executor()

        result = LocalRunner({"noop": executor}).run_step(plan, plan.step_by_id["handler"], context)

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(executor.calls, ["handler"])

    def test_when_expr_uses_scalar_output(self):
        plan = _plan(
            {
                "job_id": "expr_job",
                "steps": [
                    {
                        "step_id": "count_rows",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 2",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "next",
                        "type": "noop",
                        "depends_on": ["count_rows"],
                        "when": {"expr": "$steps.count_rows.outputs.row_count >= 2"},
                    },
                ],
            }
        )
        scalar = _Executor(outputs={"count_rows": {"row_count": 2}})
        noop = _Executor()

        result = LocalRunner({"sql.scalar": scalar, "noop": noop}).run(plan, _context("expr_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(noop.calls, ["next"])

    def test_when_expr_step_skips_when_upstream_failed_before_reading_scalar_output(self):
        plan = _plan(
            {
                "job_id": "expr_after_failure_job",
                "steps": [
                    {
                        "step_id": "count_rows",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 2",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "next",
                        "type": "noop",
                        "depends_on": ["count_rows"],
                        "when": {"expr": "$steps.count_rows.outputs.row_count >= 2"},
                    },
                ],
            }
        )
        noop = _Executor()

        result = LocalRunner({"sql.scalar": _Executor(fail=True), "noop": noop}).run(
            plan,
            _context("expr_after_failure_job"),
        )

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["count_rows"].state, StepExecutionState.FAILED)
        self.assertEqual(results["next"].state, StepExecutionState.SKIPPED)
        self.assertEqual(results["next"].skipped_reason, "upstream not all_success: count_rows")
        self.assertEqual(noop.calls, [])
        self.assertEqual(result.state, StepExecutionState.SKIPPED)

    def test_evaluate_step_expression_uses_supplied_output_resolver(self):
        seen: list[tuple[str, str]] = []

        def resolve(step_id: str, output_name: str):
            seen.append((step_id, output_name))
            return 12

        self.assertTrue(evaluate_step_expression("$steps.count_rows.outputs.row_count >= 10", resolve))
        self.assertEqual(seen, [("count_rows", "row_count")])

    def test_none_failed_min_one_success_join_skips_when_no_success(self):
        plan = _plan(
            {
                "job_id": "join_job",
                "steps": [
                    {
                        "step_id": "first",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select false",
                        "outputs": {"flag": {"kind": "scalar", "type": "bool"}},
                    },
                    {
                        "step_id": "second",
                        "type": "noop",
                        "depends_on": ["first"],
                        "when": {"expr": "$steps.first.outputs.flag == true"},
                    },
                    {
                        "step_id": "joined",
                        "type": "noop",
                        "depends_on": ["second"],
                        "join": {"rule": "none_failed_min_one_success"},
                    },
                ],
            }
        )
        first = _Executor(outputs={"first": {"flag": False}})

        result = LocalRunner({"sql.scalar": first, "noop": _Executor()}).run(plan, _context("join_job"))

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(first.calls, ["first"])
        self.assertEqual(results["second"].state, StepExecutionState.SKIPPED)
        self.assertEqual(results["joined"].state, StepExecutionState.SKIPPED)

    def test_all_done_join_runs_after_failed_upstream(self):
        plan = _plan(
            {
                "job_id": "all_done_job",
                "steps": [
                    {"step_id": "first", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                    {
                        "step_id": "cleanup",
                        "type": "noop",
                        "depends_on": ["first"],
                        "join": {"rule": "all_done"},
                    },
                ],
            }
        )
        cleanup = _Executor()

        result = LocalRunner({"sql.check": _Executor(fail=True), "noop": cleanup}).run(plan, _context("all_done_job"))

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["first"].state, StepExecutionState.FAILED)
        self.assertEqual(results["cleanup"].state, StepExecutionState.SUCCEEDED)
        self.assertEqual(cleanup.calls, ["cleanup"])

    def test_reporter_event_order(self):
        plan = _plan({"job_id": "report_job", "steps": [{"step_id": "first", "type": "noop"}]})
        reporter = _Reporter()

        LocalRunner({"noop": _Executor()}).run(plan, _context("report_job", reporter))

        self.assertEqual(
            reporter.events,
            [
                ("run_started", "report_job"),
                ("step_started", "first"),
                ("step_succeeded", "first"),
                ("run_succeeded", StepExecutionState.SUCCEEDED),
            ],
        )

    def test_reporter_records_step_output_produced_before_step_succeeded(self):
        plan = _plan(
            {
                "job_id": "output_report_job",
                "steps": [
                    {
                        "step_id": "count_rows",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 3",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )
        reporter = _Reporter()

        LocalRunner({"sql.scalar": _Executor(outputs={"count_rows": {"row_count": 3}})}).run(
            plan, _context("output_report_job", reporter)
        )

        self.assertEqual(
            reporter.events,
            [
                ("run_started", "output_report_job"),
                ("step_started", "count_rows"),
                ("step_output_produced", step_output_binding_key("count_rows", "row_count")),
                ("step_succeeded", "count_rows"),
                ("run_succeeded", StepExecutionState.SUCCEEDED),
            ],
        )

    def test_reporter_records_failed_and_skipped_step_events(self):
        plan = _plan(
            {
                "job_id": "report_failure_job",
                "steps": [
                    {"step_id": "first", "type": "sql.check", "conn": "analytics", "sql": "select 1"},
                    {"step_id": "second", "type": "noop", "depends_on": ["first"]},
                ],
            }
        )
        reporter = _Reporter()

        LocalRunner({"sql.check": _Executor(fail=True), "noop": _Executor()}).run(
            plan, _context("report_failure_job", reporter)
        )

        self.assertEqual(
            reporter.events,
            [
                ("run_started", "report_failure_job"),
                ("step_started", "first"),
                ("step_failed", "first"),
                ("step_skipped", "second"),
                ("run_skipped", StepExecutionState.SKIPPED),
            ],
        )

    def test_artifact_store_holds_canonical_step_output_binding_for_downstream(self):
        plan = _plan(
            {
                "job_id": "artifact_handoff_job",
                "steps": [
                    {
                        "step_id": "extract_orders",
                        "type": "oracle.extract",
                        "conn": "orders",
                        "source": {"kind": "table", "table": "orders.raw_orders"},
                        "output": {"orders_rows": {"kind": "rowset"}},
                    },
                    {
                        "step_id": "consume_orders",
                        "type": "noop",
                        "depends_on": ["extract_orders"],
                    },
                ],
            }
        )
        reader = _ReadsUpstreamOutputExecutor("extract_orders", "orders_rows")
        context = _context("artifact_handoff_job")

        result = LocalRunner(
            {
                "oracle.extract": _Executor(outputs={"extract_orders": {"orders_rows": "rows.parquet"}}),
                "consume_orders": reader,
            }
        ).run(plan, context)

        binding = context.artifact_store.get(step_output_binding_key("extract_orders", "orders_rows"))
        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertIsInstance(binding, StepOutputBinding)
        self.assertEqual(binding.kind, "rowset")
        self.assertEqual(binding.ref, {"kind": "rowset"})
        self.assertEqual(binding.value, "rows.parquet")
        self.assertEqual(reader.seen_binding, binding)

    def test_run_result_collects_succeeded_terminal_outputs(self):
        plan = _plan(
            {
                "job_id": "terminal_output_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 4",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )

        result = LocalRunner({"sql.scalar": _Executor(outputs={"count_orders": {"row_count": 4}})}).run(
            plan,
            _context("terminal_output_job"),
        )

        binding = result.terminal_outputs["count_orders"]["row_count"]
        self.assertIsInstance(binding, StepOutputBinding)
        self.assertEqual(binding.kind, "scalar")
        self.assertEqual(binding.value, 4)
        self.assertEqual(binding.ref, {"kind": "scalar", "type": "int"})

    def test_executor_result_step_mismatch_fails_step(self):
        plan = _plan({"job_id": "mismatch_job", "steps": [{"step_id": "first", "type": "noop"}]})

        result = LocalRunner({"noop": _WrongStepExecutor()}).run(plan, _context("mismatch_job"))

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[0].state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[0].failure.type, "StepResultMismatch")

    def test_declared_step_ignores_undeclared_canonical_output_binding(self):
        plan = _plan(
            {
                "job_id": "undeclared_output_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )

        result = LocalRunner(
            {"sql.scalar": _Executor(outputs={"count_orders": {"row_count": 1, "debug": "not-contract"}})}
        ).run(plan, _context("undeclared_output_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(result.steps[0].outputs["debug"], "not-contract")
        self.assertNotIn("debug", result.terminal_outputs["count_orders"])

    def test_declared_step_requires_non_table_outputs(self):
        plan = _plan(
            {
                "job_id": "missing_output_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )

        result = LocalRunner({"sql.scalar": _Executor(outputs={"count_orders": {}})}).run(
            plan,
            _context("missing_output_job"),
        )

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[0].failure.type, "StepOutputContractMissing")

    def test_undeclared_step_outputs_do_not_become_canonical_bindings(self):
        plan = _plan({"job_id": "summary_output_job", "steps": [{"step_id": "inspect", "type": "noop"}]})
        context = _context("summary_output_job")

        result = LocalRunner({"noop": _Executor(outputs={"inspect": {"summary": "adapter-only"}})}).run(
            plan,
            context,
        )

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(result.steps[0].outputs, {"summary": "adapter-only"})
        self.assertEqual(result.terminal_outputs, {})
        with self.assertRaises(KeyError):
            context.artifact_store.get(step_output_binding_key("inspect", "summary"))

    def test_runtime_callable_step_executor_normalizes_success_payload(self):
        plan = _plan(
            {
                "job_id": "runtime_success_job",
                "steps": [
                    {
                        "step_id": "value_step",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 7",
                        "outputs": {"value": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )
        executor = RuntimeCallableStepExecutor(_runtime_success, {"value": 7})

        result = LocalRunner({"sql.scalar": executor}).run(plan, _context("runtime_success_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(result.steps[0].outputs, {"value": 7})
        self.assertEqual(result.steps[0].raw_result["details"]["outputs"], {"value": 7})

    def test_runtime_callable_step_executor_resolves_dotted_path(self):
        plan = _plan(
            {
                "job_id": "runtime_path_job",
                "steps": [
                    {
                        "step_id": "value_step",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 9",
                        "outputs": {"value": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )
        executor = RuntimeCallableStepExecutor("tests.test_core_runner:_runtime_success", {"value": 9})

        result = LocalRunner({"sql.scalar": executor}).run(plan, _context("runtime_path_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(result.steps[0].outputs, {"value": 9})

    def test_runtime_callable_step_executor_normalizes_failure_payload(self):
        plan = _plan(
            {
                "job_id": "runtime_failure_job",
                "steps": [{"step_id": "check", "type": "sql.check", "conn": "analytics", "sql": "select 0"}],
            }
        )

        result = LocalRunner({"sql.check": RuntimeCallableStepExecutor(_runtime_failure)}).run(
            plan, _context("runtime_failure_job")
        )

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[0].failure.message, "runtime failed")
        self.assertEqual(result.steps[0].failure.type, "RuntimeFailure")

    def test_runtime_callable_step_executor_resolves_declared_connections_from_context(self):
        plan = _plan(
            {
                "job_id": "connection_job",
                "steps": [
                    {
                        "step_id": "select_one",
                        "type": "clickhouse.sql",
                        "conn": "analytics",
                        "sql": "select 1",
                    }
                ],
            }
        )
        context = _context("connection_job")
        context.connection_resolver = StaticConnectionResolver(
            {"analytics": {"type": "clickhouse", "host": "clickhouse"}}
        )

        result = LocalRunner(
            {
                "clickhouse.sql": RuntimeCallableStepExecutor(
                    _runtime_capture_connections,
                    connection_ids=("analytics",),
                )
            }
        ).run(plan, context)

        outputs = result.steps[0].outputs
        self.assertEqual(outputs["connections"], {"analytics": {"type": "clickhouse", "host": "clickhouse"}})
        self.assertEqual(outputs["connection_types"], {"analytics": "clickhouse"})

    def test_runtime_callable_step_executor_synthesizes_declared_table_outputs(self):
        plan = _plan(
            {
                "job_id": "stage_output_job",
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
            }
        )

        context = _context("stage_output_job")
        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": RuntimeCallableStepExecutor(_runtime_success_without_outputs),
            }
        ).run(plan, context)

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(
            result.steps[1].outputs,
            {"mart.stg_orders": {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"}},
        )
        binding = context.step_output_binding("stage_orders", "mart.stg_orders")
        self.assertEqual(binding.kind, "table")
        self.assertEqual(binding.table_ref.conn, "analytics")
        self.assertEqual(binding.table_ref.table, "mart.stg_orders")

    def test_runner_synthesizes_declared_table_outputs_for_any_executor(self):
        plan = _plan(
            {
                "job_id": "stage_direct_executor_output_job",
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
            }
        )
        context = _context("stage_direct_executor_output_job")

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": _Executor(outputs={"stage_orders": {}}),
            }
        ).run(plan, context)

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(
            result.steps[1].outputs,
            {"mart.stg_orders": {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"}},
        )
        binding = context.step_output_binding("stage_orders", "mart.stg_orders")
        self.assertEqual(binding.kind, "table")
        self.assertEqual(binding.table_ref.table, "mart.stg_orders")

    def test_synthesized_table_outputs_are_available_to_downstream_runtime_context(self):
        plan = _plan(
            {
                "job_id": "stage_downstream_binding_job",
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
                    {
                        "step_id": "inspect_orders",
                        "type": "noop",
                        "depends_on": ["stage_orders"],
                    },
                ],
            }
        )

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": RuntimeCallableStepExecutor(_runtime_success_without_outputs),
                "inspect_orders": RuntimeCallableStepExecutor(_runtime_capture_context),
            }
        ).run(plan, _context("stage_downstream_binding_job"))

        bindings = result.steps[2].outputs["runtime_context"]["step_output_bindings"]
        self.assertEqual(bindings["stage_orders.mart.stg_orders"]["kind"], "table")
        self.assertEqual(bindings["stage_orders.mart.stg_orders"]["value"]["conn"], "analytics")
        self.assertEqual(bindings["stage_orders.mart.stg_orders"]["table_ref"]["table"], "mart.stg_orders")

    def test_runtime_callable_step_executor_does_not_synthesize_outputs_for_failed_step(self):
        plan = _plan(
            {
                "job_id": "stage_failure_output_job",
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
            }
        )

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": RuntimeCallableStepExecutor(_runtime_failure),
            }
        ).run(plan, _context("stage_failure_output_job"))

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[1].outputs, {})

    def test_runtime_callable_step_executor_passes_core_output_bindings_to_runtime_context(self):
        plan = _plan(
            {
                "job_id": "runtime_output_binding_job",
                "steps": [
                    {
                        "step_id": "fetch_orders",
                        "type": "oracle.extract",
                        "conn": "orders",
                        "source": {"kind": "table", "table": "orders.raw_orders"},
                        "output": {"orders_rows": {"kind": "rowset"}},
                    },
                    {"step_id": "stage_orders", "type": "noop", "depends_on": ["fetch_orders"]},
                ],
            }
        )

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={
                        "fetch_orders": {
                            "orders_rows": {
                                "kind": "rowset",
                                "path": "/tmp/orders.parquet",
                                "rows": 1,
                            }
                        }
                    }
                ),
                "stage_orders": RuntimeCallableStepExecutor(_runtime_capture_context),
            }
        ).run(plan, _context("runtime_output_binding_job"))

        bindings = result.steps[1].outputs["runtime_context"]["step_output_bindings"]
        self.assertEqual(bindings["fetch_orders.orders_rows"]["kind"], "rowset")
        self.assertEqual(bindings["fetch_orders.orders_rows"]["value"]["path"], "/tmp/orders.parquet")
        self.assertEqual(bindings["fetch_orders.orders_rows"]["ref"], {"kind": "rowset"})

    def test_runtime_callable_step_executor_passes_canonical_step_execution_input(self):
        plan = _plan(
            {
                "job_id": "step_execution_input_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 5",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "check_orders",
                        "type": "sql.check",
                        "conn": "analytics",
                        "depends_on": ["count_orders"],
                        "when": {"expr": "$steps.count_orders.outputs.row_count >= 1"},
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                        "timeout": {"seconds": 10},
                        "params": {"threshold": 1},
                        "sql": "select 1",
                    },
                ],
            }
        )

        result = LocalRunner(
            {
                "sql.scalar": _Executor(outputs={"count_orders": {"row_count": 5}}),
                "sql.check": RuntimeCallableStepExecutor(_runtime_capture_context),
            }
        ).run(plan, _context("step_execution_input_job"))

        step_execution = result.steps[1].outputs["runtime_context"]["step_execution"]
        self.assertEqual(step_execution["step_id"], "check_orders")
        self.assertEqual(step_execution["step_type"], "sql.check")
        self.assertEqual(step_execution["params"], {"threshold": 1})
        self.assertEqual(step_execution["flow"]["depends_on"], ["count_orders"])
        self.assertEqual(step_execution["flow"]["join_rule"], "all_success")
        self.assertEqual(step_execution["flow"]["retry"], {"max_attempts": 2, "delay_seconds": 0})
        self.assertEqual(step_execution["flow"]["timeout"], {"seconds": 10})
        self.assertEqual(step_execution["runtime"], {"conn_id": "analytics", "pool": None})
        self.assertEqual(step_execution["outputs"], [])
        self.assertEqual(
            step_execution["upstream_output_bindings"]["count_orders.row_count"],
            {
                "step_id": "count_orders",
                "output_name": "row_count",
                "kind": "scalar",
                "value": 5,
                "ref": {"kind": "scalar", "type": "int"},
            },
        )

    def test_runtime_callable_step_executor_passes_step_execution_as_explicit_kwargs(self):
        plan = _plan(
            {
                "job_id": "step_execution_kwarg_job",
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
            }
        )

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": RuntimeCallableStepExecutor(_runtime_capture_step_execution),
            }
        ).run(plan, _context("step_execution_kwarg_job"))

        outputs = result.steps[1].outputs
        self.assertEqual(outputs["step_execution"], outputs["step_execution_payload"])
        self.assertEqual(outputs["step_execution_payload"], outputs["runtime_context_step_execution"])
        self.assertEqual(outputs["step_execution"]["step_id"], "stage_orders")
        self.assertEqual(
            outputs["step_execution"]["resolved_data_bindings"][0]["output"]["value"]["path"],
            "/tmp/orders.parquet",
        )

    def test_step_execution_input_only_includes_relevant_upstream_bindings(self):
        plan = _plan(
            {
                "job_id": "step_execution_upstream_filter_job",
                "steps": [
                    {
                        "step_id": "unrelated_count",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 99",
                        "outputs": {"count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "related_count",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "outputs": {"count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "check_count",
                        "type": "sql.check",
                        "conn": "analytics",
                        "depends_on": ["related_count"],
                        "sql": "select 1",
                    },
                ],
            }
        )

        result = LocalRunner(
            {
                "sql.scalar": _Executor(
                    outputs={
                        "unrelated_count": {"count": 99},
                        "related_count": {"count": 1},
                    }
                ),
                "sql.check": RuntimeCallableStepExecutor(_runtime_capture_context),
            }
        ).run(plan, _context("step_execution_upstream_filter_job"))

        bindings = result.steps[2].outputs["runtime_context"]["step_execution"]["upstream_output_bindings"]
        self.assertEqual(list(bindings), ["related_count.count"])
        self.assertEqual(bindings["related_count.count"]["value"], 1)

    def test_build_step_execution_input_exposes_step_output_contracts(self):
        plan = _plan(
            {
                "job_id": "step_execution_output_contract_job",
                "steps": [
                    {
                        "step_id": "stage_orders",
                        "type": "clickhouse.stage",
                        "conn": "analytics",
                        "depends_on": ["fetch_orders"],
                        "map": {"fetch_orders.orders_rows": "mart.stg_orders"},
                    },
                    {
                        "step_id": "fetch_orders",
                        "type": "oracle.extract",
                        "conn": "orders",
                        "source": {"kind": "table", "table": "orders.raw_orders"},
                        "output": {"orders_rows": {"kind": "rowset"}},
                    },
                ],
            }
        )
        context = _context("step_execution_output_contract_job")

        step_input = build_step_execution_input(plan.step_by_id["stage_orders"], context)
        payload = step_input.to_runtime_context_payload()

        self.assertEqual(
            payload["outputs"],
            [
                {
                    "name": "mart.stg_orders",
                    "kind": "table",
                    "ref": {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"},
                    "table_ref": {"conn": "analytics", "table": "mart.stg_orders"},
                }
            ],
        )
        self.assertEqual(
            payload["data_bindings"],
            [
                {
                    "downstream_id": "stage_orders",
                    "source": {"step_id": "fetch_orders", "output_name": "orders_rows"},
                    "target": {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"},
                    "field": "map",
                    "required_kind": "rowset",
                }
            ],
        )

    def test_step_execution_input_resolves_data_bindings_from_core_artifact_store(self):
        plan = _plan(
            {
                "job_id": "step_execution_data_binding_job",
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
            }
        )

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "rowset", "path": "/tmp/orders.parquet"}}}
                ),
                "clickhouse.stage": RuntimeCallableStepExecutor(_runtime_capture_context),
            }
        ).run(plan, _context("step_execution_data_binding_job"))

        step_execution = result.steps[1].outputs["runtime_context"]["step_execution"]
        self.assertEqual(
            step_execution["resolved_data_bindings"],
            [
                {
                    "downstream_id": "stage_orders",
                    "source": {"step_id": "fetch_orders", "output_name": "orders_rows"},
                    "target": {"kind": "table", "conn": "analytics", "table": "mart.stg_orders"},
                    "field": "map",
                    "required_kind": "rowset",
                    "output": {
                        "step_id": "fetch_orders",
                        "output_name": "orders_rows",
                        "kind": "rowset",
                        "value": {"kind": "rowset", "path": "/tmp/orders.parquet"},
                        "ref": {"kind": "rowset"},
                    },
                }
            ],
        )

    def test_data_binding_missing_upstream_output_fails_downstream_step(self):
        plan = _plan(
            {
                "job_id": "missing_data_binding_job",
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
            }
        )
        stage_executor = _Executor()

        result = LocalRunner(
            {
                "oracle.extract": _Executor(outputs={"fetch_orders": {}}),
                "clickhouse.stage": stage_executor,
            }
        ).run(plan, _context("missing_data_binding_job"))

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["fetch_orders"].state, StepExecutionState.FAILED)
        self.assertEqual(results["fetch_orders"].failure.type, "StepOutputContractMissing")
        self.assertEqual(results["stage_orders"].state, StepExecutionState.SKIPPED)
        self.assertEqual(stage_executor.calls, [])
        self.assertEqual(result.state, StepExecutionState.SKIPPED)

    def test_data_binding_value_kind_mismatch_fails_downstream_step(self):
        plan = _plan(
            {
                "job_id": "mismatched_data_binding_job",
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
            }
        )
        stage_executor = _Executor()

        result = LocalRunner(
            {
                "oracle.extract": _Executor(
                    outputs={"fetch_orders": {"orders_rows": {"kind": "table", "table": "raw.orders"}}}
                ),
                "clickhouse.stage": stage_executor,
            }
        ).run(plan, _context("mismatched_data_binding_job"))

        results = {step.step_id: step for step in result.steps}
        self.assertEqual(results["stage_orders"].state, StepExecutionState.FAILED)
        self.assertEqual(results["stage_orders"].failure.type, "StepDataBindingKindMismatch")
        self.assertEqual(stage_executor.calls, [])
        self.assertEqual(result.state, StepExecutionState.FAILED)

    def test_dbt_step_executor_passes_core_output_bindings_to_node_runtime_context(self):
        plan = _plan(
            {
                "job_id": "dbt_output_binding_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "build_orders",
                        "type": "dbt.run",
                        "conn": "analytics",
                        "depends_on": ["count_orders"],
                        "models": ["orders"],
                    },
                ],
            }
        )
        project = ProjectContext(
            project_id="core_runner_test",
            root=Path("/tmp/core_runner_test"),
            jobs_dir=Path("/tmp/core_runner_test/jobs"),
            dbt_dir=Path("/tmp/core_runner_test/dbt"),
            timezone="UTC",
        )
        captured: dict[str, object] = {}

        def run_dbt_node(**kwargs):
            captured.update(kwargs["runtime_context"])
            return {"status": "success", "details": {"outputs": {"orders": "built"}}}

        dbt_graph = DbtGraph(
            nodes=(
                DbtNode(
                    unique_id="model.analytics.orders",
                    name="orders",
                    resource_type="model",
                    original_file_path="models/orders.sql",
                    depends_on=(),
                ),
            )
        )
        dbt_executor = built_in_step_executor(
            project=project,
            plan=plan,
            plan_step=plan.step_by_id["build_orders"],
            runtime_home="/tmp/zeta4s",
        )

        with (
            patch("zeta4s.core.step_executors.load_dbt_graph", return_value=dbt_graph),
            patch("zeta4s.runtime.dbt.run_dbt_node", side_effect=run_dbt_node),
        ):
            context = _context("dbt_output_binding_job")
            context.connection_resolver = StaticConnectionResolver({"analytics": {"type": "clickhouse"}})
            result = LocalRunner(
                {
                    "sql.scalar": _Executor(outputs={"count_orders": {"row_count": 7}}),
                    "build_orders": dbt_executor,
                }
            ).run(plan, context)

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(captured["task_id"], "build_orders.orders")
        self.assertEqual(captured["dbt_target_id"], "build_orders")
        self.assertIsNone(captured["profile"])
        bindings = captured["step_output_bindings"]
        self.assertEqual(bindings["count_orders.row_count"]["kind"], "scalar")
        self.assertEqual(bindings["count_orders.row_count"]["value"], 7)

    def test_runtime_context_core_keys_override_user_params(self):
        plan = _plan({"job_id": "runtime_context_key_job", "steps": [{"step_id": "run", "type": "noop"}]})
        context = _context("runtime_context_key_job")
        context.params.update(
            {
                "job_id": "wrong",
                "run_id": "wrong",
                "task_id": "wrong",
                "profile": "wrong",
                "step_output_bindings": {"wrong": {}},
            }
        )

        result = LocalRunner({"noop": RuntimeCallableStepExecutor(_runtime_capture_context)}).run(plan, context)

        runtime_context = result.steps[0].outputs["runtime_context"]
        self.assertEqual(runtime_context["job_id"], "runtime_context_key_job")
        self.assertEqual(runtime_context["run_id"], "runtime_context_key_job__test")
        self.assertEqual(runtime_context["task_id"], "run")
        self.assertEqual(runtime_context["attempt"], 1)
        self.assertIsNone(runtime_context["profile"])
        self.assertEqual(runtime_context["step_output_bindings"], {})

    def test_retry_policy_retries_failed_step_until_success(self):
        plan = _plan(
            {
                "job_id": "retry_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 3, "delay_seconds": 0},
                    }
                ],
            }
        )
        executor = _FlakyExecutor(failures=2)

        result = LocalRunner({"noop": executor}).run(plan, _context("retry_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(executor.calls, 3)

    def test_retry_policy_retries_failed_result_payload(self):
        plan = _plan(
            {
                "job_id": "retry_payload_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                    }
                ],
            }
        )
        executor = _FailedResultThenSuccessExecutor()

        result = LocalRunner({"noop": executor}).run(plan, _context("retry_payload_job"))

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(executor.calls, 2)

    def test_retry_policy_reports_intermediate_failed_attempt(self):
        plan = _plan(
            {
                "job_id": "retry_event_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                    }
                ],
            }
        )
        reporter = _Reporter()

        result = LocalRunner({"noop": _FailedResultThenSuccessExecutor()}).run(
            plan,
            _context("retry_event_job", reporter),
        )

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(
            reporter.events,
            [
                ("run_started", "retry_event_job"),
                ("step_started", "flaky"),
                ("step_failed", "flaky"),
                ("step_started", "flaky"),
                ("step_succeeded", "flaky"),
                ("run_succeeded", StepExecutionState.SUCCEEDED),
            ],
        )

    def test_retry_runtime_context_exposes_current_attempt(self):
        plan = _plan(
            {
                "job_id": "retry_context_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                        "outputs": {"attempt": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )
        runtime = _CaptureAttemptRuntime()

        result = LocalRunner({"sql.scalar": RuntimeCallableStepExecutor(runtime)}).run(
            plan,
            _context("retry_context_job"),
        )

        self.assertEqual(result.state, StepExecutionState.SUCCEEDED)
        self.assertEqual(runtime.attempts, [1, 2])
        self.assertEqual(result.steps[0].outputs["attempt"], 2)

    def test_local_reporter_records_attempt_in_step_events(self):
        plan = _plan(
            {
                "job_id": "local_attempt_report_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                    }
                ],
            }
        )
        reporter = LocalRunReporter()

        LocalRunner({"noop": _FailedResultThenSuccessExecutor()}).run(
            plan,
            ExecutionContext(
                project_id="core_runner_test",
                job_id="local_attempt_report_job",
                run_id="local_attempt_report_job__test",
                reporter=reporter,
            ),
        )

        step_started = [event for event in reporter.events if event["event_type"] == "step_started"]
        step_finished = [event for event in reporter.events if event["event_type"] in {"step_failed", "step_succeeded"}]
        self.assertEqual([event["event"]["attempt"] for event in step_started], [1, 2])
        self.assertEqual([event["event"]["attempt"] for event in step_finished], [1, 2])

    def test_local_reporter_records_terminal_outputs_in_run_result_event(self):
        plan = _plan(
            {
                "job_id": "local_terminal_output_report_job",
                "steps": [
                    {
                        "step_id": "count_orders",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 8",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    }
                ],
            }
        )
        reporter = LocalRunReporter()

        LocalRunner({"sql.scalar": _Executor(outputs={"count_orders": {"row_count": 8}})}).run(
            plan,
            ExecutionContext(
                project_id="core_runner_test",
                job_id="local_terminal_output_report_job",
                run_id="local_terminal_output_report_job__test",
                reporter=reporter,
            ),
        )

        run_succeeded = reporter.events[-1]
        self.assertEqual(run_succeeded["event_type"], "run_succeeded")
        self.assertEqual(
            run_succeeded["event"]["terminal_outputs"],
            {
                "count_orders": {
                    "row_count": {
                        "step_id": "count_orders",
                        "output_name": "row_count",
                        "kind": "scalar",
                        "value": 8,
                        "ref": {"kind": "scalar", "type": "int"},
                    }
                }
            },
        )

    def test_metastore_reporter_fallback_attempt_does_not_reset_on_finish(self):
        from zeta4s.runtime.metastore_reporter import MetastoreRunReporter

        plan = _plan({"job_id": "metastore_attempt_job", "steps": [{"step_id": "flaky", "type": "noop"}]})
        context = ExecutionContext(
            project_id="core_runner_test",
            job_id="metastore_attempt_job",
            run_id="metastore_attempt_job__test",
        )
        reporter = MetastoreRunReporter(adapter=_FakeMetastoreAdapter())
        step = plan.step_by_id["flaky"]
        result = StepResult(step_id="flaky", step_type="noop", state=StepExecutionState.FAILED)

        reporter.step_started(context, step)
        reporter.step_failed(context, result)
        reporter.step_started(context, step)
        reporter.step_failed(context, result)

        records = reporter.adapter.step_execution_repository.records
        self.assertEqual([record["attempt"] for record in records], [1, 1, 2, 2])
        self.assertEqual([record["status"] for record in records], ["running", "failed", "running", "failed"])

    def test_retry_policy_fails_after_max_attempts(self):
        plan = _plan(
            {
                "job_id": "retry_fail_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 2, "delay_seconds": 0},
                    }
                ],
            }
        )
        executor = _FlakyExecutor(failures=3)

        result = LocalRunner({"noop": executor}).run(plan, _context("retry_fail_job"))

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(executor.calls, 2)
        self.assertEqual(result.steps[0].failure.type, "RuntimeError")

    def test_projected_run_step_executes_exactly_once(self):
        plan = _plan(
            {
                "job_id": "projected_retry_job",
                "steps": [
                    {
                        "step_id": "flaky",
                        "type": "noop",
                        "retry": {"max_attempts": 3, "delay_seconds": 0},
                        "timeout": {"seconds": 1},
                    }
                ],
            }
        )
        executor = _FlakyExecutor(failures=3)

        result = LocalRunner({"noop": executor}).run_step(
            plan,
            plan.step_by_id["flaky"],
            _context("projected_retry_job"),
        )

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(executor.calls, 1)

    def test_timeout_policy_fails_slow_step(self):
        plan = _plan(
            {
                "job_id": "timeout_job",
                "steps": [
                    {
                        "step_id": "slow",
                        "type": "noop",
                        "timeout": {"seconds": 1},
                    }
                ],
            }
        )

        result = LocalRunner({"noop": _SlowExecutor(seconds=2)}).run(plan, _context("timeout_job"))

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.steps[0].failure.type, "StepTimeoutError")

    def test_timeout_policy_has_same_deadline_in_main_and_worker_threads(self):
        plan = _plan(
            {
                "job_id": "thread_timeout_job",
                "steps": [
                    {
                        "step_id": "slow",
                        "type": "noop",
                        "timeout": {"seconds": 1},
                    }
                ],
            }
        )

        def run_once():
            started = time.monotonic()
            result = LocalRunner({"noop": _SlowExecutor(seconds=1.1)}).run(
                plan,
                _context("thread_timeout_job"),
            )
            return result, time.monotonic() - started

        main_result, main_elapsed = run_once()
        worker_values = []
        worker = threading.Thread(target=lambda: worker_values.append(run_once()))
        worker.start()
        worker.join(timeout=3)

        self.assertFalse(worker.is_alive())
        worker_result, worker_elapsed = worker_values[0]
        self.assertEqual(main_result.steps[0].failure.type, "StepTimeoutError")
        self.assertEqual(worker_result.steps[0].failure.type, "StepTimeoutError")
        self.assertGreaterEqual(main_elapsed, 1.05)
        self.assertGreaterEqual(worker_elapsed, 1.05)
        self.assertLess(main_elapsed, 1.5)
        self.assertLess(worker_elapsed, 1.5)


if __name__ == "__main__":
    unittest.main()
