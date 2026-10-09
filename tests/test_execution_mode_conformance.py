from __future__ import annotations

import unittest

from zeta4s.core import (
    ExecutionContext,
    LocalRunner,
    StepExecutionState,
    StepResult,
    aggregate_run_result,
)
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.step_graph import StepGraphJob


class _Executor:
    def execute(self, step, context):
        outputs = {"count": 3} if step.id == "count" else {}
        return StepResult(step.id, step.type, StepExecutionState.SUCCEEDED, outputs=outputs)


class _CheckpointedExecutor:
    def __init__(self):
        self.completed_sequences = []
        self.downstream_calls = 0

    def execute(self, step, context):
        if step.id == "extract":
            latest = max(self.completed_sequences, default=0)
            for sequence in range(latest + 1, 4):
                self.completed_sequences.append(sequence)
                if sequence == 2 and context.step_attempt(step.id) == 1:
                    return StepResult(
                        step.id,
                        step.type,
                        StepExecutionState.FAILED,
                    )
            return StepResult(
                step.id,
                step.type,
                StepExecutionState.SUCCEEDED,
                outputs={"rows": {"snapshot_id": 3}},
            )
        self.downstream_calls += 1
        return StepResult(step.id, step.type, StepExecutionState.SUCCEEDED)


class ExecutionModeConformanceTest(unittest.TestCase):
    def test_scheduler_modes_resume_step_without_exposing_partial_binding(self) -> None:
        plan = build_step_graph_execution_plan(
            StepGraphJob.model_validate(
                {
                    "job_id": "checkpoint_conformance",
                    "steps": [
                        {
                            "step_id": "extract",
                            "type": "clickhouse.extract",
                            "conn": "source",
                            "source": {"kind": "table", "table": "raw.events"},
                            "output": {"rows": {"kind": "rowset"}},
                        },
                        {"step_id": "publish", "type": "noop", "depends_on": ["extract"]},
                    ],
                }
            )
        )

        for adapter in ("airflow", "internal"):
            with self.subTest(adapter=adapter):
                executor = _CheckpointedExecutor()
                context = ExecutionContext("p", plan.job_id, f"{adapter}-run", params={"adapter": adapter})
                runner = LocalRunner({"clickhouse.extract": executor, "noop": executor})

                context.start_step_attempt("extract", 1)
                failed = runner.run_step(plan, plan.step_by_id["extract"], context)
                self.assertEqual(failed.state, StepExecutionState.FAILED)
                with self.assertRaises(KeyError):
                    context.step_output_binding("extract", "rows")

                context.start_step_attempt("extract", 2)
                resumed = runner.run_step(plan, plan.step_by_id["extract"], context)
                runner.run_step(plan, plan.step_by_id["publish"], context)

                self.assertEqual(resumed.state, StepExecutionState.SUCCEEDED)
                self.assertEqual(executor.completed_sequences, [1, 2, 3])
                self.assertEqual(executor.downstream_calls, 1)
                self.assertEqual(
                    context.step_output_binding("extract", "rows").value,
                    {"snapshot_id": 3},
                )

    def test_verification_and_projected_modes_share_semantics_and_terminal_result(self) -> None:
        plan = build_step_graph_execution_plan(
            StepGraphJob.model_validate(
                {
                    "job_id": "conformance",
                    "steps": [
                        {
                            "step_id": "count",
                            "type": "sql.scalar",
                            "conn": "analytics",
                            "sql": "select 3",
                            "outputs": {"count": {"kind": "scalar", "type": "int"}},
                        },
                        {
                            "step_id": "publish",
                            "type": "noop",
                            "depends_on": ["count"],
                            "when": {"expr": "$steps.count.outputs.count >= 2"},
                        },
                    ],
                }
            )
        )
        verification_context = ExecutionContext("p", plan.job_id, "verification")
        projected_context = ExecutionContext("p", plan.job_id, "projected")

        verification = LocalRunner({"sql.scalar": _Executor(), "noop": _Executor()}).run(
            plan,
            verification_context,
        )
        projected_runner = LocalRunner({"sql.scalar": _Executor(), "noop": _Executor()})
        for step in plan.steps:
            projected_runner.run_step(plan, step, projected_context)
        projected = aggregate_run_result(
            plan,
            projected_context.step_results,
            projected_context.step_output_binding,
        )

        self.assertEqual(projected.state, verification.state)
        self.assertEqual(
            [(result.step_id, result.state) for result in projected.steps],
            [(result.step_id, result.state) for result in verification.steps],
        )
        self.assertEqual(projected.terminal_outputs, verification.terminal_outputs)


if __name__ == "__main__":
    unittest.main()
