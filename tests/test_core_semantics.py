from __future__ import annotations

import unittest

from zeta4s.core.execution_contract import (
    StepExecutionState,
    StepFailure,
    StepOutputBinding,
    StepResult,
)
from zeta4s.core.semantics import aggregate_run_result, step_skip_reason
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.step_graph import StepGraphJob


def _plan(config: dict):
    return build_step_graph_execution_plan(StepGraphJob.model_validate(config))


def _result(step_id: str, state: StepExecutionState, *, outputs=None) -> StepResult:
    return StepResult(
        step_id=step_id,
        step_type="noop",
        state=state,
        outputs=outputs or {},
        failure=StepFailure("failed") if state == StepExecutionState.FAILED else None,
    )


class CoreSemanticsTest(unittest.TestCase):
    def test_join_rules_are_table_driven(self) -> None:
        cases = (
            ("all_success", (StepExecutionState.SUCCEEDED, StepExecutionState.SUCCEEDED), None),
            ("all_success", (StepExecutionState.SUCCEEDED, StepExecutionState.SKIPPED), "upstream not all_success: b"),
            (
                "none_failed_min_one_success",
                (StepExecutionState.SKIPPED, StepExecutionState.SKIPPED),
                "upstream has no succeeded step",
            ),
            ("none_failed_min_one_success", (StepExecutionState.SUCCEEDED, StepExecutionState.SKIPPED), None),
            (
                "none_failed_min_one_success",
                (StepExecutionState.SUCCEEDED, StepExecutionState.FAILED),
                "upstream failed: b",
            ),
            ("all_done", (StepExecutionState.FAILED, StepExecutionState.SKIPPED), None),
        )
        for join_rule, states, expected in cases:
            with self.subTest(join_rule=join_rule, states=states):
                plan = _plan(
                    {
                        "job_id": "join_job",
                        "steps": [
                            {"step_id": "a", "type": "noop"},
                            {"step_id": "b", "type": "noop"},
                            {
                                "step_id": "target",
                                "type": "noop",
                                "depends_on": ["a", "b"],
                                "join": {"rule": join_rule},
                            },
                        ],
                    }
                )
                results = {step_id: _result(step_id, state) for step_id, state in zip(("a", "b"), states, strict=True)}

                reason = step_skip_reason(
                    plan,
                    plan.step_by_id["target"],
                    results,
                    lambda step_id, output_name: results[step_id].outputs[output_name],
                )

                self.assertEqual(reason, expected)

    def test_when_rules_and_missing_upstream_use_one_eligibility_function(self) -> None:
        plan = _plan(
            {
                "job_id": "when_job",
                "steps": [
                    {
                        "step_id": "source",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 1",
                        "outputs": {"count": {"kind": "scalar", "type": "int"}},
                    },
                    {"step_id": "on_success", "type": "noop", "depends_on": ["source"], "when": {"success": "source"}},
                    {"step_id": "on_failure", "type": "noop", "depends_on": ["source"], "when": {"failed": "source"}},
                    {
                        "step_id": "on_count",
                        "type": "noop",
                        "depends_on": ["source"],
                        "when": {"expr": "$steps.source.outputs.count >= 2"},
                    },
                ],
            }
        )
        succeeded = {"source": _result("source", StepExecutionState.SUCCEEDED, outputs={"count": 1})}

        self.assertIsNone(step_skip_reason(plan, plan.step_by_id["on_success"], succeeded, lambda *_: 1))
        self.assertEqual(
            step_skip_reason(plan, plan.step_by_id["on_failure"], succeeded, lambda *_: 1),
            "when.failed not satisfied: source",
        )
        self.assertEqual(
            step_skip_reason(plan, plan.step_by_id["on_count"], succeeded, lambda *_: 1),
            "when.expr evaluated false: $steps.source.outputs.count >= 2",
        )
        self.assertEqual(
            step_skip_reason(plan, plan.step_by_id["on_success"], {}, lambda *_: 1),
            "upstream result missing: source",
        )

    def test_terminal_aggregation_uses_terminal_step_states_and_bindings(self) -> None:
        plan = _plan(
            {
                "job_id": "terminal_job",
                "steps": [
                    {"step_id": "root", "type": "noop"},
                    {
                        "step_id": "ok",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select 7",
                        "depends_on": ["root"],
                        "outputs": {"value": {"kind": "scalar", "type": "int"}},
                    },
                    {"step_id": "failed", "type": "noop", "depends_on": ["root"]},
                ],
            }
        )
        results = {
            "root": _result("root", StepExecutionState.SUCCEEDED),
            "ok": _result("ok", StepExecutionState.SUCCEEDED, outputs={"value": 7}),
            "failed": _result("failed", StepExecutionState.FAILED),
        }

        result = aggregate_run_result(
            plan,
            results,
            lambda step_id, output_name: StepOutputBinding(
                step_id=step_id,
                output_name=output_name,
                kind="scalar",
                value=results[step_id].outputs[output_name],
            ),
        )

        self.assertEqual(result.state, StepExecutionState.FAILED)
        self.assertEqual(result.terminal_step_ids, ("ok", "failed"))
        self.assertEqual(result.terminal_outputs["ok"]["value"].value, 7)


if __name__ == "__main__":
    unittest.main()
