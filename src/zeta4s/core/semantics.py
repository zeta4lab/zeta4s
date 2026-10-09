"""Pure flow-control semantics shared by every execution mode."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from zeta4s.core.execution_contract import (
    RunResult,
    StepExecutionState,
    StepOutputBinding,
    StepResult,
)
from zeta4s.project.execution_plan import ExecutionPlan, ExecutionStep
from zeta4s.project.step_graph import STEP_EXPR_RE


StepOutputResolver = Callable[[str, str], Any]
StepOutputBindingResolver = Callable[[str, str], StepOutputBinding]


def step_skip_reason(
    plan: ExecutionPlan,
    step: ExecutionStep,
    step_results: Mapping[str, StepResult],
    output_resolver: StepOutputResolver,
) -> str | None:
    """Return the canonical reason that a step is ineligible, or ``None``."""
    upstream_results = {
        upstream_id: step_results.get(upstream_id) for upstream_id in plan.upstream_ids_by_step[step.id]
    }
    missing = [step_id for step_id, result in upstream_results.items() if result is None]
    if missing:
        return "upstream result missing: " + ", ".join(missing)

    when = step.flow.when or {}
    if when.get("success") and not _step_has_state(
        step_results,
        str(when["success"]),
        StepExecutionState.SUCCEEDED,
    ):
        return f"when.success not satisfied: {when['success']}"
    if when.get("failed") and not _step_has_state(
        step_results,
        str(when["failed"]),
        StepExecutionState.FAILED,
    ):
        return f"when.failed not satisfied: {when['failed']}"

    join_reason = join_skip_reason(
        step,
        tuple(result for result in upstream_results.values() if result is not None),
    )
    if join_reason:
        return join_reason
    if when.get("expr") and not evaluate_step_expression(str(when["expr"]), output_resolver):
        return f"when.expr evaluated false: {when['expr']}"
    return None


def join_skip_reason(step: ExecutionStep, upstream_results: tuple[StepResult, ...]) -> str | None:
    """Apply the canonical ``join.rule`` semantics to completed upstreams."""
    if not upstream_results:
        return None
    if step.flow.when and step.flow.when.get("failed"):
        return None
    rule = step.flow.join_rule
    if rule == "all_success":
        blockers = [result.step_id for result in upstream_results if not result.succeeded]
        return "upstream not all_success: " + ", ".join(blockers) if blockers else None
    if rule == "none_failed_min_one_success":
        failed = [result.step_id for result in upstream_results if result.failed]
        if failed:
            return "upstream failed: " + ", ".join(failed)
        if not any(result.succeeded for result in upstream_results):
            return "upstream has no succeeded step"
        return None
    if rule == "all_done":
        return None
    raise ValueError(f"unsupported join.rule: {rule}")


def evaluate_step_expression(expr: str, output_resolver: StepOutputResolver) -> bool:
    """Evaluate the restricted canonical ``when.expr`` grammar."""
    match = STEP_EXPR_RE.match(expr)
    if not match:
        raise ValueError(f"unsupported step graph when.expr: {expr}")
    step_id, output_name, operator, literal = match.groups()
    left = output_resolver(step_id, output_name)
    right = _expr_literal(literal)
    return _compare_values(left, operator, right)


def aggregate_run_result(
    plan: ExecutionPlan,
    step_results: Mapping[str, StepResult],
    output_binding_resolver: StepOutputBindingResolver,
) -> RunResult:
    """Aggregate canonical terminal state and outputs from completed step results."""
    ordered_results = tuple(step_results[step.id] for step in plan.steps)
    terminal_results = tuple(step_results[step_id] for step_id in plan.terminal_step_ids)
    state = StepExecutionState.SUCCEEDED
    if any(result.failed for result in terminal_results):
        state = StepExecutionState.FAILED
    elif any(result.skipped for result in terminal_results):
        state = StepExecutionState.SKIPPED
    return RunResult(
        job_id=plan.job_id,
        state=state,
        steps=ordered_results,
        terminal_step_ids=plan.terminal_step_ids,
        terminal_outputs=_terminal_outputs(
            plan,
            step_results,
            output_binding_resolver,
        ),
    )


def _step_has_state(
    step_results: Mapping[str, StepResult],
    step_id: str,
    state: StepExecutionState,
) -> bool:
    result = step_results.get(step_id)
    return bool(result and result.state == state)


def _expr_literal(raw: str) -> object:
    value = raw.strip()
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _compare_values(left: Any, operator: str, right: Any) -> bool:
    if operator == "==":
        return left == right
    if operator == "!=":
        return left != right
    if operator == ">":
        return left > right
    if operator == ">=":
        return left >= right
    if operator == "<":
        return left < right
    if operator == "<=":
        return left <= right
    raise ValueError(f"unsupported step graph expression operator: {operator}")


def _terminal_outputs(
    plan: ExecutionPlan,
    step_results: Mapping[str, StepResult],
    output_binding_resolver: StepOutputBindingResolver,
) -> dict[str, dict[str, StepOutputBinding]]:
    outputs: dict[str, dict[str, StepOutputBinding]] = {}
    for step_id in plan.terminal_step_ids:
        step = plan.step_by_id[step_id]
        result = step_results.get(step_id)
        if result is None or not result.succeeded:
            continue
        step_outputs: dict[str, StepOutputBinding] = {}
        for output in step.outputs:
            try:
                step_outputs[output.name] = output_binding_resolver(step_id, output.name)
            except KeyError:
                continue
        if step_outputs:
            outputs[step_id] = step_outputs
    return outputs
