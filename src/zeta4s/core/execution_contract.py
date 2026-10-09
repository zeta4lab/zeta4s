"""Core step execution contract independent from scheduler adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from zeta4s.project.execution_plan import (
    FlowControl,
    RuntimeBinding,
    StepDataBinding,
    StepInput,
    StepOutput,
    TableRef,
)


class StepExecutionState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class StepFailure:
    message: str
    type: str = "StepExecutionError"


@dataclass(frozen=True)
class StepResult:
    step_id: str
    step_type: str
    state: StepExecutionState
    outputs: dict[str, Any] = field(default_factory=dict)
    failure: StepFailure | None = None
    skipped_reason: str | None = None
    raw_result: Any | None = None

    @property
    def succeeded(self) -> bool:
        return self.state == StepExecutionState.SUCCEEDED

    @property
    def failed(self) -> bool:
        return self.state == StepExecutionState.FAILED

    @property
    def skipped(self) -> bool:
        return self.state == StepExecutionState.SKIPPED


@dataclass(frozen=True)
class StepOutputBinding:
    step_id: str
    output_name: str
    kind: str
    value: Any
    table_ref: TableRef | None = None
    ref: dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return step_output_binding_key(self.step_id, self.output_name)


@dataclass(frozen=True)
class ResolvedStepDataBinding:
    binding: StepDataBinding
    output: StepOutputBinding


@dataclass(frozen=True)
class StepExecutionInput:
    step_id: str
    step_type: str
    params: dict[str, Any]
    inputs: tuple[StepInput, ...]
    outputs: tuple[StepOutput, ...]
    data_bindings: tuple[StepDataBinding, ...]
    flow: FlowControl
    runtime: RuntimeBinding
    upstream_output_bindings: dict[str, StepOutputBinding] = field(default_factory=dict)
    resolved_data_bindings: tuple[ResolvedStepDataBinding, ...] = ()

    def to_runtime_context_payload(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "step_type": self.step_type,
            "params": json_safe(self.params),
            "inputs": [_step_input_payload(step_input) for step_input in self.inputs],
            "outputs": [_step_output_payload(output) for output in self.outputs],
            "data_bindings": [_step_data_binding_payload(binding) for binding in self.data_bindings],
            "flow": _flow_control_payload(self.flow),
            "runtime": _runtime_binding_payload(self.runtime),
            "upstream_output_bindings": {
                key: step_output_binding_payload(binding) for key, binding in self.upstream_output_bindings.items()
            },
            "resolved_data_bindings": [
                _resolved_step_data_binding_payload(binding) for binding in self.resolved_data_bindings
            ],
        }


@dataclass(frozen=True)
class RunResult:
    job_id: str
    state: StepExecutionState
    steps: tuple[StepResult, ...]
    terminal_step_ids: tuple[str, ...]
    terminal_outputs: dict[str, dict[str, StepOutputBinding]] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.state == StepExecutionState.SUCCEEDED


def step_output_binding_key(step_id: str, output_name: str) -> str:
    return f"step-output/{step_id}/{output_name}"


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if hasattr(value, "model_dump"):
        return json_safe(value.model_dump())
    if hasattr(value, "__dict__"):
        return json_safe(vars(value))
    return str(value)


def step_output_binding_payload(binding: StepOutputBinding) -> dict[str, Any]:
    payload = {
        "step_id": binding.step_id,
        "output_name": binding.output_name,
        "kind": binding.kind,
        "value": json_safe(binding.value),
        "ref": json_safe(dict(binding.ref)),
    }
    if binding.table_ref is not None:
        payload["table_ref"] = _table_ref_payload(binding.table_ref)
    return payload


def _step_data_binding_payload(binding: StepDataBinding) -> dict[str, Any]:
    return {
        "downstream_id": binding.downstream_id,
        "source": {
            "step_id": binding.source.step_id,
            "output_name": binding.source.output_name,
        },
        "target": json_safe(dict(binding.target)),
        "field": binding.field,
        "required_kind": binding.required_kind,
    }


def _resolved_step_data_binding_payload(binding: ResolvedStepDataBinding) -> dict[str, Any]:
    return {
        **_step_data_binding_payload(binding.binding),
        "output": step_output_binding_payload(binding.output),
    }


def _step_input_payload(step_input: StepInput) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": step_input.name,
        "kind": step_input.kind,
        "ref": json_safe(dict(step_input.ref)),
    }
    if step_input.table_ref is not None:
        payload["table_ref"] = _table_ref_payload(step_input.table_ref)
    return payload


def _step_output_payload(output: StepOutput) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": output.name,
        "kind": output.kind,
        "ref": json_safe(dict(output.ref)),
    }
    if output.table_ref is not None:
        payload["table_ref"] = _table_ref_payload(output.table_ref)
    return payload


def _table_ref_payload(table_ref: TableRef) -> dict[str, str]:
    return {
        "conn": table_ref.conn,
        "table": table_ref.table,
    }


def _flow_control_payload(flow: FlowControl) -> dict[str, Any]:
    return {
        "depends_on": list(flow.depends_on),
        "when": json_safe(flow.when),
        "join_rule": flow.join_rule,
        "retry": json_safe(flow.retry),
        "timeout": json_safe(flow.timeout),
    }


def _runtime_binding_payload(runtime: RuntimeBinding) -> dict[str, Any]:
    return {
        "conn_id": runtime.conn_id,
        "pool": runtime.pool,
    }
