"""Metastore-backed runtime reporter."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from zeta4s.core import (
    ExecutionContext,
    RunResult,
    StepOutputBinding,
    StepResult,
    step_output_binding_payload,
)
from zeta4s.project.execution_plan import ExecutionStep
from zeta4s.metastore.factory import metastore_adapter_factory


class MetastoreRunReporter:
    def __init__(self, adapter: Any | None = None) -> None:
        self._attempts: dict[str, int] = {}
        self._started_at_by_step: dict[str, str] = {}
        self._adapter = adapter or metastore_adapter_factory()

    @property
    def adapter(self) -> Any:
        return self._adapter

    def run_started(self, context: ExecutionContext) -> None:
        created_at = _utc_now_iso()
        try:
            self._adapter.run_metadata_repository.create_run(
                {
                    "run_id": context.run_id,
                    "project_id": context.project_id,
                    "job_id": context.job_id,
                    "profile": context.profile,
                    "scheduler": context.params.get("scheduler"),
                    "scheduler_run_id": context.params.get("scheduler_run_id") or context.run_id,
                    "parameters": dict(context.params.get("parameters") or {}),
                    "status": "running",
                    "state": "running",
                    "created_at": created_at,
                }
            )
        except ValueError:
            pass
        _record_event(
            context,
            step_id="__run__",
            task_id="__run__",
            event_type="run_started",
            status="running",
            event={"run_id": context.run_id, "started_at": created_at},
            created_at=created_at,
            adapter=self._adapter,
        )

    def run_succeeded(self, context: ExecutionContext, result: RunResult) -> None:
        self._record_run_result(context, result, event_type="run_succeeded")

    def run_failed(self, context: ExecutionContext, result: RunResult) -> None:
        self._record_run_result(context, result, event_type="run_failed")

    def run_skipped(self, context: ExecutionContext, result: RunResult) -> None:
        self._record_run_result(context, result, event_type="run_skipped")

    def _record_run_result(self, context: ExecutionContext, result: RunResult, *, event_type: str) -> None:
        status = result.state.value
        finished_at = _utc_now_iso()
        event = {
            "run_id": context.run_id,
            "status": status,
            "state": status,
            "finished_at": finished_at,
            "terminal_step_ids": list(result.terminal_step_ids),
            "terminal_outputs": _terminal_output_payload(result),
            "step_count": len(result.steps),
        }
        self._adapter.run_metadata_repository.update_run(context.run_id, event)
        _record_event(
            context,
            step_id="__run__",
            task_id="__run__",
            event_type=event_type,
            status=status,
            event=event,
            created_at=finished_at,
            adapter=self._adapter,
        )

    def step_started(self, context: ExecutionContext, step: ExecutionStep) -> None:
        attempt = self._context_attempt(context, step.id)
        started_at = _utc_now_iso()
        task_id = _task_id(context, step.id)
        self._started_at_by_step[step.id] = started_at
        self._adapter.step_execution_repository.record_execution(
            project_id=context.project_id,
            job_id=context.job_id,
            run_id=context.run_id,
            step_id=step.id,
            step_type=step.type,
            attempt=attempt,
            status="running",
            task_id=task_id,
            started_at=started_at,
            metadata=_step_metadata(context, step, attempt=attempt, task_id=task_id),
        )
        _record_event(
            context,
            step_id=step.id,
            task_id=task_id,
            event_type="step_started",
            status="running",
            event={
                "step_type": step.type,
                "attempt": attempt,
                "adapter_attempt": context.adapter_attempt(step.id),
            },
            created_at=started_at,
            adapter=self._adapter,
        )

    def step_succeeded(self, context: ExecutionContext, result: StepResult) -> None:
        self._record_step_finished(context, result, status="success", event_type="step_succeeded")

    def step_failed(self, context: ExecutionContext, result: StepResult) -> None:
        self._record_step_finished(context, result, status="failed", event_type="step_failed")

    def step_skipped(self, context: ExecutionContext, result: StepResult) -> None:
        self._record_step_finished(context, result, status="skipped", event_type="step_skipped")

    def step_output_produced(self, context: ExecutionContext, binding: StepOutputBinding) -> None:
        created_at = _utc_now_iso()
        payload = step_output_binding_payload(binding)
        self._adapter.step_output_binding_repository.upsert_binding(
            project_id=context.project_id,
            job_id=context.job_id,
            run_id=context.run_id,
            step_id=binding.step_id,
            output_name=binding.output_name,
            output_kind=binding.kind,
            binding=payload,
        )
        _record_event(
            context,
            step_id=binding.step_id,
            task_id=_task_id(context, binding.step_id),
            event_type="step_output_produced",
            status="success",
            event={
                "output_name": binding.output_name,
                "output_kind": binding.kind,
                "binding": payload,
            },
            adapter=self._adapter,
            created_at=created_at,
        )

    def _context_attempt(self, context: ExecutionContext, step_id: str) -> int:
        if step_id in context.step_attempts:
            attempt = context.step_attempt(step_id)
        else:
            attempt = self._attempts.get(step_id, 0) + 1
        self._attempts[step_id] = attempt
        return attempt

    def _record_step_finished(
        self, context: ExecutionContext, result: StepResult, *, status: str, event_type: str
    ) -> None:
        if result.step_id in context.step_attempts:
            attempt = context.step_attempt(result.step_id)
        else:
            attempt = self._attempts.get(result.step_id, 1)
        self._attempts[result.step_id] = attempt
        ended_at = _utc_now_iso()
        task_id = _task_id(context, result.step_id)
        self._adapter.step_execution_repository.record_execution(
            project_id=context.project_id,
            job_id=context.job_id,
            run_id=context.run_id,
            step_id=result.step_id,
            step_type=result.step_type,
            attempt=attempt,
            status=status,
            task_id=task_id,
            started_at=self._started_at_by_step.get(result.step_id),
            ended_at=ended_at,
            metadata=_result_metadata(context, result, attempt=attempt, task_id=task_id),
        )
        _record_event(
            context,
            step_id=result.step_id,
            task_id=task_id,
            event_type=event_type,
            status=status,
            event=_result_metadata(context, result, attempt=attempt, task_id=task_id),
            created_at=ended_at,
            adapter=self._adapter,
        )


def _record_event(
    context: ExecutionContext,
    *,
    step_id: str,
    task_id: str,
    event_type: str,
    status: str,
    event: dict[str, Any],
    adapter: Any,
    created_at: str | None = None,
) -> None:
    adapter.step_event_repository.record_event(
        project_id=context.project_id,
        job_id=context.job_id,
        run_id=context.run_id,
        step_id=step_id,
        task_id=task_id,
        event_type=event_type,
        status=status,
        event=_json_safe(event),
        created_at=created_at,
    )


def _step_metadata(context: ExecutionContext, step: ExecutionStep, *, attempt: int, task_id: str) -> dict[str, Any]:
    return {
        "attempt": attempt,
        "adapter_attempt": context.adapter_attempt(step.id),
        "profile": context.profile,
        "step_type": step.type,
        "task_id": task_id,
    }


def _result_metadata(
    context: ExecutionContext,
    result: StepResult,
    *,
    attempt: int,
    task_id: str,
) -> dict[str, Any]:
    return {
        "attempt": attempt,
        "adapter_attempt": context.adapter_attempt(result.step_id),
        "task_id": task_id,
        "state": result.state.value,
        "outputs": _json_safe(result.outputs),
        "failure": _json_safe(result.failure) if result.failure else None,
        "skipped_reason": result.skipped_reason,
    }


def _terminal_output_payload(result: RunResult) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        step_id: {output_name: step_output_binding_payload(binding) for output_name, binding in outputs.items()}
        for step_id, outputs in result.terminal_outputs.items()
    }


def _task_id(context: ExecutionContext, step_id: str) -> str:
    value = context.params.get("task_id")
    return str(value) if value else step_id


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [_json_safe(item) for item in value]
    if is_dataclass(value):
        return _json_safe(asdict(value))
    return repr(value)
