"""Core runner for backend-independent ExecutionPlan execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import import_module
import concurrent.futures
import os
import signal
import sys
import threading
import time
from typing import Any, Callable, Protocol

from zeta4s.core.execution_contract import (
    ResolvedStepDataBinding,
    RunResult,
    StepExecutionInput,
    StepExecutionState,
    StepFailure,
    StepOutputBinding,
    StepResult,
    json_safe as _json_safe,
    step_output_binding_key,
    step_output_binding_payload as _step_output_binding_payload,
)
from zeta4s.project.execution_plan import (
    ExecutionPlan,
    ExecutionStep,
    StepOutput,
)
from zeta4s.project.step_graph import STEP_EXPR_RE
from zeta4s.core.semantics import (
    aggregate_run_result,
    step_skip_reason,
)


class ConnectionResolver(Protocol):
    def resolve(self, conn_id: str) -> Any:
        """Resolve a connection id to an executable connection config."""


class ArtifactStore(Protocol):
    def put(self, key: str, value: Any) -> None:
        """Store an artifact or output binding."""

    def get(self, key: str) -> Any:
        """Read an artifact or output binding."""


class RunReporter(Protocol):
    def run_started(self, context: "ExecutionContext") -> None:
        """Record run start."""

    def run_succeeded(self, context: "ExecutionContext", result: RunResult) -> None:
        """Record run success."""

    def run_failed(self, context: "ExecutionContext", result: RunResult) -> None:
        """Record run failure."""

    def run_skipped(self, context: "ExecutionContext", result: RunResult) -> None:
        """Record run skip."""

    def step_started(self, context: "ExecutionContext", step: ExecutionStep) -> None:
        """Record step start."""

    def step_succeeded(self, context: "ExecutionContext", result: StepResult) -> None:
        """Record step success."""

    def step_failed(self, context: "ExecutionContext", result: StepResult) -> None:
        """Record step failure."""

    def step_skipped(self, context: "ExecutionContext", result: StepResult) -> None:
        """Record step skip."""

    def step_output_produced(self, context: "ExecutionContext", binding: StepOutputBinding) -> None:
        """Record a produced step output binding."""


class StepExecutor(Protocol):
    def execute(self, step: ExecutionStep, context: "ExecutionContext") -> StepResult:
        """Execute one ExecutionStep."""


class Runner(Protocol):
    def run(self, plan: ExecutionPlan, context: "ExecutionContext") -> RunResult:
        """Execute an ExecutionPlan."""

    def run_step(self, plan: ExecutionPlan, step: ExecutionStep, context: "ExecutionContext") -> StepResult:
        """Execute one step from an ExecutionPlan."""


class StepTimeoutError(TimeoutError):
    pass


def run_unsupported_step(*, step_id: str, step_type: str, **kwargs) -> None:
    raise NotImplementedError(f"step graph runtime adapter is not implemented yet: {step_id} type={step_type}")


def run_noop_step(**kwargs) -> dict[str, Any]:
    return {"status": "success", "details": {"outputs": {}}}


RuntimeCallable = Callable[..., Any] | str


@dataclass(frozen=True)
class RuntimeCallableStepExecutor:
    runtime_callable: RuntimeCallable
    kwargs: dict[str, Any] = field(default_factory=dict)
    connection_ids: tuple[str, ...] = ()

    def execute(self, step: ExecutionStep, context: "ExecutionContext") -> StepResult:
        kwargs = dict(self.kwargs)
        _inject_resolved_connections(kwargs, self.connection_ids, context.connection_resolver)
        step_execution = build_step_execution_input(step, context)
        kwargs.setdefault("step_execution", step_execution)
        kwargs.setdefault("step_execution_payload", step_execution.to_runtime_context_payload())
        runtime_context = build_step_runtime_context(step, context, step_execution=step_execution)
        kwargs.setdefault("runtime_context", runtime_context)
        for service_name in ("rowset_store", "step_checkpoint_repository"):
            if service_name in runtime_context:
                kwargs.setdefault(service_name, runtime_context[service_name])
        result = _resolve_runtime_callable(self.runtime_callable)(**kwargs)
        return _normalize_runtime_result(step, result)


class NullRunReporter:
    def run_started(self, context: "ExecutionContext") -> None:
        return None

    def run_succeeded(self, context: "ExecutionContext", result: RunResult) -> None:
        return None

    def run_failed(self, context: "ExecutionContext", result: RunResult) -> None:
        return None

    def run_skipped(self, context: "ExecutionContext", result: RunResult) -> None:
        return None

    def step_started(self, context: "ExecutionContext", step: ExecutionStep) -> None:
        return None

    def step_succeeded(self, context: "ExecutionContext", result: StepResult) -> None:
        return None

    def step_failed(self, context: "ExecutionContext", result: StepResult) -> None:
        return None

    def step_skipped(self, context: "ExecutionContext", result: StepResult) -> None:
        return None

    def step_output_produced(self, context: "ExecutionContext", binding: StepOutputBinding) -> None:
        return None


@dataclass
class LocalRunReporter:
    events: list[dict[str, Any]] = field(default_factory=list)

    def run_started(self, context: "ExecutionContext") -> None:
        self.events.append(_run_event(context, "run_started", {"started_at": context.started_at.isoformat()}))

    def run_succeeded(self, context: "ExecutionContext", result: RunResult) -> None:
        self._append_run_result_event(context, "run_succeeded", result)

    def run_failed(self, context: "ExecutionContext", result: RunResult) -> None:
        self._append_run_result_event(context, "run_failed", result)

    def run_skipped(self, context: "ExecutionContext", result: RunResult) -> None:
        self._append_run_result_event(context, "run_skipped", result)

    def _append_run_result_event(self, context: "ExecutionContext", event_type: str, result: RunResult) -> None:
        self.events.append(
            _run_event(
                context,
                event_type,
                {
                    "state": result.state.value,
                    "terminal_step_ids": list(result.terminal_step_ids),
                    "terminal_outputs": _terminal_output_payload(result),
                },
            )
        )

    def step_started(self, context: "ExecutionContext", step: ExecutionStep) -> None:
        self.events.append(
            _step_event(
                context,
                "step_started",
                step.id,
                {
                    "step_type": step.type,
                    "attempt": context.step_attempt(step.id),
                },
            )
        )

    def step_succeeded(self, context: "ExecutionContext", result: StepResult) -> None:
        self.events.append(_step_result_event(context, "step_succeeded", result))

    def step_failed(self, context: "ExecutionContext", result: StepResult) -> None:
        self.events.append(_step_result_event(context, "step_failed", result))

    def step_skipped(self, context: "ExecutionContext", result: StepResult) -> None:
        self.events.append(_step_result_event(context, "step_skipped", result))

    def step_output_produced(self, context: "ExecutionContext", binding: StepOutputBinding) -> None:
        payload: dict[str, Any] = {
            key: value for key, value in _step_output_binding_payload(binding).items() if key not in {"step_id"}
        }
        self.events.append(_step_event(context, "step_output_produced", binding.step_id, payload))


@dataclass
class InMemoryArtifactStore:
    values: dict[str, Any] = field(default_factory=dict)

    def put(self, key: str, value: Any) -> None:
        self.values[key] = value

    def get(self, key: str) -> Any:
        return self.values[key]


@dataclass
class StaticConnectionResolver:
    connections: dict[str, Any] = field(default_factory=dict)

    def resolve(self, conn_id: str) -> Any:
        try:
            return self.connections[conn_id]
        except KeyError as exc:
            raise KeyError(f"connection not found: {conn_id}") from exc


@dataclass
class ExecutionContext:
    project_id: str
    job_id: str
    run_id: str
    profile: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    connection_resolver: ConnectionResolver = field(default_factory=StaticConnectionResolver)
    artifact_store: ArtifactStore = field(default_factory=InMemoryArtifactStore)
    reporter: RunReporter = field(default_factory=NullRunReporter)
    step_results: dict[str, StepResult] = field(default_factory=dict)
    step_attempts: dict[str, int] = field(default_factory=dict)
    adapter_attempts: dict[str, int] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def start_step_attempt(self, step_id: str, attempt: int) -> None:
        self.step_attempts[step_id] = attempt

    def step_attempt(self, step_id: str) -> int:
        return self.step_attempts.get(step_id, 1)

    def start_adapter_attempt(self, step_id: str, attempt: int) -> None:
        self.adapter_attempts[step_id] = attempt

    def adapter_attempt(self, step_id: str) -> int:
        return self.adapter_attempts.get(step_id, 1)

    def step_output(self, step_id: str, output_name: str) -> Any:
        return self.step_output_binding(step_id, output_name).value

    def step_output_binding(self, step_id: str, output_name: str) -> StepOutputBinding:
        key = step_output_binding_key(step_id, output_name)
        try:
            binding = self.artifact_store.get(key)
        except KeyError:
            binding = None
        if isinstance(binding, StepOutputBinding):
            return binding
        result = self.step_results.get(step_id)
        if result is None:
            raise KeyError(f"step result not found: {step_id}")
        if output_name not in result.outputs:
            raise KeyError(f"step output not found: {step_id}.outputs.{output_name}")
        return StepOutputBinding(
            step_id=step_id,
            output_name=output_name,
            kind="value",
            value=result.outputs[output_name],
        )

    def bind_step_output(self, step: ExecutionStep, output_name: str, value: Any) -> StepOutputBinding:
        output_contract = _step_output_contract(step, output_name)
        binding = StepOutputBinding(
            step_id=step.id,
            output_name=output_name,
            kind=output_contract.kind if output_contract else "value",
            value=value,
            table_ref=output_contract.table_ref if output_contract else None,
            ref=dict(output_contract.ref) if output_contract else {},
        )
        self.artifact_store.put(binding.key, binding)
        self.reporter.step_output_produced(self, binding)
        return binding


def _run_event(context: "ExecutionContext", event_type: str, event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "project_id": context.project_id,
        "job_id": context.job_id,
        "run_id": context.run_id,
        "profile": context.profile,
        "event": _json_safe(event),
    }


def _step_event(context: "ExecutionContext", event_type: str, step_id: str, event: dict[str, Any]) -> dict[str, Any]:
    payload = _run_event(context, event_type, event)
    payload["step_id"] = step_id
    return payload


def _step_result_event(context: "ExecutionContext", event_type: str, result: StepResult) -> dict[str, Any]:
    event: dict[str, Any] = {
        "step_type": result.step_type,
        "state": result.state.value,
        "attempt": context.step_attempt(result.step_id),
        "outputs": _json_safe(result.outputs),
        "skipped_reason": result.skipped_reason,
    }
    if result.failure is not None:
        event["failure"] = {
            "type": result.failure.type,
            "message": result.failure.message,
        }
    return _step_event(context, event_type, result.step_id, event)


def _terminal_output_payload(result: RunResult) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        step_id: {output_name: _step_output_binding_payload(binding) for output_name, binding in outputs.items()}
        for step_id, outputs in result.terminal_outputs.items()
    }


class LocalRunner:
    """Local runner for `z4s run` and CI contract verification.

    Steps run in topological order on a thread pool. The first interrupt stops
    submitting new steps and waits for running ones; the second exits hard.
    Resume/recovery and scheduling are outside this runner's contract.
    """

    def __init__(self, executors: dict[str, StepExecutor]):
        self._executors = dict(executors)

    def run(self, plan: ExecutionPlan, context: ExecutionContext) -> RunResult:
        if context.job_id != plan.job_id:
            raise ValueError(f"execution context job_id mismatch: context={context.job_id}, plan={plan.job_id}")

        context.reporter.run_started(context)

        _topological_steps(plan)
        upstreams = _control_upstreams_by_step(plan)
        downstreams: dict[str, list[str]] = {step.id: [] for step in plan.steps}
        in_degree: dict[str, int] = {step.id: 0 for step in plan.steps}
        for down_id, up_ids in upstreams.items():
            in_degree[down_id] = len(up_ids)
            for up_id in up_ids:
                downstreams[up_id].append(down_id)

        cancel_event = threading.Event()
        step_lock = threading.Lock()
        futures: set[concurrent.futures.Future[str | None]] = set()

        def _execute_node(step_id: str) -> str | None:
            if cancel_event.is_set():
                return

            step = plan.step_by_id[step_id]
            result = self._run_step(plan, step, context, apply_verification_policy=True)

            with step_lock:
                context.step_results[step.id] = result
                if result.succeeded:
                    _bind_step_outputs(step, result, context)
                _report_step_result(context, result)

            return step_id

        with concurrent.futures.ThreadPoolExecutor() as executor:
            try:
                initial_ready = [step_id for step_id, degree in in_degree.items() if degree == 0]
                for step_id in initial_ready:
                    futures.add(executor.submit(_execute_node, step_id))

                while futures:
                    done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                    for f in done:
                        completed_step_id = f.result()
                        if completed_step_id is None or cancel_event.is_set():
                            continue
                        for down_id in downstreams[completed_step_id]:
                            in_degree[down_id] -= 1
                            if in_degree[down_id] == 0:
                                futures.add(executor.submit(_execute_node, down_id))

            except KeyboardInterrupt:
                sys.stderr.write("\n[LocalRunner] Graceful stop requested. Waiting for running steps to finish...\n")
                sys.stderr.write("[LocalRunner] Press Ctrl+C again to force quit.\n")
                sys.stderr.flush()
                cancel_event.set()

                def force_quit(signum: int, frame: Any) -> None:
                    sys.stderr.write("\n[LocalRunner] Hard stop forced. Exiting immediately.\n")
                    sys.stderr.flush()
                    os._exit(1)

                original_handler = signal.signal(signal.SIGINT, force_quit)
                try:
                    concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
                finally:
                    signal.signal(signal.SIGINT, original_handler)

        run_result = aggregate_run_result(plan, context.step_results, context.step_output_binding)
        _report_run_result(context, run_result)
        return run_result

    def run_step(self, plan: ExecutionPlan, step: ExecutionStep, context: ExecutionContext) -> StepResult:
        if context.job_id != plan.job_id:
            raise ValueError(f"execution context job_id mismatch: context={context.job_id}, plan={plan.job_id}")
        if plan.step_by_id.get(step.id) != step:
            raise ValueError(f"execution step is not part of plan: {step.id}")
        result = self._run_step(plan, step, context, apply_verification_policy=False)
        context.step_results[step.id] = result
        if result.succeeded:
            _bind_step_outputs(step, result, context)
        _report_step_result(context, result)
        return result

    def _run_step(
        self,
        plan: ExecutionPlan,
        step: ExecutionStep,
        context: ExecutionContext,
        *,
        apply_verification_policy: bool,
    ) -> StepResult:
        try:
            skip_reason = step_skip_reason(plan, step, context.step_results, context.step_output)
        except Exception as exc:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.FAILED,
                failure=StepFailure(str(exc), type=exc.__class__.__name__),
            )
        if skip_reason:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.SKIPPED,
                skipped_reason=skip_reason,
            )
        binding_failure = _data_binding_failure(step, context)
        if binding_failure is not None:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.FAILED,
                failure=binding_failure,
            )
        executor = self._executors.get(step.id) or self._executors.get(step.type)
        if executor is None:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.FAILED,
                failure=StepFailure(f"step executor not found: {step.type}", type="StepExecutorNotFound"),
            )
        if apply_verification_policy:
            return _run_step_with_policy(executor, step, context)
        if step.id not in context.step_attempts:
            context.start_step_attempt(step.id, 1)
        context.reporter.step_started(context, step)
        return validate_step_result(step, _execute_step_once(executor, step, context))


def _run_step_with_policy(executor: StepExecutor, step: ExecutionStep, context: ExecutionContext) -> StepResult:
    max_attempts = _max_attempts(step)
    delay_seconds = _retry_delay_seconds(step)
    last_result: StepResult | None = None
    for attempt in range(1, max_attempts + 1):
        context.start_step_attempt(step.id, attempt)
        context.reporter.step_started(context, step)
        result = _execute_step_attempt(executor, step, context)
        result = validate_step_result(step, result)
        if not result.failed:
            return result
        last_result = result
        if attempt < max_attempts:
            context.reporter.step_failed(context, result)
        if attempt < max_attempts and delay_seconds:
            time.sleep(delay_seconds)
    return last_result or StepResult(
        step_id=step.id,
        step_type=step.type,
        state=StepExecutionState.FAILED,
        failure=StepFailure("step failed without result"),
    )


def _execute_step_attempt(executor: StepExecutor, step: ExecutionStep, context: ExecutionContext) -> StepResult:
    try:
        return _execute_with_timeout(executor, step, context, _timeout_seconds(step))
    except Exception as exc:
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.FAILED,
            failure=StepFailure(str(exc), type=exc.__class__.__name__),
        )


def _execute_step_once(executor: StepExecutor, step: ExecutionStep, context: ExecutionContext) -> StepResult:
    try:
        return executor.execute(step, context)
    except Exception as exc:
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.FAILED,
            failure=StepFailure(str(exc), type=exc.__class__.__name__),
        )


def validate_step_result(step: ExecutionStep, result: StepResult) -> StepResult:
    if result.step_id != step.id:
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.FAILED,
            failure=StepFailure(
                f"step executor returned result for wrong step: {result.step_id}, expected {step.id}",
                type="StepResultMismatch",
            ),
        )
    if result.succeeded:
        result = _step_result_with_declared_table_outputs(step, result)
        output_failure = _step_output_contract_failure(step, result)
        if output_failure is not None:
            return StepResult(
                step_id=step.id,
                step_type=step.type,
                state=StepExecutionState.FAILED,
                failure=output_failure,
                raw_result=result.raw_result,
            )
    return result


def _max_attempts(step: ExecutionStep) -> int:
    retry = step.flow.retry or {}
    return max(1, int(retry.get("max_attempts") or 1))


def _retry_delay_seconds(step: ExecutionStep) -> int:
    retry = step.flow.retry or {}
    return max(0, int(retry.get("delay_seconds") or 0))


def _timeout_seconds(step: ExecutionStep) -> int | None:
    timeout = step.flow.timeout or {}
    if not timeout:
        return None
    return max(1, int(timeout["seconds"]))


def _execute_with_timeout(
    executor: StepExecutor,
    step: ExecutionStep,
    context: ExecutionContext,
    timeout_seconds: int | None,
) -> StepResult:
    if timeout_seconds is None:
        return executor.execute(step, context)
    started = time.monotonic()
    result = executor.execute(step, context)
    if time.monotonic() - started > timeout_seconds:
        raise StepTimeoutError(f"step timed out after {timeout_seconds}s: {step.id}")
    return result


def _topological_steps(plan: ExecutionPlan) -> tuple[ExecutionStep, ...]:
    step_by_id = plan.step_by_id
    upstreams = _control_upstreams_by_step(plan)
    ordered: list[ExecutionStep] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(step_id: str, stack: list[str]) -> None:
        if step_id in visited:
            return
        if step_id in visiting:
            cycle = stack[stack.index(step_id) :] + [step_id]
            raise ValueError("execution plan cycle 이 있다: " + " -> ".join(cycle))
        visiting.add(step_id)
        stack.append(step_id)
        for upstream_id in upstreams.get(step_id, ()):
            visit(upstream_id, stack)
        stack.pop()
        visiting.remove(step_id)
        visited.add(step_id)
        ordered.append(step_by_id[step_id])

    for step in plan.steps:
        visit(step.id, [])
    return tuple(ordered)


def _control_upstreams_by_step(plan: ExecutionPlan) -> dict[str, tuple[str, ...]]:
    step_ids = [step.id for step in plan.steps]
    if not step_ids:
        raise ValueError(f"execution plan has no step: {plan.job_id}")
    duplicate_step_ids = sorted({step_id for step_id in step_ids if step_ids.count(step_id) > 1})
    if duplicate_step_ids:
        raise ValueError("execution plan has duplicated step ids: " + ", ".join(duplicate_step_ids))
    step_id_set = set(step_ids)
    upstreams: dict[str, list[str]] = {step.id: [] for step in plan.steps}
    for edge in plan.control_edges:
        if edge.upstream_id not in step_id_set:
            raise ValueError(
                f"execution plan edge references unknown upstream step: {edge.upstream_id} -> {edge.downstream_id}"
            )
        if edge.downstream_id not in step_id_set:
            raise ValueError(
                f"execution plan edge references unknown downstream step: {edge.upstream_id} -> {edge.downstream_id}"
            )
        if edge.upstream_id == edge.downstream_id:
            raise ValueError(f"execution plan self edge is not allowed: {edge.upstream_id}")
        if edge.upstream_id not in upstreams[edge.downstream_id]:
            upstreams[edge.downstream_id].append(edge.upstream_id)
    return {step_id: tuple(ids) for step_id, ids in upstreams.items()}


def _bind_step_outputs(step: ExecutionStep, result: StepResult, context: ExecutionContext) -> None:
    declared_output_names = {output.name for output in step.outputs}
    for output_name, value in result.outputs.items():
        if output_name not in declared_output_names:
            continue
        context.bind_step_output(step, output_name, value)


def _report_step_result(context: ExecutionContext, result: StepResult) -> None:
    if result.succeeded:
        context.reporter.step_succeeded(context, result)
        return
    if result.failed:
        context.reporter.step_failed(context, result)
        return
    if result.skipped:
        context.reporter.step_skipped(context, result)
        return
    raise ValueError(f"unsupported step result state: {result.state}")


def _report_run_result(context: ExecutionContext, result: RunResult) -> None:
    if result.state == StepExecutionState.SUCCEEDED:
        context.reporter.run_succeeded(context, result)
        return
    if result.state == StepExecutionState.FAILED:
        context.reporter.run_failed(context, result)
        return
    if result.state == StepExecutionState.SKIPPED:
        context.reporter.run_skipped(context, result)
        return
    raise ValueError(f"unsupported run result state: {result.state}")


def _step_output_contract(step: ExecutionStep, output_name: str) -> StepOutput | None:
    outputs = {output.name: output for output in step.outputs}
    return outputs.get(output_name)


def _resolve_runtime_callable(runtime_callable: RuntimeCallable) -> Callable[..., Any]:
    if callable(runtime_callable):
        return runtime_callable
    module_name, separator, attr_name = runtime_callable.partition(":")
    if not separator or not module_name or not attr_name:
        raise ValueError(f"runtime callable must use module:function format: {runtime_callable}")
    module = _import_runtime_callable_module(module_name)
    resolved = getattr(module, attr_name)
    if not callable(resolved):
        raise TypeError(f"runtime callable is not callable: {runtime_callable}")
    return resolved


def _import_runtime_callable_module(module_name: str) -> Any:
    try:
        return import_module(module_name)
    except ModuleNotFoundError as exc:
        requested_parts = module_name.split(".")
        missing_name = exc.name or ""
        missing_parts = missing_name.split(".")
        if requested_parts[: len(missing_parts)] != missing_parts:
            raise
        alias = module_name.rsplit(".", 1)[-1]
        module = sys.modules.get(alias)
        if module is not None:
            return module
        raise


def _inject_resolved_connections(
    kwargs: dict[str, Any],
    connection_ids: tuple[str, ...],
    connection_resolver: ConnectionResolver,
) -> None:
    if not connection_ids:
        return
    connections = dict(kwargs.get("connections") or {})
    for conn_id in connection_ids:
        connections.setdefault(conn_id, connection_resolver.resolve(conn_id))
    kwargs["connections"] = connections
    kwargs.setdefault("connection_types", _connection_types(connections))


def _connection_types(connections: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for conn_id, connection in connections.items():
        if isinstance(connection, dict):
            conn_type = connection.get("type") or connection.get("conn_type")
        else:
            conn_type = getattr(connection, "conn_type", None)
        if conn_type:
            result[str(conn_id)] = str(conn_type).strip().lower()
    return result


def build_step_runtime_context(
    step: ExecutionStep,
    context: ExecutionContext,
    *,
    task_id: str | None = None,
    step_execution: StepExecutionInput | None = None,
) -> dict[str, Any]:
    step_execution_input = step_execution or build_step_execution_input(step, context)
    return {
        **context.params,
        "project_id": context.project_id,
        "job_id": context.job_id,
        "run_id": context.run_id,
        "z4_run_id": context.run_id,
        "task_id": task_id or context.params.get("_adapter_task_id") or step.id,
        "attempt": context.step_attempt(step.id),
        "profile": context.profile,
        "step_output_bindings": _step_output_bindings_payload(context),
        "step_execution": step_execution_input.to_runtime_context_payload(),
    }


def build_step_execution_input(step: ExecutionStep, context: ExecutionContext) -> StepExecutionInput:
    return StepExecutionInput(
        step_id=step.id,
        step_type=step.type,
        params=dict(step.step.params or {}),
        inputs=step.inputs,
        outputs=step.outputs,
        data_bindings=step.data_bindings,
        flow=step.flow,
        runtime=step.runtime,
        upstream_output_bindings=_resolved_step_output_bindings(
            context,
            step_ids=_step_execution_upstream_ids(step),
        ),
        resolved_data_bindings=_resolved_step_data_bindings(step, context),
    )


def _step_output_bindings_payload(context: ExecutionContext) -> dict[str, dict[str, Any]]:
    return {
        key: _step_output_binding_payload(binding) for key, binding in _resolved_step_output_bindings(context).items()
    }


def _resolved_step_output_bindings(
    context: ExecutionContext,
    *,
    step_ids: set[str] | None = None,
) -> dict[str, StepOutputBinding]:
    bindings: dict[str, StepOutputBinding] = {}
    for result in context.step_results.values():
        if not result.succeeded:
            continue
        if step_ids is not None and result.step_id not in step_ids:
            continue
        for output_name in result.outputs:
            try:
                binding = context.artifact_store.get(step_output_binding_key(result.step_id, output_name))
            except KeyError:
                continue
            if isinstance(binding, StepOutputBinding):
                bindings[f"{binding.step_id}.{binding.output_name}"] = binding
    return bindings


def _step_execution_upstream_ids(step: ExecutionStep) -> set[str]:
    upstream_ids = set(step.flow.depends_on)
    upstream_ids.update(binding.source.step_id for binding in step.data_bindings)
    when = step.flow.when or {}
    for key in ("success", "failed"):
        if when.get(key):
            upstream_ids.add(str(when[key]))
    expr = when.get("expr")
    if expr:
        match = STEP_EXPR_RE.match(str(expr))
        if match:
            upstream_ids.add(match.group(1))
    return upstream_ids


def _resolved_step_data_bindings(
    step: ExecutionStep,
    context: ExecutionContext,
) -> tuple[ResolvedStepDataBinding, ...]:
    resolved: list[ResolvedStepDataBinding] = []
    for binding in step.data_bindings:
        try:
            output = context.step_output_binding(binding.source.step_id, binding.source.output_name)
        except KeyError:
            continue
        resolved.append(ResolvedStepDataBinding(binding=binding, output=output))
    return tuple(resolved)


def _normalize_runtime_result(step: ExecutionStep, result: Any) -> StepResult:
    if isinstance(result, StepResult):
        return result
    if not isinstance(result, dict):
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED, raw_result=result)
    status = str(result.get("status") or "success").strip().lower()
    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    if status in {"success", "succeeded"}:
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.SUCCEEDED,
            outputs=_runtime_result_outputs(step, result, details, synthesize_declared_tables=True),
            raw_result=result,
        )
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    return StepResult(
        step_id=step.id,
        step_type=step.type,
        state=StepExecutionState.FAILED,
        outputs=_runtime_result_outputs(step, result, details, synthesize_declared_tables=False),
        failure=StepFailure(
            str(error.get("message") or result.get("message") or f"runtime callable failed: {step.id}"),
            type=str(error.get("type") or "RuntimeCallableError"),
        ),
        raw_result=result,
    )


def _runtime_result_outputs(
    step: ExecutionStep,
    result: dict[str, Any],
    details: dict[str, Any],
    *,
    synthesize_declared_tables: bool,
) -> dict[str, Any]:
    outputs = details.get("outputs") if isinstance(details.get("outputs"), dict) else result.get("outputs")
    if isinstance(outputs, dict):
        return dict(outputs)
    if not synthesize_declared_tables:
        return {}
    return _declared_table_outputs(step)


def _declared_table_outputs(step: ExecutionStep) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    for output in step.outputs:
        if output.kind != "table" or output.table_ref is None:
            continue
        outputs[output.name] = {
            "kind": "table",
            "conn": output.table_ref.conn,
            "table": output.table_ref.table,
        }
    return outputs


def _step_result_with_declared_table_outputs(step: ExecutionStep, result: StepResult) -> StepResult:
    table_outputs = _declared_table_outputs(step)
    missing_table_outputs = {
        output_name: value for output_name, value in table_outputs.items() if output_name not in result.outputs
    }
    if not missing_table_outputs:
        return result
    return StepResult(
        step_id=result.step_id,
        step_type=result.step_type,
        state=result.state,
        outputs={**result.outputs, **missing_table_outputs},
        failure=result.failure,
        skipped_reason=result.skipped_reason,
        raw_result=result.raw_result,
    )


def _step_output_contract_failure(step: ExecutionStep, result: StepResult) -> StepFailure | None:
    declared_outputs = {output.name: output for output in step.outputs}
    if not declared_outputs:
        return None
    missing = sorted(
        output.name for output in step.outputs if output.name not in result.outputs and output.kind != "table"
    )
    if missing:
        return StepFailure(
            f"step did not return declared outputs: {', '.join(missing)}",
            type="StepOutputContractMissing",
        )
    return None


def _data_binding_failure(step: ExecutionStep, context: ExecutionContext) -> StepFailure | None:
    for binding in step.data_bindings:
        ref_label = f"{binding.source.step_id}.{binding.source.output_name}"
        upstream_result = context.step_results.get(binding.source.step_id)
        if upstream_result is None or not upstream_result.succeeded:
            continue
        if binding.source.output_name not in upstream_result.outputs:
            return StepFailure(
                f"required upstream output missing for {step.id} {binding.field}: {ref_label}",
                type="StepDataBindingMissing",
            )
        try:
            output = context.step_output_binding(binding.source.step_id, binding.source.output_name)
        except KeyError:
            return StepFailure(
                f"required upstream output binding missing for {step.id} {binding.field}: {ref_label}",
                type="StepDataBindingMissing",
            )
        if output.kind != binding.required_kind:
            return StepFailure(
                f"required upstream output kind mismatch for {step.id} {binding.field}: "
                f"{ref_label} kind={output.kind}, required={binding.required_kind}",
                type="StepDataBindingKindMismatch",
            )
        if isinstance(output.value, dict):
            value_kind = output.value.get("kind")
            if value_kind is not None and str(value_kind).strip().lower() != binding.required_kind:
                return StepFailure(
                    f"required upstream output value kind mismatch for {step.id} {binding.field}: "
                    f"{ref_label} value.kind={value_kind}, required={binding.required_kind}",
                    type="StepDataBindingKindMismatch",
                )
    return None
