"""Airflow operator-facing lazy runtime callables.

이 모듈은 DAG parse 시점에 heavy runtime dependency 를 import 하지 않도록
Airflow task callable 의 얇은 facade 만 제공한다.
"""

from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
from typing import Any

from zeta4s.airflow.connections import AirflowConnectionResolver
from zeta4s.core import (
    ConnectionResolver,
    ExecutionContext,
    LocalRunner,
    StepExecutionState,
    StepFailure,
    StepResult,
)
from zeta4s.core.step_executors import built_in_step_executor
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import StepGraphJob
from zeta4s.project.step_types import register_installed_step_types
from zeta4s.runtime.metastore_artifacts import MetastoreArtifactStore
from zeta4s.runtime.metastore_reporter import MetastoreRunReporter


def run_core_step(
    *,
    step_config: dict[str, Any],
    job_config: dict[str, Any],
    project_config: dict[str, Any],
    run_id: str | None = None,
    profile: str | None = None,
    connection_resolver: ConnectionResolver | None = None,
    **kwargs,
):
    context = _airflow_runtime_context(kwargs)
    z4_run_id = _zeta4s_run_id(context, explicit_run_id=run_id)
    resolved_profile = _zeta4s_profile(context, explicit_profile=profile)
    step_id = str(step_config.get("step_id") or step_config.get("id") or "")
    task_id = _context_task_id(context) or step_id
    runtime_context = _core_runtime_params(
        context,
        task_id=task_id,
        run_id=z4_run_id,
        profile=resolved_profile,
    )
    # 설치된 외부 step type 을 StepGraphJob 검증 및 executor dispatch 이전에 등록한다.
    # worker 이미지에 설치된 zeta4s.step_types 플러그인을 discover 한다.
    register_installed_step_types()
    job = StepGraphJob.model_validate(job_config)
    plan = build_step_graph_execution_plan(job)
    plan_step = plan.step_by_id[step_id]
    project = _project_context(project_config)
    executor = built_in_step_executor(
        project=project,
        plan=plan,
        plan_step=plan_step,
        runtime_home=str(_runtime_home()),
    )
    reporter = MetastoreRunReporter()
    from zeta4s.runtime.rowset_stores.iceberg import IcebergRowsetStore

    runtime_context.update(
        {
            "adapter_attempt": 1,
            "rowset_store": IcebergRowsetStore.from_environment(),
            "step_checkpoint_repository": reporter.adapter.step_checkpoint_repository,
        }
    )
    execution_context = ExecutionContext(
        project_id=project.project_id,
        job_id=plan.job_id,
        run_id=z4_run_id or context.get("run_id") or "airflow",
        profile=resolved_profile,
        params=runtime_context,
        connection_resolver=connection_resolver or AirflowConnectionResolver(),
        artifact_store=MetastoreArtifactStore(
            project_id=project.project_id,
            job_id=plan.job_id,
            run_id=z4_run_id or context.get("run_id") or "airflow",
            repository=reporter.adapter.step_output_binding_repository,
        ),
        reporter=reporter,
    )
    execution_context.start_step_attempt(plan_step.id, int(runtime_context.get("try_number") or 1))
    execution_context.start_adapter_attempt(plan_step.id, int(runtime_context["adapter_attempt"]))
    _seed_upstream_output_results(plan, plan_step, execution_context)
    result = LocalRunner({plan_step.id: executor}).run_step(plan, plan_step, execution_context)
    if result.state == StepExecutionState.FAILED:
        failure = result.failure
        raise RuntimeError(failure.message if failure else f"step failed: {step_id}")
    if result.state == StepExecutionState.SKIPPED:
        _raise_airflow_skip(result)
    if isinstance(result.raw_result, dict):
        return result.raw_result
    return {
        "status": result.state.value,
        "details": {"outputs": result.outputs},
    }


def _core_runtime_params(
    context: dict[str, Any],
    *,
    task_id: str,
    run_id: str | None,
    profile: str | None,
) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance") or context.get("ti")
    params = {
        "adapter": "airflow",
        "dag_id": context.get("dag_id") or getattr(dag_run, "dag_id", None),
        "task_id": task_id,
        "_adapter_task_id": task_id,
        "run_id": run_id or context.get("run_id"),
        "z4_run_id": run_id or context.get("run_id"),
        "profile": profile,
        "try_number": getattr(task_instance, "try_number", None) or context.get("try_number"),
        "logical_date": context.get("logical_date"),
        "data_interval_start": context.get("data_interval_start"),
        "data_interval_end": context.get("data_interval_end"),
        "ds": context.get("ds"),
        "ts": context.get("ts"),
    }
    return {key: value for key, value in params.items() if value is not None}


def _airflow_runtime_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    context = dict(kwargs)
    try:
        from airflow.sdk import get_current_context

        context.update(get_current_context())
    except Exception:
        pass
    return context


def _zeta4s_run_id(context: dict[str, Any], *, explicit_run_id: str | None) -> str | None:
    if explicit_run_id:
        return explicit_run_id
    dag_run = context.get("dag_run")
    dag_run_conf = getattr(dag_run, "conf", None) or {}
    return dag_run_conf.get("z4_run_id") or getattr(dag_run, "run_id", None) or context.get("run_id")


def _zeta4s_profile(context: dict[str, Any], *, explicit_profile: str | None) -> str | None:
    if explicit_profile:
        return explicit_profile
    dag_run = context.get("dag_run")
    dag_run_conf = getattr(dag_run, "conf", None) or {}
    value = dag_run_conf.get("profile") or dag_run_conf.get("profile_id") or context.get("profile")
    return str(value) if value else None


def _context_task_id(context: dict[str, Any]) -> str | None:
    task = context.get("task")
    if task is not None and getattr(task, "task_id", None):
        return str(task.task_id)
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is not None and getattr(task_instance, "task_id", None):
        return str(task_instance.task_id)
    value = context.get("task_id")
    return str(value) if value else None


def _project_context(config: dict[str, Any]) -> ProjectContext:
    registered_at = config.get("registered_at")
    return ProjectContext(
        project_id=str(config["project_id"]),
        root=Path(config["root"]),
        jobs_dir=Path(config["jobs_dir"]),
        assets_dir=Path(config["assets_dir"]),
        dbt_dir=Path(config["dbt_dir"]),
        timezone=str(config["timezone"]),
        display_name=config.get("display_name"),
        registered_at=datetime.fromisoformat(registered_at) if registered_at else None,
    )


def _seed_upstream_output_results(plan, plan_step, context: ExecutionContext) -> None:
    _seed_upstream_execution_results(plan, plan_step, context)
    for upstream_id in plan.upstream_ids_by_step[plan_step.id]:
        upstream = plan.step_by_id[upstream_id]
        outputs: dict[str, Any] = {}
        for output in upstream.outputs:
            try:
                binding = context.step_output_binding(upstream.id, output.name)
            except KeyError:
                continue
            outputs[output.name] = binding.value
        if outputs:
            current = context.step_results.get(upstream.id)
            context.step_results[upstream.id] = StepResult(
                step_id=upstream.id,
                step_type=upstream.type,
                state=current.state if current is not None else StepExecutionState.SUCCEEDED,
                outputs=outputs,
                failure=current.failure if current is not None else None,
                skipped_reason=current.skipped_reason if current is not None else None,
            )


def _seed_upstream_execution_results(plan, plan_step, context: ExecutionContext) -> None:
    repository = getattr(getattr(context.reporter, "adapter", None), "step_execution_repository", None)
    if repository is None or not hasattr(repository, "list_executions"):
        return
    executions = repository.list_executions(
        project_id=context.project_id,
        job_id=context.job_id,
        run_id=context.run_id,
    )
    latest_by_step: dict[str, dict[str, Any]] = {}
    for execution in executions:
        step_id = str(execution.get("step_id") or "")
        if step_id in plan.upstream_ids_by_step[plan_step.id]:
            current = latest_by_step.get(step_id)
            if current is None or _is_newer_step_execution(execution, current):
                latest_by_step[step_id] = execution
    for upstream_id, execution in latest_by_step.items():
        upstream = plan.step_by_id[upstream_id]
        state = _step_execution_state(str(execution.get("status") or ""))
        failure = None
        if state == StepExecutionState.FAILED:
            failure = StepFailure(f"upstream step failed: {upstream_id}", type="UpstreamStepFailed")
        context.step_results[upstream_id] = StepResult(
            step_id=upstream.id,
            step_type=upstream.type,
            state=state,
            failure=failure,
        )


def _step_execution_state(status: str) -> StepExecutionState:
    normalized = status.strip().lower()
    if normalized in {"success", "succeeded"}:
        return StepExecutionState.SUCCEEDED
    if normalized in {"failed", "failure"}:
        return StepExecutionState.FAILED
    if normalized == "skipped":
        return StepExecutionState.SKIPPED
    return StepExecutionState.PENDING


def _is_newer_step_execution(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    return _step_execution_sort_key(candidate) > _step_execution_sort_key(current)


def _step_execution_sort_key(execution: dict[str, Any]) -> tuple[int, int, str]:
    return (
        _int_value(execution.get("attempt")),
        _int_value(execution.get("revision")),
        str(execution.get("updated_at") or ""),
    )


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _raise_airflow_skip(result: StepResult) -> None:
    message = result.skipped_reason or f"step skipped: {result.step_id}"
    try:
        from airflow.exceptions import AirflowSkipException
    except ImportError:
        raise RuntimeError(message) from None
    raise AirflowSkipException(message)


def _runtime_home() -> Path:
    return Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))


__all__ = [
    "run_core_step",
]
