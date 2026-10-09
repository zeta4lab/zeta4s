"""Scheduler-projected core execution facades shared by internal workers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from zeta4s.core import (
    ExecutionContext,
    LocalRunner,
    StepExecutionState,
    StepFailure,
    StepResult,
    aggregate_run_result,
    step_skip_reason,
)
from zeta4s.core.step_executors import built_in_step_executor
from zeta4s.project.execution_plan import ExecutionPlan, build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext, load_project_context
from zeta4s.project.step_graph import StepGraphJob, step_graph_config_paths
from zeta4s.runtime.connections import ProfileConnectionResolver
from zeta4s.runtime.metastore_artifacts import MetastoreArtifactStore
from zeta4s.runtime.metastore_reporter import MetastoreRunReporter


def load_scheduled_plan(project_root: str | Path, job_id: str) -> tuple[ExecutionPlan, ProjectContext]:
    project = load_project_context(Path(project_root))
    for config_path in step_graph_config_paths(project.jobs_dir):
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        job = StepGraphJob.model_validate(config)
        if job.job_id == job_id:
            return build_step_graph_execution_plan(job), project
    raise ValueError(f"job not found in scheduled project: {project.project_id}/{job_id}")


def start_scheduled_run(
    project: ProjectContext,
    plan: ExecutionPlan,
    run_id: str,
    profile: str,
    *,
    parameters: dict[str, Any] | None = None,
    scheduler: str | None = None,
    scheduler_run_id: str | None = None,
) -> None:
    MetastoreRunReporter().run_started(
        ExecutionContext(
            project_id=project.project_id,
            job_id=plan.job_id,
            run_id=run_id,
            profile=profile,
            params={
                "parameters": dict(parameters or {}),
                "scheduler": scheduler,
                "scheduler_run_id": scheduler_run_id,
            },
        )
    )


def run_scheduled_step(
    *,
    project: ProjectContext,
    plan: ExecutionPlan,
    step_id: str,
    run_id: str,
    profile: str,
    profile_data: dict[str, Any],
    attempt: int = 1,
    adapter_attempt: int = 1,
    adapter: str = "internal",
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    step = plan.step_by_id[step_id]
    reporter = MetastoreRunReporter()
    from zeta4s.runtime.rowset_stores.iceberg import IcebergRowsetStore

    context = ExecutionContext(
        project_id=project.project_id,
        job_id=plan.job_id,
        run_id=run_id,
        profile=profile,
        params={
            **dict(parameters or {}),
            "adapter": adapter,
            "task_id": step_id,
            "adapter_attempt": adapter_attempt,
            "rowset_store": IcebergRowsetStore.from_environment(),
            "step_checkpoint_repository": reporter.adapter.step_checkpoint_repository,
        },
        connection_resolver=ProfileConnectionResolver(profile_data),
        artifact_store=MetastoreArtifactStore(
            project_id=project.project_id,
            job_id=plan.job_id,
            run_id=run_id,
            repository=reporter.adapter.step_output_binding_repository,
        ),
        reporter=reporter,
    )
    context.start_step_attempt(step_id, attempt)
    context.start_adapter_attempt(step_id, adapter_attempt)
    _seed_upstream_results(plan, step_id, context)
    executor = built_in_step_executor(
        project=project,
        plan=plan,
        plan_step=step,
        runtime_home=os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"),
    )
    result = LocalRunner({step_id: executor}).run_step(plan, step, context)
    if result.state == StepExecutionState.FAILED:
        raise RuntimeError(result.failure.message if result.failure else f"step failed: {step_id}")
    response: dict[str, Any] = {
        "status": result.state.value,
        "details": {"outputs": result.outputs},
    }
    # generated Airflow DAG 는 이 값을 AirflowSkipException 사유로 그대로 쓴다.
    if result.state == StepExecutionState.SKIPPED and result.skipped_reason:
        response["reason"] = result.skipped_reason
    return response


def finalize_scheduled_run(
    project: ProjectContext,
    plan: ExecutionPlan,
    run_id: str,
    profile: str,
) -> dict[str, Any]:
    reporter = MetastoreRunReporter()
    context = ExecutionContext(
        project_id=project.project_id,
        job_id=plan.job_id,
        run_id=run_id,
        profile=profile,
        artifact_store=MetastoreArtifactStore(
            project_id=project.project_id,
            job_id=plan.job_id,
            run_id=run_id,
            repository=reporter.adapter.step_output_binding_repository,
        ),
        reporter=reporter,
    )
    records = reporter.adapter.step_execution_repository.list_executions(
        project_id=project.project_id,
        job_id=plan.job_id,
        run_id=run_id,
    )
    latest = _latest_execution_by_step(records)
    for step in plan.steps:
        record = latest.get(step.id)
        if record is not None:
            context.step_results[step.id] = _record_step_result(step.id, step.type, record)
            continue
        reason = step_skip_reason(plan, step, context.step_results, context.step_output)
        context.step_results[step.id] = StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.SKIPPED if reason else StepExecutionState.FAILED,
            skipped_reason=reason,
            failure=None if reason else StepFailure("scheduler step did not record a result"),
        )
    result = aggregate_run_result(plan, context.step_results, context.step_output_binding)
    if result.state == StepExecutionState.FAILED:
        reporter.run_failed(context, result)
    elif result.state == StepExecutionState.SKIPPED:
        reporter.run_skipped(context, result)
    else:
        reporter.run_succeeded(context, result)
    return {"run_id": run_id, "state": result.state.value}


def _seed_upstream_results(plan: ExecutionPlan, step_id: str, context: ExecutionContext) -> None:
    records = context.reporter.adapter.step_execution_repository.list_executions(
        project_id=context.project_id,
        job_id=context.job_id,
        run_id=context.run_id,
    )
    latest = _latest_execution_by_step(records)
    for upstream_id in plan.upstream_ids_by_step[step_id]:
        record = latest.get(upstream_id)
        if record is None:
            continue
        upstream = plan.step_by_id[upstream_id]
        result = _record_step_result(upstream_id, upstream.type, record)
        outputs = {}
        for output in upstream.outputs:
            try:
                outputs[output.name] = context.step_output_binding(upstream_id, output.name).value
            except KeyError:
                continue
        context.step_results[upstream_id] = StepResult(
            step_id=result.step_id,
            step_type=result.step_type,
            state=result.state,
            outputs=outputs,
            failure=result.failure,
            skipped_reason=result.skipped_reason,
        )


def _latest_execution_by_step(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest = {}
    for record in records:
        step_id = str(record.get("step_id") or "")
        current = latest.get(step_id)
        key = (int(record.get("attempt") or 0), int(record.get("revision") or 0), str(record.get("updated_at") or ""))
        current_key = (
            (
                int(current.get("attempt") or 0),
                int(current.get("revision") or 0),
                str(current.get("updated_at") or ""),
            )
            if current
            else None
        )
        if current_key is None or key > current_key:
            latest[step_id] = record
    return latest


def _record_step_result(step_id: str, step_type: str, record: dict[str, Any]) -> StepResult:
    status = str(record.get("status") or "").lower()
    state = {
        "success": StepExecutionState.SUCCEEDED,
        "succeeded": StepExecutionState.SUCCEEDED,
        "failed": StepExecutionState.FAILED,
        "skipped": StepExecutionState.SKIPPED,
    }.get(status, StepExecutionState.FAILED)
    metadata = record.get("metadata") or {}
    return StepResult(
        step_id=step_id,
        step_type=step_type,
        state=state,
        outputs=dict(metadata.get("outputs") or {}),
        failure=StepFailure(f"step failed: {step_id}") if state == StepExecutionState.FAILED else None,
        skipped_reason=metadata.get("skipped_reason"),
    )
