"""Prefect backend deployment and execution projection helpers."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
import json
import os
from typing import Any
import urllib.error
import urllib.request

from prefect import Flow, Task, flow, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId, LogFilter
from prefect.client.schemas.sorting import LogSort, TaskRunSort
from prefect.concurrency.sync import concurrency
from prefect.context import get_run_context
from prefect.deployments.runner import EntrypointType
from prefect.exceptions import ObjectNotFound
from prefect.schedules import Cron, Interval
from prefect.states import Cancelled
from prefect.utilities.asyncutils import run_coro_as_sync

from zeta4s.project.execution_plan import ExecutionPlan, ExecutionStep
from zeta4s.project.pools import execution_step_pool_name
from zeta4s.prefect.contracts import (
    ScheduleIdentity,
    ScheduleState,
)


ProjectedStepCallable = Callable[[str, str], Any]
ProjectedFinalizeCallable = Callable[[str], Any]


def prefect_task_policy(step: ExecutionStep) -> dict[str, int | None]:
    """Map canonical step policy to one Prefect-native task policy."""
    retry = step.flow.retry or {}
    timeout = step.flow.timeout or {}
    return {
        "retries": max(0, int(retry.get("max_attempts") or 1) - 1),
        "retry_delay_seconds": max(0, int(retry.get("delay_seconds") or 0)),
        "timeout_seconds": max(1, int(timeout["seconds"])) if timeout else None,
    }


def build_step_tasks(
    plan: ExecutionPlan,
    execute_step: ProjectedStepCallable,
    *,
    project_id: str,
) -> dict[str, Task]:
    """Build exactly one Prefect task per canonical execution step."""
    return {
        step.id: _build_step_task(
            step,
            execute_step,
            pool_name=execution_step_pool_name(project_id, plan, step),
        )
        for step in plan.steps
    }


def build_projected_flow(
    plan: ExecutionPlan,
    execute_step: ProjectedStepCallable,
    finalize_run: ProjectedFinalizeCallable,
    *,
    project_id: str,
) -> Flow:
    """Project control edges while keeping execution and finalization in core facades."""
    step_tasks = build_step_tasks(plan, execute_step, project_id=project_id)

    @flow(name="zeta4s-scheduled-job", persist_result=False, retries=3)
    def projected(run_id: str) -> Any:
        futures = {}
        for step in plan.steps:
            wait_for = [futures[upstream_id] for upstream_id in plan.upstream_ids_by_step[step.id]]
            futures[step.id] = step_tasks[step.id].submit(run_id, wait_for=wait_for)
        for future in futures.values():
            future.wait()
        return finalize_run(run_id)

    return projected


@flow(name="zeta4s-scheduled-job", persist_result=False, retries=3)
def scheduled_job_flow(
    project_id: str,
    artifact_id: str,
    job_id: str,
    profile: str,
    projection: dict[str, Any],
    parameters: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> Any:
    """Importable worker entrypoint that projects a server-validated canonical job."""
    scheduler_run_id = str(get_run_context().flow_run.id)
    run_id = run_id or scheduler_run_id
    run_parameters = dict(parameters or {})

    def execute_scheduled_step(step_id: str, current_run_id: str) -> Any:
        attempt, _adapter_attempt = _current_task_attempts()
        return _post_runtime(
            "/internal/v1/runtime/steps/execute",
            {
                "project_id": project_id,
                "artifact_id": artifact_id,
                "profile_id": profile,
                "job_id": job_id,
                "step_id": step_id,
                "run_id": current_run_id,
                "attempt": attempt,
                "parameters": run_parameters,
            },
        )

    steps = projection.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ValueError("Prefect scheduler projection must contain at least one step")
    tasks = {
        str(step["id"]): _build_projected_step_task(step, execute_scheduled_step)
        for step in steps
        if isinstance(step, dict)
    }
    if len(tasks) != len(steps):
        raise ValueError("Prefect scheduler projection contains an invalid step")
    futures = {}
    for step in steps:
        step_id = str(step["id"])
        upstream_ids = step.get("upstream_ids") or []
        wait_for = [futures[str(upstream_id)] for upstream_id in upstream_ids]
        futures[step_id] = tasks[step_id].submit(run_id, wait_for=wait_for)
    for future in futures.values():
        future.wait()
    result = _post_runtime(
        "/internal/v1/runtime/runs/finalize",
        {
            "project_id": project_id,
            "artifact_id": artifact_id,
            "profile_id": profile,
            "job_id": job_id,
            "run_id": run_id,
        },
    )
    if result["state"] == "failed":
        raise RuntimeError(f"scheduled job failed: {project_id}/{job_id}")
    return result


def deploy_prefect_job(
    *,
    identity: ScheduleIdentity,
    plan: ExecutionPlan,
    project_timezone: str,
    artifact_id: str,
) -> ScheduleState:
    schedule = plan.schedule
    paused = bool(schedule.paused) if schedule is not None else False
    deployment = scheduled_job_flow.to_deployment(
        name=identity.key,
        schedule=_prefect_schedule(schedule, project_timezone),
        paused=paused,
        parameters={
            "project_id": identity.project_id,
            "artifact_id": artifact_id,
            "job_id": identity.job_id,
            "profile": identity.profile,
            "projection": _scheduler_projection(identity.project_id, plan),
            "parameters": {},
            "run_id": None,
        },
        tags=["zeta4s", f"project:{identity.project_id}"],
        work_pool_name="zeta4s-internal",
        entrypoint_type=EntrypointType.MODULE_PATH,
    )
    return ScheduleState(identity, str(deployment.apply()), paused)


def delete_prefect_job(identity: ScheduleIdentity) -> bool:
    return run_coro_as_sync(_delete_deployment(identity))


def _build_step_task(
    step: ExecutionStep,
    execute_step: ProjectedStepCallable,
    *,
    pool_name: str | None,
) -> Task:
    def run(run_id: str) -> Any:
        if pool_name:
            with concurrency(pool_name, occupy=1, strict=True):
                return execute_step(step.id, run_id)
        return execute_step(step.id, run_id)

    return task(
        name=step.id,
        persist_result=False,
        **prefect_task_policy(step),
    )(run)


def _build_projected_step_task(
    step: dict[str, Any],
    execute_step: ProjectedStepCallable,
) -> Task:
    step_id = str(step["id"])
    pool_name = str(step["pool_name"]) if step.get("pool_name") else None

    def run(run_id: str) -> Any:
        if pool_name:
            with concurrency(pool_name, occupy=1, strict=True):
                return execute_step(step_id, run_id)
        return execute_step(step_id, run_id)

    retries = max(0, int(step.get("retries") or 0))
    retry_delay_seconds = max(0, int(step.get("retry_delay_seconds") or 0))
    timeout_value = step.get("timeout_seconds")
    timeout_seconds = max(1, int(timeout_value)) if timeout_value is not None else None
    return task(
        name=step_id,
        persist_result=False,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
    )(run)


def _scheduler_projection(project_id: str, plan: ExecutionPlan) -> dict[str, Any]:
    upstream_ids = plan.upstream_ids_by_step
    steps = []
    for step in plan.steps:
        policy = prefect_task_policy(step)
        steps.append(
            {
                "id": step.id,
                "type": step.type,
                "upstream_ids": list(upstream_ids[step.id]),
                "pool_name": execution_step_pool_name(project_id, plan, step),
                **policy,
            }
        )
    return {"schema_version": 1, "steps": steps}


def _post_runtime(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    base_url = os.environ["ZETA4S_API_INTERNAL_URL"].rstrip("/")
    token = os.environ["ZETA4S_RUNTIME_INTERNAL_TOKEN"]
    request = urllib.request.Request(
        base_url + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=86400) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"zeta4s-api {path} failed: HTTP {error.code}: {detail}") from error
    if not isinstance(result, dict):
        raise RuntimeError(f"zeta4s-api {path} returned a non-object response")
    return result


def _prefect_schedule(schedule, project_timezone: str):
    if schedule is None:
        return None
    timezone = schedule.effective_timezone(project_timezone)
    if schedule.cron is not None:
        return Cron(
            schedule.cron,
            timezone=timezone,
            active=not schedule.paused,
            slug="canonical",
        )
    return Interval(
        timedelta(seconds=schedule.interval_seconds),
        timezone=timezone,
        active=not schedule.paused,
        slug="canonical",
    )


async def _read_deployment(identity: ScheduleIdentity):
    async with get_client() as client:
        try:
            return await client.read_deployment_by_name(f"zeta4s-scheduled-job/{identity.key}")
        except ObjectNotFound:
            return None


async def _delete_deployment(identity: ScheduleIdentity) -> bool:
    deployment = await _read_deployment(identity)
    if deployment is None:
        return False
    async with get_client() as client:
        await client.delete_deployment(deployment.id)
    return True


def _current_task_attempts() -> tuple[int, int]:
    task_run = getattr(get_run_context(), "task_run", None)
    return max(1, int(getattr(task_run, "run_count", 1) or 1)), 1


async def _sync_prefect_concurrency_limits(project_root: str) -> None:
    from pathlib import Path

    from zeta4s.project.loader import load_project_context
    from zeta4s.project.pools import project_pool_payloads

    project = load_project_context(Path(project_root))
    payloads = project_pool_payloads(project.project_id, Path(project_root))
    async with get_client() as client:
        for payload in payloads:
            await client.upsert_global_concurrency_limit_by_name(
                name=payload["name"],
                limit=payload["slots"],
            )


def sync_prefect_concurrency_limits(project_root: str) -> None:
    return run_coro_as_sync(_sync_prefect_concurrency_limits(project_root))


def trigger_prefect_deployment(
    deployment_name: str,
    *,
    parameters: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    run_id: str | None = None,
) -> str:
    from prefect.deployments import run_deployment

    flow_run = run_deployment(
        name=deployment_name,
        parameters={"parameters": dict(parameters or {}), "run_id": run_id},
        idempotency_key=idempotency_key,
        timeout=0,
    )
    return str(flow_run.id)


def read_prefect_flow_run(run_id: str):
    import uuid

    async def fetch():
        async with get_client() as client:
            return await client.read_flow_run(uuid.UUID(run_id))

    return run_coro_as_sync(fetch())


def read_prefect_task_runs(run_id: str):
    import uuid

    async def fetch():
        async with get_client() as client:
            return await client.read_task_runs(
                flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[uuid.UUID(run_id)])),
                sort=TaskRunSort.EXPECTED_START_TIME_ASC,
            )

    return run_coro_as_sync(fetch())


def read_prefect_logs(run_id: str, *, limit: int | None = None):
    import uuid

    async def fetch():
        async with get_client() as client:
            return await client.read_logs(
                log_filter=LogFilter(flow_run_id={"any_": [uuid.UUID(run_id)]}),
                limit=limit,
                sort=LogSort.TIMESTAMP_ASC,
            )

    return run_coro_as_sync(fetch())


def cancel_prefect_flow_run(run_id: str) -> None:
    async def cancel():
        async with get_client() as client:
            await client.set_flow_run_state(run_id, Cancelled(), force=True)

    run_coro_as_sync(cancel())


def prefect_run_state(
    run: dict[str, Any],
    timezone: str | None = None,
    format_display_time: Callable[[Any, str | None], str | None] = lambda *_: None,
) -> dict[str, Any]:
    import uuid
    from prefect import get_client

    async def fetch() -> Any:
        async with get_client() as client:
            return await client.read_flow_run(uuid.UUID(run["run_id"]))

    flow_run = run_coro_as_sync(fetch())
    if not flow_run:
        return {"airflow_state": "not_found"}

    state_map = {
        "COMPLETED": "success",
        "FAILED": "failed",
        "CRASHED": "failed",
        "CANCELLED": "failed",
        "RUNNING": "running",
        "PENDING": "queued",
        "SCHEDULED": "queued",
    }
    state_name = flow_run.state.type.value.upper() if flow_run.state else "UNKNOWN"
    airflow_state = state_map.get(state_name, "running")

    start_date = flow_run.start_time
    end_date = flow_run.end_time

    return {
        "airflow_state": airflow_state,
        "airflow_start_date": start_date.isoformat() if start_date else None,
        "airflow_end_date": end_date.isoformat() if end_date else None,
        "airflow_duration_seconds": flow_run.total_run_time.total_seconds() if flow_run.total_run_time else 0,
        "airflow_start_date_display": format_display_time(start_date, timezone),
        "airflow_end_date_display": format_display_time(end_date, timezone),
    }
