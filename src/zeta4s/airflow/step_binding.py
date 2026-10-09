"""Generic Airflow task binding for step graph steps.

모든 step type 은 동일하게 `run_core_step` facade 를 실행하는 단일 PythonOperator 로
바인딩된다. type 별 분기는 없다 — 실행 축 dispatch 는 core `built_in_step_executor` 가
task 프로세스 안에서 수행한다. 이 모듈은 DAG parse 시점에 heavy runtime dependency 를
import 하지 않는 바인딩 경계다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator

from zeta4s.airflow.operators import run_core_step
from zeta4s.project.execution_plan import ExecutionPlan, ExecutionStep
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import StepGraphStep


@dataclass(frozen=True)
class StepGraphTaskBinding:
    roots: tuple[object, ...]
    terminals: tuple[object, ...]
    tasks: tuple[object, ...] = ()


@dataclass(frozen=True)
class StepBindingContext:
    project: ProjectContext
    plan: ExecutionPlan
    plan_step: ExecutionStep
    step: StepGraphStep
    profile_id: str | None = None


def single_task_binding(task: object) -> StepGraphTaskBinding:
    return StepGraphTaskBinding(roots=(task,), terminals=(task,), tasks=(task,))


def core_step_operator(
    ctx: StepBindingContext,
    *,
    task_id: str | None = None,
) -> PythonOperator:
    retry = ctx.plan_step.flow.retry or {}
    timeout = ctx.plan_step.flow.timeout or {}
    return PythonOperator(
        task_id=task_id or ctx.step.id,
        python_callable=run_core_step,
        op_kwargs={
            "step_config": ctx.step.model_dump(exclude_none=True),
            "job_config": _job_config_from_plan(ctx.plan),
            "project_config": _project_config(ctx.project),
            "profile": ctx.profile_id,
        },
        pool=ctx.step.pool,
        retries=max(0, int(retry.get("max_attempts") or 1) - 1),
        retry_delay=timedelta(seconds=max(0, int(retry.get("delay_seconds") or 0))),
        execution_timeout=(timedelta(seconds=max(1, int(timeout["seconds"]))) if timeout else None),
    )


def _job_config_from_plan(plan: ExecutionPlan) -> dict:
    config = {
        "job_id": plan.job_id,
        "schedule": plan.schedule.model_dump(exclude_none=True) if plan.schedule else None,
        "steps": [step.step.model_dump(exclude_none=True) for step in plan.steps],
    }
    if plan.execution is not None:
        config["execution"] = plan.execution.model_dump(exclude_none=True)
    if plan.defaults is not None:
        config["defaults"] = plan.defaults.model_dump(exclude_none=True)
    return config


def _project_config(project: ProjectContext) -> dict:
    config = {
        "project_id": project.project_id,
        "root": str(project.root),
        "jobs_dir": str(project.jobs_dir),
        "assets_dir": str(project.assets_dir),
        "dbt_dir": str(project.dbt_dir),
        "timezone": project.timezone,
    }
    if project.display_name is not None:
        config["display_name"] = project.display_name
    if project.registered_at is not None:
        config["registered_at"] = project.registered_at.isoformat()
    return config
