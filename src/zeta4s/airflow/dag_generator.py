"""Step graph contract 를 Airflow DAG 로 동적 생성."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from pydantic import BaseModel, ValidationError

from zeta4s.project.pools import execution_step_pool_name
from zeta4s.airflow.step_binding import (
    StepBindingContext,
    StepGraphTaskBinding,
    core_step_operator,
    single_task_binding,
)
from zeta4s.airflow.task_result_notes import sync_task_result_notes_callback
from zeta4s.project.execution_plan import ExecutionPlan, ExecutionStep, build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import StepGraphJob, StepGraphStep
from zeta4s.project.step_types import register_installed_step_types

DEFAULT_ARGS = {
    "owner": "zeta4s",
    "start_date": datetime(2026, 1, 1, tzinfo=timezone.utc),
}
DAG_MAX_ACTIVE_RUNS = 1


def _dag_id(project: ProjectContext, job_id: str) -> str:
    return f"{project.project_id}__{job_id}"


def _dag_tags(project: ProjectContext, job_type: str) -> list[str]:
    return ["zeta4s", job_type, f"project:{project.project_id}"]


def _validate_or_raise(model_cls: type[BaseModel], config: dict):
    """Pydantic 검증 + 친절한 에러 포맷."""
    try:
        return model_cls.model_validate(config)
    except ValidationError as e:
        formatted = "\n".join(f"  - {'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in e.errors())
        raise ValueError(
            f"yaml schema 검증 실패 [{model_cls.__name__}, job_id={config.get('job_id', '<unknown>')}]:\n{formatted}"
        ) from e


def _dag_default_args(project: ProjectContext, schedule_timezone: str) -> dict:
    # Airflow 는 timezone-aware start_date 의 tzinfo 를 DAG timezone 으로 쓰고 cron/interval
    # timetable 을 그 timezone 에서 해석한다.
    default_args = dict(DEFAULT_ARGS)
    start_date = project.registered_at or default_args["start_date"]
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    default_args["start_date"] = start_date.astimezone(ZoneInfo(schedule_timezone))
    return default_args


def _make_dag(
    job_id: str,
    schedule,
    tags: list[str],
    project: ProjectContext,
    *,
    schedule_timezone: str,
    paused: bool = False,
) -> DAG:
    dag = DAG(
        dag_id=job_id,
        default_args=_dag_default_args(project, schedule_timezone),
        schedule=schedule,
        catchup=False,
        max_active_runs=DAG_MAX_ACTIVE_RUNS,
        is_paused_upon_creation=paused,
        tags=tags,
        on_success_callback=sync_task_result_notes_callback,
        on_failure_callback=sync_task_result_notes_callback,
    )
    _assert_dag_runtime_invariants(dag)
    return dag


def _assert_dag_runtime_invariants(dag: DAG) -> None:
    if dag.max_active_runs != DAG_MAX_ACTIVE_RUNS:
        raise ValueError(
            f"zeta4s generated DAG must use max_active_runs={DAG_MAX_ACTIVE_RUNS}: "
            f"{dag.dag_id} has max_active_runs={dag.max_active_runs}"
        )
    if dag.catchup:
        raise ValueError(f"zeta4s generated DAG must use catchup=False: {dag.dag_id}")


def _resolve_step_graph_schedule(schedule):
    if schedule is None:
        return None
    if schedule.cron is not None:
        return schedule.cron
    return timedelta(seconds=schedule.interval_seconds)


def _make_job_success_emit_task() -> EmptyOperator:
    return EmptyOperator(
        task_id="system.emit_job_success",
        trigger_rule="none_failed",
    )


def _step_graph_trigger_rule(step: StepGraphStep) -> str:
    if step.when and step.when.success and step.when.failed:
        raise ValueError(f"step graph when.success and when.failed cannot be used together: {step.id}")
    if step.when and step.when.failed:
        if step.join is not None:
            raise ValueError(f"step graph when.failed cannot be combined with join.rule yet: {step.id}")
        return "one_failed"
    return step.join.rule if step.join is not None else "all_success"


def _apply_step_graph_trigger_rule(binding: StepGraphTaskBinding, step: StepGraphStep) -> StepGraphTaskBinding:
    trigger_rule = _step_graph_trigger_rule(step)
    if trigger_rule == "all_success":
        return binding
    for root in binding.roots:
        root.trigger_rule = trigger_rule
    return binding


def _step_from_plan_step(project: ProjectContext, plan: ExecutionPlan, plan_step: ExecutionStep) -> StepGraphStep:
    pool_name = execution_step_pool_name(project.project_id, plan, plan_step)
    if pool_name != plan_step.step.pool:
        return plan_step.step.model_copy(update={"pool": pool_name})
    return plan_step.step


def _make_step_graph_binding(
    project: ProjectContext,
    plan: ExecutionPlan,
    plan_step: ExecutionStep,
    *,
    profile_id: str | None = None,
) -> StepGraphTaskBinding:
    step = _step_from_plan_step(project, plan, plan_step)
    ctx = StepBindingContext(
        project=project,
        plan=plan,
        plan_step=plan_step,
        step=step,
        profile_id=profile_id,
    )
    # 모든 step type 은 동일한 generic 바인딩을 쓴다. type 별 실행 dispatch 는 task
    # 프로세스 안 core built_in_step_executor 가 수행하고, schema 검증은 StepGraphJob
    # pydantic 검증이 이미 마쳤다. 여기서는 flow control 특례(trigger_rule)만 적용한다.
    binding = single_task_binding(core_step_operator(ctx))
    return _apply_step_graph_trigger_rule(binding, step)


def _generate_step_graph_dag(config: dict, project: ProjectContext, *, profile_id: str | None = None) -> DAG:
    # 설치된 외부 step type 을 StepGraphJob 검증 이전에 등록한다. scheduler 이미지에 설치된
    # zeta4s.step_types 플러그인을 discover 해 외부 type membership 을 통과시킨다.
    register_installed_step_types()
    job = _validate_or_raise(StepGraphJob, config)
    plan = build_step_graph_execution_plan(job)
    return _generate_execution_plan_dag(project, plan, profile_id=profile_id)


def _generate_execution_plan_dag(project: ProjectContext, plan: ExecutionPlan, *, profile_id: str | None = None) -> DAG:
    dag = _make_dag(
        _dag_id(project, plan.job_id),
        _resolve_step_graph_schedule(plan.schedule),
        tags=_dag_tags(project, "step-graph"),
        project=project,
        schedule_timezone=plan.schedule.effective_timezone(project.timezone) if plan.schedule else project.timezone,
        paused=plan.schedule.paused if plan.schedule else False,
    )
    with dag:
        tasks = {
            plan_step.id: _make_step_graph_binding(project, plan, plan_step, profile_id=profile_id)
            for plan_step in plan.steps
        }
        for edge in plan.control_edges:
            downstream = tasks[edge.downstream_id]
            for upstream in tasks[edge.upstream_id].terminals:
                for root in downstream.roots:
                    upstream >> root
        terminal_tasks = [
            terminal
            for plan_step in plan.steps
            if plan_step.id in plan.terminal_step_ids
            for terminal in tasks[plan_step.id].terminals
        ]
        success_task = _make_job_success_emit_task()
        _wire_tasks_to_downstream(terminal_tasks, success_task)
    return dag


def _wire_tasks_to_downstream(tasks, downstream) -> None:
    for task in tasks:
        task >> downstream


def generate_dag(config: dict, project: ProjectContext, *, profile_id: str | None = None) -> DAG:
    """YAML config dict 로부터 step graph Airflow DAG 를 생성한다."""
    if isinstance(config.get("steps"), list):
        return _generate_step_graph_dag(config, project, profile_id=profile_id)
    raise ValueError("step graph job requires steps[]")
