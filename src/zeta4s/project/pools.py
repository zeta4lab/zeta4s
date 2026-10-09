"""Project-scoped runtime pool contract helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import yaml

from zeta4s.project.loader import load_project_context, project_manifest_path
from zeta4s.project.resource_names import encode_project_resource_name
from zeta4s.project.step_graph import PROJECT_POOL_STAGES, STEP_TYPE_POOL_STAGES, step_graph_config_paths

PROJECT_POOL_FALLBACK_SLOTS = {
    "extract": 4,
    "stage": 4,
    "transform": 4,
    "write": 4,
}
PROJECT_POOL_MAX_SLOTS = {
    "extract": 8,
    "stage": 8,
    "transform": 8,
    "write": 8,
}


def project_pool_name(project_name: str, stage: str) -> str:
    if stage not in PROJECT_POOL_STAGES:
        raise ValueError(f"unknown project pool stage: {stage}")
    return f"z4p_{encode_project_resource_name(project_name)}_{stage}"


def project_pool_names(project_name: str) -> set[str]:
    """Return all zeta4s-owned pool names for one project."""
    return {project_pool_name(project_name, stage) for stage in PROJECT_POOL_STAGES}


def pool_stage_for_task(stage: str) -> str:
    if stage in PROJECT_POOL_STAGES:
        return stage
    raise ValueError(f"unknown pool task stage: {stage}")


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def step_graph_project_pool_stage(step_type: str, step: Any | None = None) -> str | None:
    return STEP_TYPE_POOL_STAGES.get(step_type)


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml config must be a mapping: {path}")
    return data


def _iter_job_dicts(jobs_dir: Path) -> Iterable[dict[str, Any]]:
    for path in step_graph_config_paths(jobs_dir):
        yield _load_yaml(path)


def _step_graph_default_pools(config: dict[str, Any]) -> dict[str, Any]:
    defaults = config.get("defaults") or {}
    pools = defaults.get("pools") if isinstance(defaults, dict) else None
    return pools if isinstance(pools, dict) else {}


def _step_graph_project_pool_stages(config: dict[str, Any]) -> list[str]:
    steps = config.get("steps") or []
    if not isinstance(steps, list):
        return []
    default_pools = _step_graph_default_pools(config)
    stages: list[str] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_type = str(step.get("type") or "")
        stage = step_graph_project_pool_stage(step_type, step)
        if step.get("pool") or default_pools.get(step_type) or (stage and default_pools.get(stage)):
            continue
        if stage:
            stages.append(stage)
    return stages


def _bounded_slots(stage: str, count: int) -> int:
    if count <= 0:
        return PROJECT_POOL_FALLBACK_SLOTS[stage]
    return max(1, min(count, PROJECT_POOL_MAX_SLOTS[stage]))


def _auto_project_pool_slots(project_root: Path, stage: str) -> int:
    project = load_project_context(project_root)
    configs = list(_iter_job_dicts(project.jobs_dir))
    step_stage_count = sum(
        1 for config in configs for step_stage in _step_graph_project_pool_stages(config) if step_stage == stage
    )
    if stage in PROJECT_POOL_STAGES:
        return _bounded_slots(stage, step_stage_count)
    raise ValueError(f"unknown project pool stage: {stage}")


def required_project_pool_stages(project_root: Path) -> set[str]:
    project = load_project_context(project_root)
    stages: set[str] = set()
    for config in _iter_job_dicts(project.jobs_dir):
        stages.update(_step_graph_project_pool_stages(config))
    return stages


def custom_pool_overrides(project_root: Path) -> set[str]:
    project = load_project_context(project_root)
    overrides: set[str] = set()
    for config in _iter_job_dicts(project.jobs_dir):
        execution = config.get("execution") or {}
        pools = execution.get("pools") if isinstance(execution, dict) else None
        if not isinstance(pools, dict):
            pools = {}
        overrides.update(str(value) for value in pools.values() if value)
        defaults = config.get("defaults") or {}
        default_pools = defaults.get("pools") if isinstance(defaults, dict) else None
        if isinstance(default_pools, dict):
            overrides.update(str(value) for value in default_pools.values() if value)
        steps = config.get("steps") or []
        if isinstance(steps, list):
            overrides.update(str(step.get("pool")) for step in steps if isinstance(step, dict) and step.get("pool"))
    return overrides


def _project_pool_slots(project_root: Path, stage: str) -> int:
    manifest_path = project_manifest_path(project_root)
    if manifest_path is None:
        raise ValueError(f"project.yml is required: {project_root}")
    manifest = _load_yaml(manifest_path)
    runtime = manifest.get("runtime") or {}
    pools = runtime.get("pools") if isinstance(runtime, dict) else None
    stage_config = pools.get(stage) if isinstance(pools, dict) else None
    slots = stage_config.get("slots") if isinstance(stage_config, dict) else None
    return int(slots) if slots is not None else _auto_project_pool_slots(project_root, stage)


def project_pool_payloads(project_name: str, project_root: Path) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for stage in PROJECT_POOL_STAGES:
        if stage not in required_project_pool_stages(project_root):
            continue
        payloads.append(
            {
                "name": project_pool_name(project_name, stage),
                "stage": stage,
                "slots": _project_pool_slots(project_root, stage),
                "description": f"ZETA4S project {project_name} {stage} concurrency",
            }
        )
    return payloads


def required_runtime_pool_names(project_name: str, project_root: Path) -> set[str]:
    stages = required_project_pool_stages(project_root)
    return {
        *(project_pool_name(project_name, stage) for stage in stages),
        *custom_pool_overrides(project_root),
    }


def resolve_pool(project_name: str, job, stage: str) -> str:
    pools: dict[str, Any] = {}
    execution = getattr(job, "execution", None)
    if execution:
        if isinstance(execution, dict):
            pools = execution.get("pools", {}) or {}
        else:
            pools = getattr(execution, "pools", {}) or {}
    override = pools.get(stage)
    if override:
        return str(override)
    return project_pool_name(project_name, pool_stage_for_task(stage))


def execution_step_pool_name(project_name: str, job: Any, step: Any) -> str | None:
    """Resolve one execution step to its explicit or automatic project pool."""
    runtime_pool = _field(_field(step, "runtime"), "pool")
    if runtime_pool:
        return str(runtime_pool)
    step_type = str(_field(step, "type") or "")
    stage = step_graph_project_pool_stage(step_type, _field(step, "step"))
    return resolve_pool(project_name, job, stage) if stage else None
