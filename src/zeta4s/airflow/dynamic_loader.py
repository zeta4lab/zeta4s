"""Airflow DAG registration loader for zeta4s artifacts."""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import yaml

from zeta4s.airflow.dag_generator import generate_dag
from zeta4s.metastore.scheduler_snapshot import (
    load_scheduler_snapshot,
    scheduler_last_good_snapshot_path,
    scheduler_snapshot_path,
)
from zeta4s.project.loader import ProjectContext, load_project_context
from zeta4s.project.paths import normalize_relative_ref

logger = logging.getLogger(__name__)

ZETA4S_API_HOME = Path(os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))
SCHEDULER_SNAPSHOT_FILE = scheduler_snapshot_path(ZETA4S_API_HOME)
LAST_GOOD_SCHEDULER_SNAPSHOT_FILE = scheduler_last_good_snapshot_path(ZETA4S_API_HOME)


def _artifact_dir_name(artifact_id: str) -> str:
    return artifact_id.replace(":", "-", 1)


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml config must be a mapping: {path}")
    return data


def _iter_registered_items(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        logger.info("No zeta4s scheduler snapshot found: %s", path)
        return
    try:
        data = load_scheduler_snapshot(path)
    except Exception:
        if path == LAST_GOOD_SCHEDULER_SNAPSHOT_FILE:
            raise
        logger.exception("Invalid zeta4s scheduler snapshot, using last-good snapshot: %s", path)
        data = load_scheduler_snapshot(LAST_GOOD_SCHEDULER_SNAPSHOT_FILE)
    registrations = data.get("registrations") or []
    if not isinstance(registrations, list):
        raise ValueError(f"registrations must be a list: {path}")
    for registration in registrations:
        if not isinstance(registration, dict):
            raise ValueError(f"registration item must be a mapping: {path}")
        yield registration


def _registered_project_root(registration: dict[str, Any]) -> Path:
    project_name = registration.get("project_id")
    artifact_id = registration.get("artifact_id")
    if not project_name or not artifact_id:
        raise ValueError("registration requires project_id and artifact_id")
    return ZETA4S_API_HOME / "artifacts" / _artifact_dir_name(str(artifact_id)) / "projects" / str(project_name)


def _current_parsing_dag_id() -> str | None:
    try:
        from airflow.sdk import get_parsing_context

        context = get_parsing_context()
        dag_id = getattr(context, "dag_id", None)
        if dag_id:
            return str(dag_id)
    except Exception:
        pass
    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
    return dag_id.strip() if dag_id else None


def _registered_dag_id(registration: dict[str, Any], dag_spec: dict[str, Any]) -> str | None:
    dag_id = dag_spec.get("dag_id")
    if dag_id:
        return str(dag_id)
    project_id = registration.get("project_id")
    job_id = dag_spec.get("job_id")
    if project_id and job_id:
        return f"{project_id}__{job_id}"
    return None


def _registration_contains_dag(registration: dict[str, Any], dag_id: str) -> bool:
    dags = registration.get("dags") or []
    if not isinstance(dags, list):
        return False
    return any(isinstance(dag_spec, dict) and _registered_dag_id(registration, dag_spec) == dag_id for dag_spec in dags)


def _parse_registered_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        raise ValueError(f"registered_at must be an ISO datetime string: {value!r}")
    raw_value = value.strip()
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = raw_value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw_value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _registered_dag_configs(
    registration: dict[str, Any], target_dag_id: str | None = None
) -> Iterator[tuple[ProjectContext, Path, dict[str, Any], str | None]]:
    project_root = _registered_project_root(registration)
    project = load_project_context(project_root)
    project = replace(project, registered_at=_parse_registered_at(registration.get("registered_at")))
    profile_id = registration.get("profile_id")
    profile_id = str(profile_id) if profile_id else None
    dags = registration.get("dags") or []
    if not isinstance(dags, list):
        raise ValueError(f"registration dags must be a list: {project.project_id}")
    for dag_spec in dags:
        if not isinstance(dag_spec, dict):
            raise ValueError(f"dag spec must be a mapping: {project.project_id}")
        job_id = dag_spec.get("job_id")
        if not job_id:
            raise ValueError(f"dag spec requires job_id: {project.project_id}")
        if target_dag_id and _registered_dag_id(registration, dag_spec) != target_dag_id:
            continue
        config_ref = normalize_relative_ref(str(dag_spec.get("config") or f"jobs/{job_id}.yml"))
        config_path = (project.root / str(config_ref)).resolve()
        try:
            config_path.relative_to(project.root.resolve())
        except ValueError as e:
            raise ValueError(f"registered config escapes project root: {config_ref}") from e
        config = _load_yaml(config_path)
        if config.get("job_id") != job_id:
            raise ValueError(
                f"registered job_id mismatch: {project.project_id} {config_ref} "
                f"expected={job_id!r} actual={config.get('job_id')!r}"
            )
        yield project, config_path, config, profile_id


def register_dags(namespace: dict[str, Any], registration_file: Path = SCHEDULER_SNAPSHOT_FILE) -> set[str]:
    """Register Airflow DAGs declared by zeta4s API 서버."""
    registered_dag_ids: set[str] = set()
    target_dag_id = _current_parsing_dag_id()
    for registration in _iter_registered_items(registration_file):
        if target_dag_id and not _registration_contains_dag(registration, target_dag_id):
            continue
        try:
            config_items = list(_registered_dag_configs(registration, target_dag_id))
        except Exception:
            logger.exception("Invalid zeta4s DAG registration skipped")
            continue
        for project, config_path, config, profile_id in config_items:
            try:
                dag = generate_dag(config, project, profile_id=profile_id)
            except Exception:
                logger.exception("Failed to generate DAG from %s", config_path)
                continue
            if dag.dag_id in registered_dag_ids:
                logger.error("Duplicate DAG id %s from %s skipped", dag.dag_id, config_path)
                continue
            registered_dag_ids.add(dag.dag_id)
            namespace[dag.dag_id] = dag
    return registered_dag_ids
