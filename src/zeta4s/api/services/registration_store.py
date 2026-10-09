"""Deployment registration store and scheduler snapshot publisher."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from zeta4s.api.services.artifact_store import ZETA4S_API_HOME
from zeta4s.api.services.locks import registration_lock
from zeta4s.metastore.contracts import require_scheduler_backend
from zeta4s.metastore.factory import metastore_adapter_factory
from zeta4s.metastore.scheduler_snapshot import publish_scheduler_snapshot, scheduler_snapshot_path


def registration_path(home: Path = ZETA4S_API_HOME) -> Path:
    return scheduler_snapshot_path(home)


def load_registrations(home: Path = ZETA4S_API_HOME) -> dict[str, Any]:
    repository = metastore_adapter_factory().deployment_repository
    return {"registrations": [item.as_scheduler_item() for item in repository.list_active()]}


def publish_current_scheduler_snapshot(home: Path = ZETA4S_API_HOME) -> Path:
    from zeta4s.airflow.dag_source import publish_airflow_dag_sources

    data = load_registrations(home)
    registrations = [item for item in data.get("registrations", []) if item["scheduler_backend"] == "airflow"]
    path = publish_scheduler_snapshot(registrations=registrations, home=home)
    publish_airflow_dag_sources(registrations, home=home)
    return path


def upsert_project_registration(
    *,
    project_id: str,
    artifact_id: str,
    profile_id: str,
    scheduler_backend: str,
    dags: list[dict[str, Any]],
    home: Path = ZETA4S_API_HOME,
) -> Path:
    scheduler_backend = require_scheduler_backend(scheduler_backend)
    with registration_lock(home=home):
        repository = metastore_adapter_factory().deployment_repository
        repository.upsert_active(
            project_id=project_id,
            artifact_id=artifact_id,
            profile_id=profile_id,
            scheduler_backend=scheduler_backend,
            dags=dags,
        )
        return publish_current_scheduler_snapshot(home)


def remove_project_registration(
    project_id: str,
    *,
    home: Path = ZETA4S_API_HOME,
) -> tuple[Path, dict[str, Any] | None]:
    with registration_lock(home=home):
        repository = metastore_adapter_factory().deployment_repository
        removed = repository.remove_active(project_id)
        path = publish_current_scheduler_snapshot(home)
        return path, removed.as_scheduler_item() if removed else None


def artifact_is_registered(
    artifact_id: str,
    *,
    home: Path = ZETA4S_API_HOME,
) -> bool:
    return metastore_adapter_factory().deployment_repository.artifact_is_active(artifact_id)
