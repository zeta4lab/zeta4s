"""zeta4s run metadata store."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from zeta4s.common.time_display import display_timezone_name, format_display_time, local_run_timestamp, utc_now
from zeta4s.metastore.factory import metastore_adapter_factory


def new_run_id(project_id: str, job_id: str, timezone_name: str | None = None) -> str:
    timestamp = local_run_timestamp(timezone_name=timezone_name)
    return f"{project_id}__{job_id}__{timestamp}__{uuid4().hex[:8]}"


def run_display_fields(run: dict[str, Any], timezone_name: str | None = None) -> dict[str, Any]:
    created_at = run.get("created_at")
    if not created_at:
        return {}
    return {"created_at_display": format_display_time(str(created_at), timezone_name)}


def new_run_metadata_base(timezone_name: str | None = None) -> dict[str, str]:
    now = utc_now()
    return {
        "created_at": now.isoformat(),
        "display_timezone": display_timezone_name(timezone_name),
    }


def create_run(run: dict[str, Any]) -> None:
    metastore_adapter_factory().run_metadata_repository.create_run(run)


def get_run(run_id: str) -> dict[str, Any] | None:
    return metastore_adapter_factory().run_metadata_repository.get_run(run_id)


def list_runs(
    limit: int = 30,
    project_id: str | None = None,
    job_id: str | None = None,
) -> list[dict[str, Any]]:
    return metastore_adapter_factory().run_metadata_repository.list_runs(
        project_id=project_id,
        job_id=job_id,
        limit=limit,
    )
