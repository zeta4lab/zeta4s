"""Metastore-backed runtime metadata helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from zeta4s.metastore.factory import metastore_adapter_factory
from zeta4s.project.loader import validate_project_id
from zeta4s.project.step_graph import validate_job_id, validate_step_id

EPOCH = datetime(1970, 1, 1)
EXTRACT_HISTORY_EVENT_TYPE = "extract_history"
WATERMARK_STATE_TYPE = "watermark"


@dataclass(frozen=True)
class ExtractHistoryEvent:
    project_id: str
    job_id: str
    run_id: str
    step_id: str
    task_id: str
    output_name: str
    source_kind: str
    source_conn: str
    source_object: str
    mode: str
    watermark_column: str | None
    selected_from: datetime | None
    selected_to: datetime | None
    loaded_rows: int
    status: str
    error_message: str | None
    started_at: datetime
    ended_at: datetime | None


def record_extract_history(event: ExtractHistoryEvent) -> None:
    project_id = validate_project_id(_required_text(event.project_id, "extract_history.project_id"))
    job_id = validate_job_id(event.job_id, "extract_history.job_id")
    step_id = validate_step_id(event.step_id, "extract_history.step_id")
    payload = _json_ready(asdict(event))
    metastore_adapter_factory().step_event_repository.record_event(
        project_id=project_id,
        job_id=job_id,
        run_id=_required_text(event.run_id, "extract_history.run_id"),
        step_id=step_id,
        task_id=_required_text(event.task_id, "extract_history.task_id"),
        event_type=EXTRACT_HISTORY_EVENT_TYPE,
        status=_required_text(event.status, "extract_history.status"),
        event=payload,
        created_at=event.started_at.isoformat(),
    )


def _required_text(value: str | None, label: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"{label} is required")
    return str(value).strip()


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
