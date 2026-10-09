"""Runtime helpers for metastore-backed step state."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from zeta4s.metastore.factory import metastore_adapter_factory
from zeta4s.runtime.project_metadata import EPOCH

WATERMARK_STATE_TYPE = "watermark"


def json_state_value(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def watermark_state_key(*, output_name: str, watermark_column: str) -> str:
    return f"watermark:{output_name}:{watermark_column}"


def get_step_watermark(
    *,
    project_id: str | None,
    job_id: str | None,
    step_id: str | None,
    output_name: str,
    watermark_column: str,
) -> datetime:
    project, job, step = _required_step_state_identity(project_id=project_id, job_id=job_id, step_id=step_id)
    state = metastore_adapter_factory().step_state_repository.get_state(
        project_id=project,
        job_id=job,
        step_id=step,
        state_key=watermark_state_key(output_name=output_name, watermark_column=watermark_column),
    )
    if not state:
        return EPOCH
    state_value = state.get("state_value")
    value = state_value.get("value") if isinstance(state_value, dict) else state_value
    return parse_watermark_value(value)


def set_step_watermark(
    *,
    project_id: str | None,
    job_id: str | None,
    step_id: str | None,
    output_name: str,
    watermark_column: str,
    watermark_value: Any,
    run_id: str,
) -> None:
    project, job, step = _required_step_state_identity(project_id=project_id, job_id=job_id, step_id=step_id)
    metastore_adapter_factory().step_state_repository.upsert_state(
        project_id=project,
        job_id=job,
        step_id=step,
        state_key=watermark_state_key(output_name=output_name, watermark_column=watermark_column),
        state_value={
            "value": json_state_value(watermark_value),
            "column": watermark_column,
            "output": output_name,
        },
        state_type=WATERMARK_STATE_TYPE,
        run_id=run_id,
    )


def parse_watermark_value(value: Any) -> datetime:
    if value is None:
        return EPOCH
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    raise ValueError(f"extract watermark state value must be datetime or ISO string: {value!r}")


def _required_step_state_identity(
    *,
    project_id: str | None,
    job_id: str | None,
    step_id: str | None,
) -> tuple[str, str, str]:
    if not project_id or not job_id or not step_id:
        raise ValueError("extract watermark state requires project_id, job_id, and step_id")
    return project_id, job_id, step_id
