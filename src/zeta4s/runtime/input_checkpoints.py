"""Checkpoint helpers for immutable rowset input consumption."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from zeta4s.metastore.contracts import StepCheckpoint, StepCheckpointRepository
from zeta4s.runtime.checkpoints import CheckpointCorruptionError
from zeta4s.runtime.rowset_models import RowsetStorage


def load_input_position(
    *,
    rowset,
    repository: StepCheckpointRepository | None,
    project_id: str | None,
    job_id: str | None,
    run_id: str | None,
    step_id: str | None,
    task_id: str | None,
    unit_id: str,
    expected_receipt: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if repository is None or rowset.descriptor.storage is not RowsetStorage.ICEBERG:
        return None
    identity = _identity(project_id, job_id, run_id, step_id, task_id)
    checkpoint = repository.latest_checkpoint(
        project_id=identity[0],
        job_id=identity[1],
        run_id=identity[2],
        step_id=identity[3],
        task_id=identity[4],
        unit_id=unit_id,
    )
    if checkpoint is None:
        return None
    descriptor = rowset.descriptor
    if (
        checkpoint.snapshot_id != descriptor.snapshot_id
        or checkpoint.table_identifier != descriptor.table_identifier
        or checkpoint.schema_fingerprint != descriptor.schema_fingerprint
    ):
        raise CheckpointCorruptionError("input checkpoint does not match the immutable rowset snapshot")
    position = checkpoint.continuation.get("input_position")
    if not isinstance(position, dict):
        raise CheckpointCorruptionError("input checkpoint is missing input_position")
    receipt = checkpoint.continuation.get("target_receipt")
    if not isinstance(receipt, dict):
        raise CheckpointCorruptionError("input checkpoint is missing target_receipt")
    if expected_receipt:
        mismatched = [key for key, value in expected_receipt.items() if receipt.get(key) != value]
        if mismatched:
            raise CheckpointCorruptionError(
                f"input checkpoint target receipt mismatch: {', '.join(sorted(mismatched))}"
            )
    return position


def append_input_checkpoint(
    *,
    rowset,
    repository: StepCheckpointRepository,
    project_id: str,
    job_id: str,
    run_id: str,
    step_id: str,
    task_id: str,
    unit_id: str,
    attempt: int,
    position: dict[str, Any],
    receipt: dict[str, Any],
) -> StepCheckpoint:
    descriptor = rowset.descriptor
    if descriptor.snapshot_id is None or descriptor.table_identifier is None:
        raise CheckpointCorruptionError("input checkpoint requires an Iceberg descriptor")
    latest = repository.latest_checkpoint(
        project_id=project_id,
        job_id=job_id,
        run_id=run_id,
        step_id=step_id,
        task_id=task_id,
        unit_id=unit_id,
    )
    checkpoint = StepCheckpoint(
        project_id=project_id,
        job_id=job_id,
        run_id=run_id,
        step_id=step_id,
        task_id=task_id,
        attempt=attempt,
        sequence=(latest.sequence + 1 if latest else 1),
        unit_id=unit_id,
        storage_uri=descriptor.uri,
        table_identifier=descriptor.table_identifier,
        snapshot_id=descriptor.snapshot_id,
        continuation={"input_position": dict(position), "target_receipt": dict(receipt)},
        schema_fingerprint=descriptor.schema_fingerprint,
        rows=descriptor.rows,
        bytes=descriptor.bytes,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    repository.append_checkpoint(checkpoint)
    return checkpoint


def runtime_checkpoint_identity(
    context: dict[str, Any], job_id: str | None
) -> tuple[str, str, str, str, str, int] | None:
    project_id = context.get("project_id")
    run_id = context.get("z4_run_id") or context.get("run_id")
    task_id = context.get("task_id")
    step_id = context.get("step_id") or task_id
    if not all((project_id, job_id, run_id, step_id, task_id)):
        return None
    return (
        str(project_id),
        str(job_id),
        str(run_id),
        str(step_id),
        str(task_id),
        int(context.get("attempt") or 1),
    )


def _identity(*values: str | None) -> tuple[str, str, str, str, str]:
    if not all(values):
        raise ValueError("input checkpoint requires project/job/run/step/task identity")
    return tuple(str(value) for value in values)  # type: ignore[return-value]


__all__ = ["append_input_checkpoint", "load_input_position", "runtime_checkpoint_identity"]
