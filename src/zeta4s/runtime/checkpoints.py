"""Ordered Iceberg snapshot and metastore checkpoint commit boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from zeta4s.metastore.contracts import StepCheckpoint, StepCheckpointRepository
from zeta4s.runtime.rowset_models import RowsetIdentity


class CheckpointCorruptionError(RuntimeError):
    pass


def commit_step_checkpoint(
    session: Any,
    continuation: Mapping[str, Any],
    repository: StepCheckpointRepository,
) -> StepCheckpoint:
    descriptor = session.checkpoint(continuation)
    identity: RowsetIdentity = session.identity
    if descriptor.snapshot_id is None or descriptor.table_identifier is None:
        raise CheckpointCorruptionError("Iceberg checkpoint descriptor is incomplete")
    checkpoint = StepCheckpoint(
        project_id=identity.project_id,
        job_id=identity.job_id,
        run_id=identity.run_id,
        step_id=identity.step_id,
        task_id=identity.step_id,
        attempt=identity.attempt,
        sequence=session.sequence,
        unit_id=identity.output_name,
        storage_uri=descriptor.uri,
        table_identifier=descriptor.table_identifier,
        snapshot_id=descriptor.snapshot_id,
        continuation=dict(continuation),
        schema_fingerprint=descriptor.schema_fingerprint,
        rows=descriptor.rows,
        bytes=descriptor.bytes,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    repository.append_checkpoint(checkpoint)
    return checkpoint


def load_verified_checkpoint(
    identity: RowsetIdentity,
    repository: StepCheckpointRepository,
    store: Any,
) -> StepCheckpoint | None:
    checkpoint = repository.latest_checkpoint(
        project_id=identity.project_id,
        job_id=identity.job_id,
        run_id=identity.run_id,
        step_id=identity.step_id,
        task_id=identity.step_id,
        unit_id=identity.output_name,
    )
    if checkpoint is None:
        return None
    table = store.catalog.load_table(tuple(checkpoint.table_identifier.split(".")))
    snapshot = next(
        (item for item in table.snapshots() if item.snapshot_id == checkpoint.snapshot_id),
        None,
    )
    if snapshot is None:
        raise CheckpointCorruptionError(f"checkpoint snapshot is missing: {checkpoint.snapshot_id}")
    properties = snapshot.summary.additional_properties
    expected = {
        "zeta4s.project-id": checkpoint.project_id,
        "zeta4s.job-id": checkpoint.job_id,
        "zeta4s.run-id": checkpoint.run_id,
        "zeta4s.step-id": checkpoint.step_id,
        "zeta4s.attempt": str(checkpoint.attempt),
        "zeta4s.output-name": checkpoint.unit_id,
        "zeta4s.sequence": str(checkpoint.sequence),
        "zeta4s.rows": str(checkpoint.rows),
        "zeta4s.bytes": str(checkpoint.bytes),
    }
    mismatched = [key for key, value in expected.items() if properties.get(key) != value]
    if mismatched:
        raise CheckpointCorruptionError(f"checkpoint snapshot metadata mismatch: {', '.join(sorted(mismatched))}")
    from zeta4s.runtime.rowset_stores.iceberg import _schema_fingerprint

    if _schema_fingerprint(table.schema().as_arrow()) != checkpoint.schema_fingerprint:
        raise CheckpointCorruptionError("checkpoint schema fingerprint mismatch")
    return checkpoint


__all__ = [
    "CheckpointCorruptionError",
    "commit_step_checkpoint",
    "load_verified_checkpoint",
]
