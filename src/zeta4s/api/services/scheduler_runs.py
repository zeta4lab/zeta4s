"""Scheduler-neutral run projection contracts used by zeta4s-api."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


CANONICAL_RUN_STATES = frozenset(
    {"queued", "running", "succeeded", "failed", "cancelled", "skipped", "not_found", "unavailable", "timeout"}
)


class RunCapabilityUnsupported(RuntimeError):
    code = "Z4E_RUN_CAPABILITY_UNSUPPORTED"

    def __init__(self, operation: str, scheduler: str) -> None:
        self.operation = operation
        self.scheduler = scheduler
        super().__init__(f"run operation is not supported: scheduler={scheduler} operation={operation}")


@dataclass(frozen=True)
class RunSnapshot:
    project_id: str
    job_id: str
    run_id: str
    scheduler: str
    scheduler_run_id: str
    state: str
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    adapter_metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        if self.state not in CANONICAL_RUN_STATES:
            raise ValueError(f"invalid canonical run state: {self.state}")
        return {
            "project_id": self.project_id,
            "job_id": self.job_id,
            "run_id": self.run_id,
            "scheduler": self.scheduler,
            "scheduler_run_id": self.scheduler_run_id,
            "state": self.state,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "parameters": self.parameters,
            "adapter_metadata": self.adapter_metadata,
        }


class SchedulerRunAdapter(Protocol):
    scheduler: str

    def create_run(self, *, project_id: str, job_id: str, run_id: str, parameters: dict[str, Any]) -> RunSnapshot: ...

    def get_run(self, run: dict[str, Any]) -> RunSnapshot: ...

    def list_runs(self, *, project_id: str, job_id: str, limit: int) -> list[RunSnapshot]: ...

    def list_tasks(self, run: dict[str, Any]) -> list[dict[str, Any]]: ...

    def read_logs(
        self,
        run: dict[str, Any],
        *,
        task_id: str | None,
        failed_only: bool,
        latest_attempt_only: bool,
        tail: int | None,
    ) -> list[dict[str, Any]]: ...

    def cancel_run(self, run: dict[str, Any]) -> RunSnapshot: ...


def adapter_for_registration(registration: dict[str, Any]) -> SchedulerRunAdapter:
    scheduler = str(registration.get("scheduler_backend") or registration.get("scheduler") or "")
    if scheduler == "airflow":
        from zeta4s.airflow.run_adapter import AirflowRunAdapter

        return AirflowRunAdapter(registration)
    if scheduler == "prefect":
        from zeta4s.prefect.run_adapter import PrefectRunAdapter

        return PrefectRunAdapter(registration)
    raise ValueError(f"active deployment has unsupported scheduler: {scheduler or '<missing>'}")


def duration_seconds(started_at: Any, ended_at: Any) -> float | None:
    from datetime import datetime, timezone

    if not started_at or not ended_at:
        return None
    try:
        start = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if end < start:
        return None
    return round((end - start).total_seconds(), 6)
