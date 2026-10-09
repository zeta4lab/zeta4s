"""Prefect-native projection of the scheduler-neutral run contract."""

from __future__ import annotations

from typing import Any

from zeta4s.api.services.scheduler_runs import RunSnapshot, duration_seconds
from zeta4s.prefect.contracts import ScheduleIdentity
from zeta4s.prefect.prefect_engine import (
    cancel_prefect_flow_run,
    read_prefect_flow_run,
    read_prefect_logs,
    read_prefect_task_runs,
    trigger_prefect_deployment,
)


_STATE = {
    "PENDING": "queued",
    "SCHEDULED": "queued",
    "RUNNING": "running",
    "COMPLETED": "succeeded",
    "FAILED": "failed",
    "CRASHED": "failed",
    "CANCELLED": "cancelled",
    "CANCELLING": "cancelled",
    "PAUSED": "queued",
}


class PrefectRunAdapter:
    scheduler = "prefect"

    def __init__(self, registration: dict[str, Any]) -> None:
        self.registration = registration

    def _deployment_name(self, project_id: str, job_id: str) -> str:
        profile_id = str(self.registration.get("profile_id") or "local")
        return f"zeta4s-scheduled-job/{ScheduleIdentity(project_id, job_id, profile_id).key}"

    def create_run(self, *, project_id: str, job_id: str, run_id: str, parameters: dict[str, Any]) -> RunSnapshot:
        scheduler_run_id = trigger_prefect_deployment(
            self._deployment_name(project_id, job_id), parameters=parameters, idempotency_key=run_id, run_id=run_id
        )
        native = self._read_flow_run(scheduler_run_id)
        return self._snapshot(project_id, job_id, run_id, native, parameters)

    def get_run(self, run: dict[str, Any]) -> RunSnapshot:
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        try:
            native = self._read_flow_run(scheduler_run_id)
        except Exception:
            return self._missing(run)
        return self._snapshot(
            str(run["project_id"]), str(run["job_id"]), str(run["run_id"]), native, dict(run.get("parameters") or {})
        )

    def list_runs(self, *, project_id: str, job_id: str, limit: int) -> list[RunSnapshot]:
        # zeta4s-api lists canonical metastore rows; native bulk discovery is intentionally not an identity source.
        return []

    def list_tasks(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        flow_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        return [self._task(item) for item in read_prefect_task_runs(flow_run_id)]

    def read_logs(
        self,
        run: dict[str, Any],
        *,
        task_id: str | None,
        failed_only: bool,
        latest_attempt_only: bool,
        tail: int | None,
    ) -> list[dict[str, Any]]:
        flow_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        tasks = self.list_tasks(run)
        wanted = {
            item["task_id"]
            for item in tasks
            if (not task_id or item["task_id"] == task_id) and (not failed_only or item["state"] == "failed")
        }

        grouped: dict[str, list[str]] = {}
        for item in read_prefect_logs(flow_run_id, limit=tail):
            current = str(getattr(item, "task_run_id", None) or "__run__")
            if wanted and current not in wanted:
                continue
            grouped.setdefault(current, []).append(str(getattr(item, "message", "")))
        return [
            {"task_id": current, "attempt": None, "state": None, "content": "\n".join(lines[-tail:] if tail else lines)}
            for current, lines in grouped.items()
        ]

    def cancel_run(self, run: dict[str, Any]) -> RunSnapshot:
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])

        cancel_prefect_flow_run(scheduler_run_id)
        snapshot = self.get_run(run)
        return RunSnapshot(**{**snapshot.__dict__, "state": "cancelled"})

    @staticmethod
    def _read_flow_run(run_id: str):
        return read_prefect_flow_run(run_id)

    def _snapshot(
        self, project_id: str, job_id: str, run_id: str, native: Any, parameters: dict[str, Any]
    ) -> RunSnapshot:
        native_state = native.state.type.value.upper() if native.state else "PENDING"
        started_at = native.start_time.isoformat() if native.start_time else None
        ended_at = native.end_time.isoformat() if native.end_time else None
        created_at = native.created.isoformat() if getattr(native, "created", None) else None
        return RunSnapshot(
            project_id=project_id,
            job_id=job_id,
            run_id=run_id,
            scheduler=self.scheduler,
            scheduler_run_id=str(native.id),
            state=_STATE.get(native_state, "running"),
            created_at=created_at,
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=(
                native.total_run_time.total_seconds()
                if getattr(native, "total_run_time", None)
                else duration_seconds(started_at, ended_at)
            ),
            parameters=parameters,
            adapter_metadata={"native_state": native_state, "deployment_id": str(native.deployment_id)},
        )

    def _missing(self, run: dict[str, Any]) -> RunSnapshot:
        return RunSnapshot(
            project_id=str(run["project_id"]),
            job_id=str(run["job_id"]),
            run_id=str(run["run_id"]),
            scheduler=self.scheduler,
            scheduler_run_id=str(run.get("scheduler_run_id") or run["run_id"]),
            state="not_found",
            parameters=dict(run.get("parameters") or {}),
        )

    @staticmethod
    def _task(native: Any) -> dict[str, Any]:
        native_state = native.state_type.value.upper() if native.state_type else "PENDING"
        started_at = native.start_time.isoformat() if native.start_time else None
        ended_at = native.end_time.isoformat() if native.end_time else None
        return {
            "step_id": str(native.name),
            "task_id": str(native.id),
            "state": _STATE.get(native_state, "running"),
            "attempt": int(native.run_count or 0),
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds(started_at, ended_at),
        }
