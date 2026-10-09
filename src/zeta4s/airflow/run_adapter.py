"""Airflow-native projection of the scheduler-neutral run contract."""

from __future__ import annotations

from typing import Any

from zeta4s.api.services.scheduler_runs import RunSnapshot, duration_seconds
from zeta4s.airflow import runs


_STATE = {
    "queued": "queued",
    "scheduled": "queued",
    "running": "running",
    "success": "succeeded",
    "failed": "failed",
}

_TASK_STATE = {**_STATE, "upstream_failed": "failed", "skipped": "skipped", "removed": "skipped"}


class AirflowRunAdapter:
    scheduler = "airflow"

    def __init__(self, registration: dict[str, Any]) -> None:
        self.registration = registration

    @staticmethod
    def _job_native_id(project_id: str, job_id: str) -> str:
        return f"{project_id}__{job_id}"

    def create_run(self, *, project_id: str, job_id: str, run_id: str, parameters: dict[str, Any]) -> RunSnapshot:
        native_id = self._job_native_id(project_id, job_id)
        conf = {**parameters, "z4_run_id": run_id}
        row = runs.trigger_dag_run(native_id, run_id, conf)
        return self._snapshot(project_id, job_id, run_id, row, parameters=parameters)

    def get_run(self, run: dict[str, Any]) -> RunSnapshot:
        native_id = self._job_native_id(str(run["project_id"]), str(run["job_id"]))
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        row = runs.dag_run(native_id, scheduler_run_id)
        if row is None:
            return self._missing(run)
        return self._snapshot(
            str(run["project_id"]),
            str(run["job_id"]),
            str(run["run_id"]),
            row,
            parameters=dict(run.get("parameters") or {}),
        )

    def list_runs(self, *, project_id: str, job_id: str, limit: int) -> list[RunSnapshot]:
        native_id = self._job_native_id(project_id, job_id)
        return [
            self._snapshot(
                project_id,
                job_id,
                str(row["run_id"]),
                row,
                parameters={key: value for key, value in dict(row.get("conf") or {}).items() if key != "z4_run_id"},
            )
            for row in runs.dag_runs(native_id, limit)
        ]

    def list_tasks(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        native_id = self._job_native_id(str(run["project_id"]), str(run["job_id"]))
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        rows = runs.task_instances(native_id, scheduler_run_id) or []
        return [self._task(row) for row in rows]

    def read_logs(
        self,
        run: dict[str, Any],
        *,
        task_id: str | None,
        failed_only: bool,
        latest_attempt_only: bool,
        tail: int | None,
    ) -> list[dict[str, Any]]:
        native_id = self._job_native_id(str(run["project_id"]), str(run["job_id"]))
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        failed_states = {"failed", "upstream_failed", "up_for_retry"}
        entries: list[dict[str, Any]] = []
        for native_task in runs.task_instances(native_id, scheduler_run_id) or []:
            current_task_id = str(native_task.get("task_id") or "")
            if not current_task_id or (task_id and current_task_id != task_id):
                continue
            if failed_only and native_task.get("state") not in failed_states:
                continue
            latest_attempt = int(native_task.get("try_number") or 0)
            attempts = [latest_attempt] if latest_attempt_only else list(range(1, latest_attempt + 1))
            for attempt in attempts:
                if attempt <= 0:
                    continue
                lines = runs.task_log_lines(
                    native_id,
                    scheduler_run_id,
                    current_task_id,
                    attempt,
                    map_index=native_task.get("map_index"),
                )
                if lines is None:
                    continue
                if tail and tail > 0:
                    lines = lines[-tail:]
                entries.append(
                    {
                        "task_id": current_task_id,
                        "attempt": attempt,
                        "state": _TASK_STATE.get(str(native_task.get("state") or ""), "running"),
                        "content": "\n".join(lines),
                    }
                )
        return entries

    def cancel_run(self, run: dict[str, Any]) -> RunSnapshot:
        native_id = self._job_native_id(str(run["project_id"]), str(run["job_id"]))
        scheduler_run_id = str(run.get("scheduler_run_id") or run["run_id"])
        if not runs.set_dag_run_state(native_id, scheduler_run_id, "failed"):
            return self._missing(run)
        return RunSnapshot(
            project_id=str(run["project_id"]),
            job_id=str(run["job_id"]),
            run_id=str(run["run_id"]),
            scheduler=self.scheduler,
            scheduler_run_id=scheduler_run_id,
            state="cancelled",
            created_at=run.get("created_at"),
            started_at=run.get("started_at"),
            ended_at=run.get("ended_at"),
            duration_seconds=run.get("duration_seconds"),
            parameters=dict(run.get("parameters") or {}),
            adapter_metadata={"native_job_id": native_id, "native_state": "failed", "cancelled": True},
        )

    def _snapshot(
        self,
        project_id: str,
        job_id: str,
        run_id: str,
        row: dict[str, Any],
        *,
        parameters: dict[str, Any],
    ) -> RunSnapshot:
        scheduler_run_id = str(row.get("run_id") or run_id)
        started_at = row.get("start_date")
        ended_at = row.get("end_date")
        native_state = str(row.get("state") or "queued")
        return RunSnapshot(
            project_id=project_id,
            job_id=job_id,
            run_id=run_id,
            scheduler=self.scheduler,
            scheduler_run_id=scheduler_run_id,
            state=_STATE.get(native_state, "running"),
            created_at=row.get("queued_at") or started_at or row.get("logical_date"),
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=duration_seconds(started_at, ended_at),
            parameters=parameters,
            adapter_metadata={"native_job_id": self._job_native_id(project_id, job_id), "native_state": native_state},
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
    def _task(row: dict[str, Any]) -> dict[str, Any]:
        started_at = row.get("start_date")
        ended_at = row.get("end_date")
        return {
            "step_id": str(row.get("task_id") or ""),
            "task_id": str(row.get("task_id") or ""),
            "state": _TASK_STATE.get(str(row.get("state") or ""), "running"),
            "attempt": int(row.get("try_number") or 0),
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": row.get("duration") or duration_seconds(started_at, ended_at),
        }
