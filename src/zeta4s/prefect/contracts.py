"""Prefect deployment identity and state."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScheduleIdentity:
    project_id: str
    job_id: str
    profile: str

    @property
    def key(self) -> str:
        return f"{self.project_id}__{self.job_id}__{self.profile}"


@dataclass(frozen=True)
class ScheduleState:
    identity: ScheduleIdentity
    deployment_id: str
    paused: bool
