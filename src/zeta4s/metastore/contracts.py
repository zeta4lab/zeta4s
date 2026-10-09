"""Metastore adapter and repository contracts."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Protocol


SCHEDULER_BACKENDS = frozenset({"airflow", "prefect"})


def require_scheduler_backend(value: object) -> str:
    if not isinstance(value, str) or value not in SCHEDULER_BACKENDS:
        raise ValueError(f"unsupported scheduler backend: {value}")
    return value


@dataclass(frozen=True)
class DeploymentRegistration:
    project_id: str
    artifact_id: str
    profile_id: str
    scheduler_backend: str
    registered_at: str
    dags: list[dict[str, Any]]
    status: str = "active"

    def as_scheduler_item(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "artifact_id": self.artifact_id,
            "profile_id": self.profile_id,
            "scheduler_backend": self.scheduler_backend,
            "registered_at": self.registered_at,
            "dags": self.dags,
        }


@dataclass(frozen=True)
class ArtifactMetadata:
    artifact_id: str
    project_id: str
    storage_uri: str
    dags: list[dict[str, Any]]
    created_at: str
    runtime_connections: list[dict[str, Any]] = field(default_factory=list)


class DeploymentRepository(Protocol):
    def list_active(self) -> list[DeploymentRegistration]:
        """Return active project deployments."""

    def upsert_active(
        self,
        *,
        project_id: str,
        artifact_id: str,
        profile_id: str,
        scheduler_backend: str,
        dags: list[dict[str, Any]],
    ) -> DeploymentRegistration:
        """Replace a project's active deployment."""

    def remove_active(self, project_id: str) -> DeploymentRegistration | None:
        """Mark a project's active deployment as removed."""

    def artifact_is_active(self, artifact_id: str) -> bool:
        """Return whether an artifact is referenced by an active deployment."""


class ArtifactRepository(Protocol):
    def upsert_artifact(
        self,
        *,
        artifact_id: str,
        project_id: str,
        storage_uri: str,
        dags: list[dict[str, Any]],
        runtime_connections: list[dict[str, Any]] | None = None,
    ) -> ArtifactMetadata:
        """Record artifact metadata stored in artifact storage."""

    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        """Return artifact metadata by artifact id."""


class OperationReportRepository(Protocol):
    def save_report(self, report: dict[str, Any]) -> None:
        """Persist a runtime operation report."""


class RunMetadataRepository(Protocol):
    def create_run(self, run: dict[str, Any]) -> None:
        """Persist newly created run metadata."""

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        """Persist a new revision of existing run metadata."""

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return run metadata by run id."""

    def list_runs(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        """Return recent run metadata."""


class StepExecutionRepository(Protocol):
    def record_execution(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        step_type: str,
        attempt: int,
        status: str,
        task_id: str,
        started_at: str | None = None,
        ended_at: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Persist a step task attempt execution state."""

    def list_executions(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Return latest execution states for a run."""


@dataclass(frozen=True)
class StepCheckpoint:
    project_id: str
    job_id: str
    run_id: str
    step_id: str
    task_id: str
    attempt: int
    sequence: int
    unit_id: str
    storage_uri: str
    table_identifier: str
    snapshot_id: int
    continuation: dict[str, Any]
    schema_fingerprint: str
    rows: int
    bytes: int
    created_at: str


class StepCheckpointRepository(Protocol):
    def append_checkpoint(self, checkpoint: StepCheckpoint) -> None:
        """Append one immutable committed checkpoint."""

    def latest_checkpoint(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        task_id: str,
        unit_id: str,
    ) -> StepCheckpoint | None:
        """Return the latest committed checkpoint for one step processing unit."""

    def list_checkpoints(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
    ) -> list[StepCheckpoint]:
        """Return committed checkpoints in deterministic unit/sequence order."""


class StepStateRepository(Protocol):
    def upsert_state(
        self,
        *,
        project_id: str,
        job_id: str,
        step_id: str,
        state_key: str,
        state_value: Any,
        state_type: str,
        run_id: str,
    ) -> None:
        """Persist the latest state value for a step key."""

    def get_state(
        self,
        *,
        project_id: str,
        job_id: str,
        step_id: str,
        state_key: str,
    ) -> dict[str, Any] | None:
        """Return the latest state value for a step key."""

    def list_states(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return latest state values for a project, job, or single step."""


class StepEventRepository(Protocol):
    def record_event(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        task_id: str,
        event_type: str,
        status: str,
        event: dict[str, Any],
        created_at: str | None = None,
    ) -> None:
        """Persist an append-only step-scoped runtime event."""

    def list_events(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return recent step-scoped runtime events."""


class StepOutputBindingRepository(Protocol):
    def upsert_binding(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        output_name: str,
        output_kind: str,
        binding: dict[str, Any],
    ) -> None:
        """Persist the latest runtime binding for a named step output."""

    def get_binding(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        output_name: str,
    ) -> dict[str, Any] | None:
        """Return the latest runtime binding for a named step output."""

    def list_bindings(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return latest runtime output bindings for a run."""


class BackendRegistryRepository(Protocol):
    def upsert_backend(
        self,
        *,
        project_id: str,
        backend_id: str,
        backend_type: str,
        backend: dict[str, Any],
        status: str,
    ) -> None:
        """Persist the latest runtime backend registry entry."""

    def get_backend(
        self,
        *,
        project_id: str,
        backend_id: str,
    ) -> dict[str, Any] | None:
        """Return the latest runtime backend registry entry."""

    def list_backends(
        self,
        *,
        project_id: str,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return latest runtime backend registry entries for a project."""


class SecretRepository(Protocol):
    def put_secret_version(
        self,
        *,
        secret_key: str,
        version: int,
        ciphertext: str,
        algorithm: str,
        key_id: str | None,
        status: str,
    ) -> None:
        """Persist an encrypted secret version. Plaintext secret values are never accepted."""

    def get_active_secret(self, secret_key: str) -> dict[str, Any] | None:
        """Return the active encrypted secret envelope for a key."""

    def list_secret_metadata(self) -> list[dict[str, Any]]:
        """Return secret metadata without plaintext or ciphertext payloads."""

    def secret_write_lock(self, secret_key: str) -> AbstractContextManager[None]:
        """Serialize writes for one secret key.

        `set_secret` reads the current versions and writes `max + 1`. Concurrent
        callers would otherwise compute the same next version and clobber each
        other through the upsert.
        """

    def reencrypt_secret_version(
        self,
        *,
        secret_key: str,
        version: int,
        expected_key_id: str,
        ciphertext: str,
        key_id: str,
    ) -> bool:
        """Replace ciphertext in place when the row still holds `expected_key_id`.

        Returns False when the row moved on — a concurrent write already produced
        a newer version encrypted with the active generation, so nothing is lost.
        """


class MetastoreAdapter(Protocol):
    deployment_repository: DeploymentRepository
    artifact_repository: ArtifactRepository
    operation_report_repository: OperationReportRepository
    run_metadata_repository: RunMetadataRepository
    step_execution_repository: StepExecutionRepository
    step_checkpoint_repository: StepCheckpointRepository
    step_state_repository: StepStateRepository
    step_event_repository: StepEventRepository
    step_output_binding_repository: StepOutputBindingRepository
    backend_registry_repository: BackendRegistryRepository
    secret_repository: SecretRepository

    def bootstrap(self) -> None:
        """Create metastore schema objects required by repositories."""

    def inspect_schema(self) -> dict[str, Any]:
        """Return read-only metastore schema status without creating schema objects."""
