"""PostgreSQL metastore adapter."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import os
import time
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

import psycopg
from psycopg.types.json import Jsonb
from psycopg.rows import dict_row

from zeta4s.metastore.contracts import (
    ArtifactMetadata,
    DeploymentRegistration,
    StepCheckpoint,
    require_scheduler_backend,
)


DEFAULT_POSTGRES_DSN = "postgresql://airflow:airflow@postgres:5432/zeta4s_metastore"
DEPLOY_REGISTRATION_TABLE = "deploy_registration"
ARTIFACT_TABLE = "artifact"
OPERATION_REPORT_TABLE = "operation_report"
RUN_METADATA_TABLE = "run_metadata"
STEP_EXECUTION_TABLE = "step_execution"
STEP_CHECKPOINT_TABLE = "step_checkpoint"
STEP_STATE_TABLE = "step_state"
STEP_EVENT_TABLE = "step_event"
STEP_OUTPUT_BINDING_TABLE = "step_output_binding"
BACKEND_REGISTRY_TABLE = "backend_registry"
SECRET_TABLE = "secret"
REQUIRED_TABLES = (
    DEPLOY_REGISTRATION_TABLE,
    ARTIFACT_TABLE,
    OPERATION_REPORT_TABLE,
    RUN_METADATA_TABLE,
    STEP_EXECUTION_TABLE,
    STEP_CHECKPOINT_TABLE,
    STEP_STATE_TABLE,
    STEP_EVENT_TABLE,
    STEP_OUTPUT_BINDING_TABLE,
    BACKEND_REGISTRY_TABLE,
    SECRET_TABLE,
)

SCHEMA_STATEMENTS = (
    f"""
    CREATE TABLE IF NOT EXISTS {DEPLOY_REGISTRATION_TABLE} (
        project_id TEXT NOT NULL,
        artifact_id TEXT NOT NULL,
        profile_id TEXT NOT NULL,
        scheduler_backend TEXT NOT NULL CHECK (scheduler_backend IN ('airflow', 'prefect')),
        registered_at TIMESTAMPTZ NOT NULL,
        dags JSONB NOT NULL,
        status TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id)
    )
    """,
    f"""
    ALTER TABLE {DEPLOY_REGISTRATION_TABLE}
    ADD COLUMN IF NOT EXISTS scheduler_backend TEXT NOT NULL
    CHECK (scheduler_backend IN ('airflow', 'prefect'))
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {ARTIFACT_TABLE} (
        artifact_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        storage_uri TEXT NOT NULL,
        runtime_connections JSONB NOT NULL,
        dags JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {OPERATION_REPORT_TABLE} (
        report_id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        project_id TEXT,
        status TEXT NOT NULL,
        report JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {RUN_METADATA_TABLE} (
        run_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        artifact_id TEXT,
        scheduler_run_id TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        run JSONB NOT NULL,
        revision BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STEP_EXECUTION_TABLE} (
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        step_type TEXT NOT NULL,
        attempt INTEGER NOT NULL CHECK (attempt >= 0),
        status TEXT NOT NULL,
        started_at TIMESTAMPTZ,
        ended_at TIMESTAMPTZ,
        metadata JSONB NOT NULL,
        revision BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, job_id, run_id, step_id, task_id, attempt)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STEP_CHECKPOINT_TABLE} (
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        attempt INTEGER NOT NULL CHECK (attempt >= 1),
        sequence INTEGER NOT NULL CHECK (sequence >= 1),
        unit_id TEXT NOT NULL,
        storage_uri TEXT NOT NULL,
        table_identifier TEXT NOT NULL,
        snapshot_id BIGINT NOT NULL,
        continuation JSONB NOT NULL,
        schema_fingerprint TEXT NOT NULL,
        rows BIGINT NOT NULL CHECK (rows >= 0),
        bytes BIGINT NOT NULL CHECK (bytes >= 0),
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (project_id, job_id, run_id, step_id, task_id, unit_id, sequence)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STEP_STATE_TABLE} (
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        state_key TEXT NOT NULL,
        state_type TEXT NOT NULL,
        state_value JSONB NOT NULL,
        run_id TEXT NOT NULL,
        revision BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, job_id, step_id, state_key)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STEP_EVENT_TABLE} (
        event_id BIGSERIAL PRIMARY KEY,
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL,
        event JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        revision BIGINT NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {STEP_OUTPUT_BINDING_TABLE} (
        project_id TEXT NOT NULL,
        job_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        step_id TEXT NOT NULL,
        output_name TEXT NOT NULL,
        output_kind TEXT NOT NULL,
        binding JSONB NOT NULL,
        revision BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, job_id, run_id, step_id, output_name)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {BACKEND_REGISTRY_TABLE} (
        project_id TEXT NOT NULL,
        backend_id TEXT NOT NULL,
        backend_type TEXT NOT NULL,
        backend JSONB NOT NULL,
        status TEXT NOT NULL,
        revision BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (project_id, backend_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SECRET_TABLE} (
        secret_key TEXT NOT NULL,
        version BIGINT NOT NULL CHECK (version >= 0),
        ciphertext TEXT NOT NULL,
        algorithm TEXT NOT NULL,
        key_id TEXT,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        rotated_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (secret_key, version)
    )
    """,
)


def postgres_dsn() -> str:
    return os.environ.get("ZETA4S_METASTORE_DSN", DEFAULT_POSTGRES_DSN)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _revision() -> int:
    return time.time_ns()


def _datetime_iso(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


class PostgresMetastoreAdapter:
    def __init__(
        self,
        *,
        dsn: str | None = None,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self.dsn = dsn or postgres_dsn()
        self.database = urlparse(self.dsn).path.lstrip("/")
        self._connect = connect or psycopg.connect
        self.deployment_repository = PostgresDeploymentRepository(self)
        self.artifact_repository = PostgresArtifactRepository(self)
        self.operation_report_repository = PostgresOperationReportRepository(self)
        self.run_metadata_repository = PostgresRunMetadataRepository(self)
        self.step_execution_repository = PostgresStepExecutionRepository(self)
        self.step_checkpoint_repository = PostgresStepCheckpointRepository(self)
        self.step_state_repository = PostgresStepStateRepository(self)
        self.step_event_repository = PostgresStepEventRepository(self)
        self.step_output_binding_repository = PostgresStepOutputBindingRepository(self)
        self.backend_registry_repository = PostgresBackendRegistryRepository(self)
        self.secret_repository = PostgresSecretRepository(self)

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self._connect(self.dsn, row_factory=dict_row) as connection:
            yield connection

    def bootstrap(self) -> None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                for statement in SCHEMA_STATEMENTS:
                    cursor.execute(statement)

    def inspect_schema(self) -> dict[str, Any]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%(required_tables)s)
                    ORDER BY table_name
                    """,
                    {"required_tables": list(REQUIRED_TABLES)},
                )
                existing = sorted(str(row["table_name"]) for row in cursor.fetchall())
        missing = sorted(set(REQUIRED_TABLES) - set(existing))
        return {
            "database": self.database,
            "required_tables": list(REQUIRED_TABLES),
            "existing_tables": existing,
            "missing_tables": missing,
            "status": "ok" if not missing else "missing",
        }


class PostgresDeploymentRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    @staticmethod
    def _registration(row: dict[str, Any]) -> DeploymentRegistration:
        return DeploymentRegistration(
            project_id=str(row["project_id"]),
            artifact_id=str(row["artifact_id"]),
            profile_id=str(row["profile_id"]),
            scheduler_backend=str(row["scheduler_backend"]),
            registered_at=_datetime_iso(row["registered_at"]),
            dags=list(row["dags"]),
            status=str(row["status"]),
        )

    def list_active(self) -> list[DeploymentRegistration]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, artifact_id, profile_id, scheduler_backend,
                           registered_at, dags, status
                    FROM {DEPLOY_REGISTRATION_TABLE}
                    WHERE status = 'active'
                    ORDER BY project_id
                    """
                )
                rows = cursor.fetchall()
        return [self._registration(row) for row in rows]

    def upsert_active(
        self,
        *,
        project_id: str,
        artifact_id: str,
        profile_id: str,
        scheduler_backend: str,
        dags: list[dict[str, Any]],
    ) -> DeploymentRegistration:
        scheduler_backend = require_scheduler_backend(scheduler_backend)
        registered_at = _utc_now()
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {DEPLOY_REGISTRATION_TABLE} (
                        project_id, artifact_id, profile_id, scheduler_backend,
                        registered_at, dags, status, updated_at
                    ) VALUES (
                        %(project_id)s, %(artifact_id)s, %(profile_id)s,
                        %(scheduler_backend)s, %(registered_at)s, %(dags)s,
                        'active', %(registered_at)s
                    )
                    ON CONFLICT (project_id) DO UPDATE SET
                        artifact_id = EXCLUDED.artifact_id,
                        profile_id = EXCLUDED.profile_id,
                        scheduler_backend = EXCLUDED.scheduler_backend,
                        registered_at = EXCLUDED.registered_at,
                        dags = EXCLUDED.dags,
                        status = 'active',
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "project_id": project_id,
                        "artifact_id": artifact_id,
                        "profile_id": profile_id,
                        "scheduler_backend": scheduler_backend,
                        "registered_at": registered_at,
                        "dags": Jsonb(dags),
                    },
                )
        return DeploymentRegistration(
            project_id=project_id,
            artifact_id=artifact_id,
            profile_id=profile_id,
            scheduler_backend=scheduler_backend,
            registered_at=registered_at.isoformat(),
            dags=dags,
        )

    def remove_active(self, project_id: str) -> DeploymentRegistration | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, artifact_id, profile_id, scheduler_backend,
                           registered_at, dags, status
                    FROM {DEPLOY_REGISTRATION_TABLE}
                    WHERE project_id = %(project_id)s AND status = 'active'
                    FOR UPDATE
                    """,
                    {"project_id": project_id},
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                cursor.execute(
                    f"""
                    UPDATE {DEPLOY_REGISTRATION_TABLE}
                    SET status = 'removed', updated_at = CURRENT_TIMESTAMP
                    WHERE project_id = %(project_id)s
                    """,
                    {"project_id": project_id},
                )
        return self._registration(row)

    def artifact_is_active(self, artifact_id: str) -> bool:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT EXISTS (
                        SELECT 1 FROM {DEPLOY_REGISTRATION_TABLE}
                        WHERE artifact_id = %(artifact_id)s AND status = 'active'
                    ) AS is_active
                    """,
                    {"artifact_id": artifact_id},
                )
                row = cursor.fetchone()
        return bool(row and row["is_active"])


class PostgresArtifactRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    def upsert_artifact(
        self,
        *,
        artifact_id: str,
        project_id: str,
        storage_uri: str,
        dags: list[dict[str, Any]],
        runtime_connections: list[dict[str, Any]] | None = None,
    ) -> ArtifactMetadata:
        created_at = _utc_now()
        connections = runtime_connections or []
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {ARTIFACT_TABLE} (
                        artifact_id, project_id, storage_uri,
                        runtime_connections, dags, created_at, updated_at
                    ) VALUES (
                        %(artifact_id)s, %(project_id)s, %(storage_uri)s,
                        %(runtime_connections)s,
                        %(dags)s, %(created_at)s, %(created_at)s
                    )
                    ON CONFLICT (artifact_id) DO UPDATE SET
                        project_id = EXCLUDED.project_id,
                        storage_uri = EXCLUDED.storage_uri,
                        runtime_connections = EXCLUDED.runtime_connections,
                        dags = EXCLUDED.dags,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "artifact_id": artifact_id,
                        "project_id": project_id,
                        "storage_uri": storage_uri,
                        "runtime_connections": Jsonb(connections),
                        "dags": Jsonb(dags),
                        "created_at": created_at,
                    },
                )
        return ArtifactMetadata(
            artifact_id=artifact_id,
            project_id=project_id,
            storage_uri=storage_uri,
            dags=dags,
            created_at=created_at.isoformat(),
            runtime_connections=connections,
        )

    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT artifact_id, project_id, storage_uri,
                           runtime_connections, dags, created_at
                    FROM {ARTIFACT_TABLE}
                    WHERE artifact_id = %(artifact_id)s
                    """,
                    {"artifact_id": artifact_id},
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return ArtifactMetadata(
            artifact_id=str(row["artifact_id"]),
            project_id=str(row["project_id"]),
            storage_uri=str(row["storage_uri"]),
            dags=list(row["dags"]),
            created_at=_datetime_iso(row["created_at"]),
            runtime_connections=list(row["runtime_connections"]),
        )


class PostgresOperationReportRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    def save_report(self, report: dict[str, Any]) -> None:
        report_id = str(report.get("operation_id") or "")
        if not report_id:
            return
        created_at = _utc_now()
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {OPERATION_REPORT_TABLE} (
                        report_id, command, project_id, status, report,
                        created_at, updated_at
                    ) VALUES (
                        %(report_id)s, %(command)s, %(project_id)s, %(status)s,
                        %(report)s, %(created_at)s, %(created_at)s
                    )
                    ON CONFLICT (report_id) DO UPDATE SET
                        command = EXCLUDED.command,
                        project_id = EXCLUDED.project_id,
                        status = EXCLUDED.status,
                        report = EXCLUDED.report,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "report_id": report_id,
                        "command": str(report.get("command") or ""),
                        "project_id": (str(report["project_id"]) if report.get("project_id") is not None else None),
                        "status": str(report.get("status") or ""),
                        "report": Jsonb(report),
                        "created_at": created_at,
                    },
                )


class PostgresSecretRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

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
        created_at = _utc_now()
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                if status == "active":
                    cursor.execute(
                        f"""
                        UPDATE {SECRET_TABLE}
                        SET status = 'inactive', rotated_at = %(created_at)s,
                            updated_at = %(created_at)s
                        WHERE secret_key = %(secret_key)s AND status = 'active'
                          AND version <> %(version)s
                        """,
                        {
                            "secret_key": secret_key,
                            "version": int(version),
                            "created_at": created_at,
                        },
                    )
                cursor.execute(
                    f"""
                    INSERT INTO {SECRET_TABLE} (
                        secret_key, version, ciphertext, algorithm, key_id,
                        status, created_at, rotated_at, updated_at
                    ) VALUES (
                        %(secret_key)s, %(version)s, %(ciphertext)s, %(algorithm)s,
                        %(key_id)s, %(status)s, %(created_at)s, NULL, %(created_at)s
                    )
                    ON CONFLICT (secret_key, version) DO UPDATE SET
                        ciphertext = EXCLUDED.ciphertext,
                        algorithm = EXCLUDED.algorithm,
                        key_id = EXCLUDED.key_id,
                        status = EXCLUDED.status,
                        rotated_at = EXCLUDED.rotated_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "secret_key": secret_key,
                        "version": int(version),
                        "ciphertext": ciphertext,
                        "algorithm": algorithm,
                        "key_id": key_id,
                        "status": status,
                        "created_at": created_at,
                    },
                )

    @contextmanager
    def secret_write_lock(self, secret_key: str) -> Iterator[None]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%(lock_key)s))",
                    {"lock_key": f"zeta4s:secret:{secret_key}"},
                )
                yield

    def reencrypt_secret_version(
        self,
        *,
        secret_key: str,
        version: int,
        expected_key_id: str,
        ciphertext: str,
        key_id: str,
    ) -> bool:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {SECRET_TABLE}
                    SET ciphertext = %(ciphertext)s, key_id = %(key_id)s
                    WHERE secret_key = %(secret_key)s
                      AND version = %(version)s
                      AND status = 'active'
                      AND key_id = %(expected_key_id)s
                    """,
                    {
                        "secret_key": secret_key,
                        "version": version,
                        "expected_key_id": expected_key_id,
                        "ciphertext": ciphertext,
                        "key_id": key_id,
                    },
                )
                return cursor.rowcount == 1

    def get_active_secret(self, secret_key: str) -> dict[str, Any] | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT secret_key, version, ciphertext, algorithm, key_id,
                           status, created_at, rotated_at
                    FROM {SECRET_TABLE}
                    WHERE secret_key = %(secret_key)s AND status = 'active'
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    {"secret_key": secret_key},
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "secret_key": str(row["secret_key"]),
            "version": int(row["version"]),
            "ciphertext": str(row["ciphertext"]),
            "algorithm": str(row["algorithm"]),
            "key_id": str(row["key_id"]) if row["key_id"] is not None else None,
            "status": str(row["status"]),
            "created_at": _datetime_iso(row["created_at"]),
            "rotated_at": (_datetime_iso(row["rotated_at"]) if row["rotated_at"] is not None else None),
        }

    def list_secret_metadata(self) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT secret_key, version, algorithm, key_id, status,
                           created_at, rotated_at
                    FROM {SECRET_TABLE}
                    ORDER BY secret_key, version DESC
                    """
                )
                rows = cursor.fetchall()
        return [
            {
                "secret_key": str(row["secret_key"]),
                "version": int(row["version"]),
                "algorithm": str(row["algorithm"]),
                "key_id": str(row["key_id"]) if row["key_id"] is not None else None,
                "status": str(row["status"]),
                "created_at": _datetime_iso(row["created_at"]),
                "rotated_at": (_datetime_iso(row["rotated_at"]) if row["rotated_at"] is not None else None),
            }
            for row in rows
        ]


class PostgresRunMetadataRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    def create_run(self, run: dict[str, Any]) -> None:
        run_id = str(run.get("run_id") or "")
        if not run_id:
            raise ValueError("run_id is required")
        created_at = _parse_datetime(run["created_at"]) if run.get("created_at") else _utc_now()
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT run FROM {RUN_METADATA_TABLE} WHERE run_id = %(run_id)s FOR UPDATE",
                    {"run_id": run_id},
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing["run"] == run:
                        return
                    raise ValueError(f"run metadata already exists: {run_id}")
                cursor.execute(
                    f"""
                    INSERT INTO {RUN_METADATA_TABLE} (
                        run_id, project_id, job_id, artifact_id, scheduler_run_id,
                        created_at, run, revision, updated_at
                    ) VALUES (
                        %(run_id)s, %(project_id)s, %(job_id)s, %(artifact_id)s,
                        %(scheduler_run_id)s, %(created_at)s, %(run)s, %(revision)s,
                        CURRENT_TIMESTAMP
                    )
                    """,
                    {
                        "run_id": run_id,
                        "project_id": str(run.get("project_id") or ""),
                        "job_id": str(run.get("job_id") or ""),
                        "artifact_id": (str(run["artifact_id"]) if run.get("artifact_id") is not None else None),
                        "scheduler_run_id": (
                            str(run["scheduler_run_id"]) if run.get("scheduler_run_id") is not None else None
                        ),
                        "created_at": created_at,
                        "run": Jsonb(run),
                        "revision": _revision(),
                    },
                )

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        if not run_id:
            raise ValueError("run_id is required")
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT run FROM {RUN_METADATA_TABLE} WHERE run_id = %(run_id)s FOR UPDATE",
                    {"run_id": run_id},
                )
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(f"run metadata not found: {run_id}")
                updated = {**row["run"], **patch, "run_id": run_id}
                created_at = _parse_datetime(updated["created_at"]) if updated.get("created_at") else _utc_now()
                cursor.execute(
                    f"""
                    UPDATE {RUN_METADATA_TABLE}
                    SET project_id = %(project_id)s,
                        job_id = %(job_id)s,
                        artifact_id = %(artifact_id)s,
                        scheduler_run_id = %(scheduler_run_id)s,
                        created_at = %(created_at)s,
                        run = %(run)s,
                        revision = %(revision)s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = %(run_id)s
                    """,
                    {
                        "run_id": run_id,
                        "project_id": str(updated.get("project_id") or ""),
                        "job_id": str(updated.get("job_id") or ""),
                        "artifact_id": (
                            str(updated["artifact_id"]) if updated.get("artifact_id") is not None else None
                        ),
                        "scheduler_run_id": (
                            str(updated["scheduler_run_id"]) if updated.get("scheduler_run_id") is not None else None
                        ),
                        "created_at": created_at,
                        "run": Jsonb(updated),
                        "revision": _revision(),
                    },
                )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT run FROM {RUN_METADATA_TABLE} WHERE run_id = %(run_id)s",
                    {"run_id": run_id},
                )
                row = cursor.fetchone()
        return dict(row["run"]) if row is not None else None

    def list_runs(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT run
                    FROM {RUN_METADATA_TABLE}
                    WHERE (CAST(%(project_id)s AS TEXT) IS NULL OR project_id = %(project_id)s)
                      AND (CAST(%(job_id)s AS TEXT) IS NULL OR job_id = %(job_id)s)
                    ORDER BY created_at DESC, updated_at DESC, run_id DESC
                    LIMIT %(limit)s
                    """,
                    {"project_id": project_id, "job_id": job_id, "limit": int(limit)},
                )
                rows = cursor.fetchall()
        return [dict(row["run"]) for row in rows]


class PostgresStepExecutionRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

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
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {STEP_EXECUTION_TABLE} (
                        project_id, job_id, run_id, step_id, task_id, step_type,
                        attempt, status, started_at, ended_at, metadata, revision, updated_at
                    ) VALUES (
                        %(project_id)s, %(job_id)s, %(run_id)s, %(step_id)s,
                        %(task_id)s, %(step_type)s, %(attempt)s, %(status)s,
                        %(started_at)s, %(ended_at)s, %(metadata)s, %(revision)s,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (project_id, job_id, run_id, step_id, task_id, attempt)
                    DO UPDATE SET
                        step_type = EXCLUDED.step_type,
                        status = EXCLUDED.status,
                        started_at = EXCLUDED.started_at,
                        ended_at = EXCLUDED.ended_at,
                        metadata = EXCLUDED.metadata,
                        revision = EXCLUDED.revision,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                        "task_id": task_id,
                        "step_type": step_type,
                        "attempt": int(attempt),
                        "status": status,
                        "started_at": _parse_datetime(started_at) if started_at else None,
                        "ended_at": _parse_datetime(ended_at) if ended_at else None,
                        "metadata": Jsonb(metadata or {}),
                        "revision": _revision(),
                    },
                )

    def list_executions(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        run_id: str,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, job_id, run_id, step_id, task_id, attempt,
                           step_type, status, started_at, ended_at, metadata,
                           revision, updated_at
                    FROM {STEP_EXECUTION_TABLE}
                    WHERE run_id = %(run_id)s
                      AND (CAST(%(project_id)s AS TEXT) IS NULL OR project_id = %(project_id)s)
                      AND (CAST(%(job_id)s AS TEXT) IS NULL OR job_id = %(job_id)s)
                    ORDER BY step_id, task_id, attempt
                    """,
                    {"project_id": project_id, "job_id": job_id, "run_id": run_id},
                )
                rows = cursor.fetchall()
        return [self._execution(row) for row in rows]

    @staticmethod
    def _execution(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "job_id": str(row["job_id"]),
            "run_id": str(row["run_id"]),
            "step_id": str(row["step_id"]),
            "task_id": str(row["task_id"]),
            "attempt": int(row["attempt"]),
            "step_type": str(row["step_type"]),
            "status": str(row["status"]),
            "started_at": _datetime_iso(row["started_at"]) if row["started_at"] else None,
            "ended_at": _datetime_iso(row["ended_at"]) if row["ended_at"] else None,
            "metadata": dict(row["metadata"]),
            "revision": int(row["revision"]),
            "updated_at": _datetime_iso(row["updated_at"]),
        }


class PostgresStepCheckpointRepository:
    _COLUMNS = """
        project_id, job_id, run_id, step_id, task_id, attempt, sequence,
        unit_id, storage_uri, table_identifier, snapshot_id, continuation,
        schema_fingerprint, rows, bytes, created_at
    """

    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    def append_checkpoint(self, checkpoint: StepCheckpoint) -> None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {STEP_CHECKPOINT_TABLE} ({self._COLUMNS})
                    VALUES (
                        %(project_id)s, %(job_id)s, %(run_id)s, %(step_id)s,
                        %(task_id)s, %(attempt)s, %(sequence)s, %(unit_id)s,
                        %(storage_uri)s, %(table_identifier)s, %(snapshot_id)s,
                        %(continuation)s, %(schema_fingerprint)s, %(rows)s,
                        %(bytes)s, %(created_at)s
                    )
                    ON CONFLICT (
                        project_id, job_id, run_id, step_id, task_id, unit_id, sequence
                    ) DO UPDATE SET attempt = {STEP_CHECKPOINT_TABLE}.attempt
                    WHERE {STEP_CHECKPOINT_TABLE}.attempt = EXCLUDED.attempt
                      AND {STEP_CHECKPOINT_TABLE}.storage_uri = EXCLUDED.storage_uri
                      AND {STEP_CHECKPOINT_TABLE}.table_identifier = EXCLUDED.table_identifier
                      AND {STEP_CHECKPOINT_TABLE}.snapshot_id = EXCLUDED.snapshot_id
                      AND {STEP_CHECKPOINT_TABLE}.continuation = EXCLUDED.continuation
                      AND {STEP_CHECKPOINT_TABLE}.schema_fingerprint = EXCLUDED.schema_fingerprint
                      AND {STEP_CHECKPOINT_TABLE}.rows = EXCLUDED.rows
                      AND {STEP_CHECKPOINT_TABLE}.bytes = EXCLUDED.bytes
                      AND {STEP_CHECKPOINT_TABLE}.created_at = EXCLUDED.created_at
                    RETURNING {self._COLUMNS}
                    """,
                    self._parameters(checkpoint),
                )
                row = cursor.fetchone()
        if row is None:
            raise ValueError(
                "conflicting step checkpoint: "
                f"{checkpoint.run_id}/{checkpoint.step_id}/{checkpoint.unit_id}/{checkpoint.sequence}"
            )

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
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {self._COLUMNS}
                    FROM {STEP_CHECKPOINT_TABLE}
                    WHERE project_id = %(project_id)s
                      AND job_id = %(job_id)s
                      AND run_id = %(run_id)s
                      AND step_id = %(step_id)s
                      AND task_id = %(task_id)s
                      AND unit_id = %(unit_id)s
                    ORDER BY sequence DESC, attempt DESC, created_at DESC
                    LIMIT 1
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                        "task_id": task_id,
                        "unit_id": unit_id,
                    },
                )
                row = cursor.fetchone()
        return self._checkpoint(row) if row is not None else None

    def list_checkpoints(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
    ) -> list[StepCheckpoint]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {self._COLUMNS}
                    FROM {STEP_CHECKPOINT_TABLE}
                    WHERE project_id = %(project_id)s
                      AND job_id = %(job_id)s
                      AND run_id = %(run_id)s
                      AND step_id = %(step_id)s
                    ORDER BY unit_id, sequence, attempt
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                    },
                )
                rows = cursor.fetchall()
        return [self._checkpoint(row) for row in rows]

    @staticmethod
    def _parameters(checkpoint: StepCheckpoint) -> dict[str, Any]:
        return {
            **checkpoint.__dict__,
            "continuation": Jsonb(checkpoint.continuation),
            "created_at": _parse_datetime(checkpoint.created_at),
        }

    @staticmethod
    def _checkpoint(row: dict[str, Any]) -> StepCheckpoint:
        return StepCheckpoint(
            project_id=str(row["project_id"]),
            job_id=str(row["job_id"]),
            run_id=str(row["run_id"]),
            step_id=str(row["step_id"]),
            task_id=str(row["task_id"]),
            attempt=int(row["attempt"]),
            sequence=int(row["sequence"]),
            unit_id=str(row["unit_id"]),
            storage_uri=str(row["storage_uri"]),
            table_identifier=str(row["table_identifier"]),
            snapshot_id=int(row["snapshot_id"]),
            continuation=dict(row["continuation"]),
            schema_fingerprint=str(row["schema_fingerprint"]),
            rows=int(row["rows"]),
            bytes=int(row["bytes"]),
            created_at=_datetime_iso(row["created_at"]),
        )


class PostgresStepStateRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

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
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {STEP_STATE_TABLE} (
                        project_id, job_id, step_id, state_key, state_type,
                        state_value, run_id, revision, updated_at
                    ) VALUES (
                        %(project_id)s, %(job_id)s, %(step_id)s, %(state_key)s,
                        %(state_type)s, %(state_value)s, %(run_id)s, %(revision)s,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (project_id, job_id, step_id, state_key) DO UPDATE SET
                        state_type = EXCLUDED.state_type,
                        state_value = EXCLUDED.state_value,
                        run_id = EXCLUDED.run_id,
                        revision = EXCLUDED.revision,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "step_id": step_id,
                        "state_key": state_key,
                        "state_type": state_type,
                        "state_value": Jsonb(state_value),
                        "run_id": run_id,
                        "revision": _revision(),
                    },
                )

    def get_state(
        self,
        *,
        project_id: str,
        job_id: str,
        step_id: str,
        state_key: str,
    ) -> dict[str, Any] | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, job_id, step_id, state_key, state_type,
                           state_value, run_id, revision, updated_at
                    FROM {STEP_STATE_TABLE}
                    WHERE project_id = %(project_id)s AND job_id = %(job_id)s
                      AND step_id = %(step_id)s AND state_key = %(state_key)s
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "step_id": step_id,
                        "state_key": state_key,
                    },
                )
                row = cursor.fetchone()
        return self._state(row) if row is not None else None

    def list_states(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, job_id, step_id, state_key, state_type,
                           state_value, run_id, revision, updated_at
                    FROM {STEP_STATE_TABLE}
                    WHERE project_id = %(project_id)s
                      AND (CAST(%(job_id)s AS TEXT) IS NULL OR job_id = %(job_id)s)
                      AND (CAST(%(step_id)s AS TEXT) IS NULL OR step_id = %(step_id)s)
                    ORDER BY job_id, step_id, state_key
                    """,
                    {"project_id": project_id, "job_id": job_id, "step_id": step_id},
                )
                rows = cursor.fetchall()
        return [self._state(row) for row in rows]

    @staticmethod
    def _state(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "job_id": str(row["job_id"]),
            "step_id": str(row["step_id"]),
            "state_key": str(row["state_key"]),
            "state_type": str(row["state_type"]),
            "state_value": row["state_value"],
            "run_id": str(row["run_id"]),
            "revision": int(row["revision"]),
            "updated_at": _datetime_iso(row["updated_at"]),
        }


class PostgresStepEventRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

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
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {STEP_EVENT_TABLE} (
                        project_id, job_id, run_id, step_id, task_id,
                        event_type, status, event, created_at, revision
                    ) VALUES (
                        %(project_id)s, %(job_id)s, %(run_id)s, %(step_id)s,
                        %(task_id)s, %(event_type)s, %(status)s, %(event)s,
                        %(created_at)s, %(revision)s
                    )
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                        "task_id": task_id,
                        "event_type": event_type,
                        "status": status,
                        "event": Jsonb(event),
                        "created_at": _parse_datetime(created_at) if created_at else _utc_now(),
                        "revision": _revision(),
                    },
                )

    def list_events(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT event_id, project_id, job_id, run_id, step_id, task_id,
                           event_type, status, event, created_at, revision
                    FROM {STEP_EVENT_TABLE}
                    WHERE project_id = %(project_id)s
                      AND (CAST(%(job_id)s AS TEXT) IS NULL OR job_id = %(job_id)s)
                      AND (CAST(%(event_type)s AS TEXT) IS NULL OR event_type = %(event_type)s)
                    ORDER BY created_at DESC, event_id DESC
                    LIMIT %(limit)s
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "event_type": event_type,
                        "limit": max(1, min(int(limit), 500)),
                    },
                )
                rows = cursor.fetchall()
        return [self._event(row) for row in rows]

    @staticmethod
    def _event(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "job_id": str(row["job_id"]),
            "run_id": str(row["run_id"]),
            "step_id": str(row["step_id"]),
            "task_id": str(row["task_id"]),
            "event_type": str(row["event_type"]),
            "status": str(row["status"]),
            "event": dict(row["event"]),
            "created_at": _datetime_iso(row["created_at"]),
            "revision": int(row["revision"]),
            "updated_at": _datetime_iso(row["created_at"]),
        }


class PostgresStepOutputBindingRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

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
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {STEP_OUTPUT_BINDING_TABLE} (
                        project_id, job_id, run_id, step_id, output_name,
                        output_kind, binding, revision, updated_at
                    ) VALUES (
                        %(project_id)s, %(job_id)s, %(run_id)s, %(step_id)s,
                        %(output_name)s, %(output_kind)s, %(binding)s, %(revision)s,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (project_id, job_id, run_id, step_id, output_name)
                    DO UPDATE SET
                        output_kind = EXCLUDED.output_kind,
                        binding = EXCLUDED.binding,
                        revision = EXCLUDED.revision,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                        "output_name": output_name,
                        "output_kind": output_kind,
                        "binding": Jsonb(binding),
                        "revision": _revision(),
                    },
                )

    def get_binding(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
        output_name: str,
    ) -> dict[str, Any] | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, job_id, run_id, step_id, output_name,
                           output_kind, binding, revision, updated_at
                    FROM {STEP_OUTPUT_BINDING_TABLE}
                    WHERE project_id = %(project_id)s AND job_id = %(job_id)s
                      AND run_id = %(run_id)s AND step_id = %(step_id)s
                      AND output_name = %(output_name)s
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                        "output_name": output_name,
                    },
                )
                row = cursor.fetchone()
        return self._binding(row) if row is not None else None

    def list_bindings(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, job_id, run_id, step_id, output_name,
                           output_kind, binding, revision, updated_at
                    FROM {STEP_OUTPUT_BINDING_TABLE}
                    WHERE project_id = %(project_id)s AND job_id = %(job_id)s
                      AND run_id = %(run_id)s
                      AND (CAST(%(step_id)s AS TEXT) IS NULL OR step_id = %(step_id)s)
                    ORDER BY step_id, output_name
                    """,
                    {
                        "project_id": project_id,
                        "job_id": job_id,
                        "run_id": run_id,
                        "step_id": step_id,
                    },
                )
                rows = cursor.fetchall()
        return [self._binding(row) for row in rows]

    @staticmethod
    def _binding(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "job_id": str(row["job_id"]),
            "run_id": str(row["run_id"]),
            "step_id": str(row["step_id"]),
            "output_name": str(row["output_name"]),
            "output_kind": str(row["output_kind"]),
            "binding": dict(row["binding"]),
            "revision": int(row["revision"]),
            "updated_at": _datetime_iso(row["updated_at"]),
        }


class PostgresBackendRegistryRepository:
    def __init__(self, adapter: PostgresMetastoreAdapter) -> None:
        self.adapter = adapter

    def upsert_backend(
        self,
        *,
        project_id: str,
        backend_id: str,
        backend_type: str,
        backend: dict[str, Any],
        status: str,
    ) -> None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {BACKEND_REGISTRY_TABLE} (
                        project_id, backend_id, backend_type, backend, status,
                        revision, updated_at
                    ) VALUES (
                        %(project_id)s, %(backend_id)s, %(backend_type)s,
                        %(backend)s, %(status)s, %(revision)s, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (project_id, backend_id) DO UPDATE SET
                        backend_type = EXCLUDED.backend_type,
                        backend = EXCLUDED.backend,
                        status = EXCLUDED.status,
                        revision = EXCLUDED.revision,
                        updated_at = EXCLUDED.updated_at
                    """,
                    {
                        "project_id": project_id,
                        "backend_id": backend_id,
                        "backend_type": backend_type,
                        "backend": Jsonb(backend),
                        "status": status,
                        "revision": _revision(),
                    },
                )

    def get_backend(self, *, project_id: str, backend_id: str) -> dict[str, Any] | None:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, backend_id, backend_type, backend, status,
                           revision, updated_at
                    FROM {BACKEND_REGISTRY_TABLE}
                    WHERE project_id = %(project_id)s AND backend_id = %(backend_id)s
                    """,
                    {"project_id": project_id, "backend_id": backend_id},
                )
                row = cursor.fetchone()
        return self._backend(row) if row is not None else None

    def list_backends(
        self,
        *,
        project_id: str,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.adapter.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT project_id, backend_id, backend_type, backend, status,
                           revision, updated_at
                    FROM {BACKEND_REGISTRY_TABLE}
                    WHERE project_id = %(project_id)s
                      AND (CAST(%(status)s AS TEXT) IS NULL OR status = %(status)s)
                    ORDER BY backend_id
                    """,
                    {"project_id": project_id, "status": status},
                )
                rows = cursor.fetchall()
        return [self._backend(row) for row in rows]

    @staticmethod
    def _backend(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "backend_id": str(row["backend_id"]),
            "backend_type": str(row["backend_type"]),
            "backend": dict(row["backend"]),
            "status": str(row["status"]),
            "revision": int(row["revision"]),
            "updated_at": _datetime_iso(row["updated_at"]),
        }
