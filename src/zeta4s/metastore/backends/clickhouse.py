"""ClickHouse metastore adapter."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
import time
from typing import Any, Iterator

from zeta4s.metastore.contracts import (
    ArtifactMetadata,
    DeploymentRegistration,
    StepCheckpoint,
    require_scheduler_backend,
)

METASTORE_DATABASE = os.environ.get("ZETA4S_METASTORE_DATABASE", "zeta4s_metastore")
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


def _connection_kwargs() -> dict[str, Any]:
    return {
        "host": os.environ.get("ZETA4S_METASTORE_HOST", "metastore"),
        "port": int(os.environ.get("ZETA4S_METASTORE_HTTP_PORT", "8123")),
        "username": os.environ.get("ZETA4S_METASTORE_USER", "metastore"),
        "password": os.environ.get("ZETA4S_METASTORE_PASSWORD", "metastore_pwd"),
    }


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


def _clickhouse_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class ClickHouseMetastoreAdapter:
    def __init__(self, *, database: str | None = None):
        self.database = database or os.environ.get("ZETA4S_METASTORE_DATABASE", METASTORE_DATABASE)
        self.deployment_repository = ClickHouseDeploymentRepository(self)
        self.artifact_repository = ClickHouseArtifactRepository(self)
        self.operation_report_repository = ClickHouseOperationReportRepository(self)
        self.run_metadata_repository = ClickHouseRunMetadataRepository(self)
        self.step_execution_repository = ClickHouseStepExecutionRepository(self)
        self.step_checkpoint_repository = ClickHouseStepCheckpointRepository(self)
        self.step_state_repository = ClickHouseStepStateRepository(self)
        self.step_event_repository = ClickHouseStepEventRepository(self)
        self.step_output_binding_repository = ClickHouseStepOutputBindingRepository(self)
        self.backend_registry_repository = ClickHouseBackendRegistryRepository(self)
        self.secret_repository = ClickHouseSecretRepository(self)

    def client(self):
        import clickhouse_connect

        return clickhouse_connect.get_client(**_connection_kwargs(), database=self.database)

    def inspect_schema(self) -> dict[str, Any]:
        import clickhouse_connect

        bootstrap_db = os.environ.get("ZETA4S_METASTORE_BOOTSTRAP_DB", "default")
        client = clickhouse_connect.get_client(**_connection_kwargs(), database=bootstrap_db)
        rows = client.query(
            f"""
            SELECT name
            FROM system.tables
            WHERE database = {_clickhouse_string(self.database)}
              AND name IN ({", ".join(_clickhouse_string(name) for name in REQUIRED_TABLES)})
            """
        ).result_rows
        existing = sorted(str(row[0]) for row in rows)
        missing = sorted(set(REQUIRED_TABLES) - set(existing))
        return {
            "database": self.database,
            "required_tables": list(REQUIRED_TABLES),
            "existing_tables": existing,
            "missing_tables": missing,
            "status": "ok" if not missing else "missing",
        }

    def bootstrap(self) -> None:
        import clickhouse_connect

        bootstrap_db = os.environ.get("ZETA4S_METASTORE_BOOTSTRAP_DB", "default")
        bootstrap_client = clickhouse_connect.get_client(**_connection_kwargs(), database=bootstrap_db)
        bootstrap_client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        client = self.client()
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {DEPLOY_REGISTRATION_TABLE} (
                project_id String,
                artifact_id String,
                profile_id String,
                scheduler_backend String,
                registered_at DateTime64(3),
                dags_json String,
                status String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY project_id
            """
        )
        client.command(
            f"""
            ALTER TABLE {DEPLOY_REGISTRATION_TABLE}
            ADD COLUMN IF NOT EXISTS scheduler_backend String
            AFTER profile_id
            """
        )
        client.command(
            f"""
            ALTER TABLE {DEPLOY_REGISTRATION_TABLE}
            ADD CONSTRAINT IF NOT EXISTS scheduler_backend_allowed
            CHECK scheduler_backend IN ('airflow', 'prefect')
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {ARTIFACT_TABLE} (
                artifact_id String,
                project_id String,
                storage_uri String,
                runtime_connections_json String,
                dags_json String,
                created_at DateTime64(3),
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY artifact_id
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {OPERATION_REPORT_TABLE} (
                report_id String,
                command String,
                project_id Nullable(String),
                status String,
                report_json String,
                created_at DateTime64(3),
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY report_id
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {RUN_METADATA_TABLE} (
                run_id String,
                project_id String,
                job_id String,
                artifact_id Nullable(String),
                scheduler_run_id Nullable(String),
                created_at DateTime64(3),
                run_json String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY run_id
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STEP_EXECUTION_TABLE} (
                project_id String,
                job_id String,
                run_id String,
                step_id String,
                task_id String,
                step_type String,
                attempt UInt32,
                status String,
                started_at Nullable(DateTime64(3)),
                ended_at Nullable(DateTime64(3)),
                metadata_json String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY (project_id, job_id, run_id, step_id, task_id, attempt)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STEP_CHECKPOINT_TABLE} (
                project_id String,
                job_id String,
                run_id String,
                step_id String,
                task_id String,
                attempt UInt32,
                sequence UInt32,
                unit_id String,
                storage_uri String,
                table_identifier String,
                snapshot_id Int64,
                continuation_json String,
                schema_fingerprint String,
                rows UInt64,
                bytes UInt64,
                created_at DateTime64(3),
                revision UInt64
            )
            ENGINE = MergeTree
            ORDER BY (project_id, job_id, run_id, step_id, task_id, unit_id, sequence, revision)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STEP_STATE_TABLE} (
                project_id String,
                job_id String,
                step_id String,
                state_key String,
                state_type String,
                state_value_json String,
                run_id String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY (project_id, job_id, step_id, state_key)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STEP_EVENT_TABLE} (
                project_id String,
                job_id String,
                run_id String,
                step_id String,
                task_id String,
                event_type String,
                status String,
                event_json String,
                created_at DateTime64(3),
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = MergeTree
            ORDER BY (project_id, job_id, event_type, created_at, run_id, step_id, task_id)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STEP_OUTPUT_BINDING_TABLE} (
                project_id String,
                job_id String,
                run_id String,
                step_id String,
                output_name String,
                output_kind String,
                binding_json String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY (project_id, job_id, run_id, step_id, output_name)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {BACKEND_REGISTRY_TABLE} (
                project_id String,
                backend_id String,
                backend_type String,
                backend_json String,
                status String,
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY (project_id, backend_id)
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {SECRET_TABLE} (
                secret_key String,
                version UInt64,
                ciphertext String,
                algorithm String,
                key_id Nullable(String),
                status String,
                created_at DateTime64(3),
                rotated_at Nullable(DateTime64(3)),
                revision UInt64,
                updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(revision)
            ORDER BY (secret_key, version)
            """
        )


class ClickHouseDeploymentRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    def list_active(self) -> list[DeploymentRegistration]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                tupleElement(latest, 1) AS artifact_id,
                tupleElement(latest, 2) AS profile_id,
                tupleElement(latest, 3) AS scheduler_backend,
                tupleElement(latest, 4) AS registered_at,
                tupleElement(latest, 5) AS dags_json,
                tupleElement(latest, 6) AS status
            FROM (
                SELECT
                    project_id,
                    argMax(
                        tuple(
                            artifact_id,
                            profile_id,
                            scheduler_backend,
                            registered_at,
                            dags_json,
                            status
                        ),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {DEPLOY_REGISTRATION_TABLE}
                GROUP BY project_id
            )
            WHERE tupleElement(latest, 6) = 'active'
            ORDER BY project_id ASC
            """
            )
            .result_rows
        )
        registrations: list[DeploymentRegistration] = []
        for (
            project_id,
            artifact_id,
            profile_id,
            scheduler_backend,
            registered_at,
            dags_json,
            status,
        ) in rows:
            registrations.append(
                DeploymentRegistration(
                    project_id=str(project_id),
                    artifact_id=str(artifact_id),
                    profile_id=str(profile_id),
                    scheduler_backend=str(scheduler_backend),
                    registered_at=_datetime_iso(registered_at),
                    dags=json.loads(str(dags_json)),
                    status=str(status),
                )
            )
        return registrations

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
        self._client().insert(
            DEPLOY_REGISTRATION_TABLE,
            [
                [
                    project_id,
                    artifact_id,
                    profile_id,
                    scheduler_backend,
                    registered_at,
                    json.dumps(dags, ensure_ascii=False),
                    "active",
                    _revision(),
                    registered_at,
                ]
            ],
            column_names=[
                "project_id",
                "artifact_id",
                "profile_id",
                "scheduler_backend",
                "registered_at",
                "dags_json",
                "status",
                "revision",
                "updated_at",
            ],
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
        current = next((item for item in self.list_active() if item.project_id == project_id), None)
        if current is None:
            return None
        removed_at = _utc_now()
        self._client().insert(
            DEPLOY_REGISTRATION_TABLE,
            [
                [
                    project_id,
                    current.artifact_id,
                    current.profile_id,
                    current.scheduler_backend,
                    removed_at,
                    json.dumps(current.dags, ensure_ascii=False),
                    "removed",
                    _revision(),
                    removed_at,
                ]
            ],
            column_names=[
                "project_id",
                "artifact_id",
                "profile_id",
                "scheduler_backend",
                "registered_at",
                "dags_json",
                "status",
                "revision",
                "updated_at",
            ],
        )
        return current

    def artifact_is_active(self, artifact_id: str) -> bool:
        return any(item.artifact_id == artifact_id for item in self.list_active())


class ClickHouseArtifactRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

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
        self._client().insert(
            ARTIFACT_TABLE,
            [
                [
                    artifact_id,
                    project_id,
                    storage_uri,
                    json.dumps(runtime_connections or [], ensure_ascii=False),
                    json.dumps(dags, ensure_ascii=False),
                    created_at,
                    _revision(),
                    created_at,
                ]
            ],
            column_names=[
                "artifact_id",
                "project_id",
                "storage_uri",
                "runtime_connections_json",
                "dags_json",
                "created_at",
                "revision",
                "updated_at",
            ],
        )
        return ArtifactMetadata(
            artifact_id=artifact_id,
            project_id=project_id,
            storage_uri=storage_uri,
            dags=dags,
            created_at=created_at.isoformat(),
            runtime_connections=runtime_connections or [],
        )

    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                artifact_id,
                project_id,
                storage_uri,
                runtime_connections_json,
                dags_json,
                created_at
            FROM {ARTIFACT_TABLE}
            WHERE artifact_id = {{artifact_id:String}}
            ORDER BY revision DESC, updated_at DESC
            LIMIT 1
            """,
                parameters={"artifact_id": artifact_id},
            )
            .result_rows
        )
        if not rows:
            return None
        (
            artifact_id_,
            project_id,
            storage_uri,
            runtime_connections_json,
            dags_json,
            created_at,
        ) = rows[0]
        return ArtifactMetadata(
            artifact_id=str(artifact_id_),
            project_id=str(project_id),
            storage_uri=str(storage_uri),
            dags=json.loads(str(dags_json)),
            created_at=_datetime_iso(created_at),
            runtime_connections=json.loads(str(runtime_connections_json)),
        )


class ClickHouseOperationReportRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    def save_report(self, report: dict[str, Any]) -> None:
        report_id = str(report.get("operation_id") or "")
        if not report_id:
            return
        created_at = _utc_now()
        self._client().insert(
            OPERATION_REPORT_TABLE,
            [
                [
                    report_id,
                    str(report.get("command") or ""),
                    str(report.get("project_id")) if report.get("project_id") is not None else None,
                    str(report.get("status") or ""),
                    json.dumps(report, ensure_ascii=False, sort_keys=True),
                    _revision(),
                    created_at,
                    created_at,
                ]
            ],
            column_names=[
                "report_id",
                "command",
                "project_id",
                "status",
                "report_json",
                "revision",
                "created_at",
                "updated_at",
            ],
        )


class ClickHouseSecretRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    @contextmanager
    def secret_write_lock(self, secret_key: str) -> Iterator[None]:
        """ClickHouse 는 직렬화 구간을 제공하지 못한다.

        lock 없이 통과시키면 동시 `set_secret` 이 같은 version 을 계산해 서로를
        덮는다. 조용히 지는 대신 닫는다. secret 쓰기는 PostgreSQL metastore 가
        정본이다.
        """
        raise NotImplementedError("secret write serialization requires PostgreSQL metastore")
        yield  # pragma: no cover - 계약 상 도달하지 않는다

    def reencrypt_secret_version(
        self,
        *,
        secret_key: str,
        version: int,
        expected_key_id: str,
        ciphertext: str,
        key_id: str,
    ) -> bool:
        raise NotImplementedError("master key rotation requires PostgreSQL metastore")

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
        self._client().insert(
            SECRET_TABLE,
            [
                [
                    secret_key,
                    int(version),
                    ciphertext,
                    algorithm,
                    key_id,
                    status,
                    created_at,
                    None,
                    _revision(),
                    created_at,
                ]
            ],
            column_names=[
                "secret_key",
                "version",
                "ciphertext",
                "algorithm",
                "key_id",
                "status",
                "created_at",
                "rotated_at",
                "revision",
                "updated_at",
            ],
        )

    def get_active_secret(self, secret_key: str) -> dict[str, Any] | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                secret_key,
                version,
                ciphertext,
                algorithm,
                key_id,
                status,
                created_at,
                rotated_at
            FROM (
                SELECT
                    secret_key,
                    version,
                    tupleElement(latest, 1) AS ciphertext,
                    tupleElement(latest, 2) AS algorithm,
                    tupleElement(latest, 3) AS key_id,
                    tupleElement(latest, 4) AS status,
                    tupleElement(latest, 5) AS created_at,
                    tupleElement(latest, 6) AS rotated_at
                FROM (
                    SELECT
                        secret_key,
                        version,
                        argMax(
                            tuple(ciphertext, algorithm, key_id, status, created_at, rotated_at),
                            tuple(revision, updated_at)
                        ) AS latest
                    FROM {SECRET_TABLE}
                    WHERE secret_key = {{secret_key:String}}
                    GROUP BY secret_key, version
                )
            )
            WHERE status = 'active'
            ORDER BY version DESC
            LIMIT 1
            """,
                parameters={"secret_key": secret_key},
            )
            .result_rows
        )
        if not rows:
            return None
        key, version, ciphertext, algorithm, key_id, status, created_at, rotated_at = rows[0]
        if str(status) != "active":
            return None
        return {
            "secret_key": str(key),
            "version": int(version),
            "ciphertext": str(ciphertext),
            "algorithm": str(algorithm),
            "key_id": str(key_id) if key_id is not None else None,
            "status": str(status),
            "created_at": _datetime_iso(created_at),
            "rotated_at": _datetime_iso(rotated_at) if rotated_at is not None else None,
        }

    def list_secret_metadata(self) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                secret_key,
                version,
                tupleElement(latest, 1) AS algorithm,
                tupleElement(latest, 2) AS key_id,
                tupleElement(latest, 3) AS status,
                tupleElement(latest, 4) AS created_at,
                tupleElement(latest, 5) AS rotated_at
            FROM (
                SELECT
                    secret_key,
                    version,
                    argMax(
                        tuple(algorithm, key_id, status, created_at, rotated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {SECRET_TABLE}
                GROUP BY secret_key, version
            )
            ORDER BY secret_key ASC, version DESC
            """
            )
            .result_rows
        )
        return [
            {
                "secret_key": str(secret_key),
                "version": int(version),
                "algorithm": str(algorithm),
                "key_id": str(key_id) if key_id is not None else None,
                "status": str(status),
                "created_at": _datetime_iso(created_at),
                "rotated_at": _datetime_iso(rotated_at) if rotated_at is not None else None,
            }
            for secret_key, version, algorithm, key_id, status, created_at, rotated_at in rows
        ]


class ClickHouseRunMetadataRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    def create_run(self, run: dict[str, Any]) -> None:
        run_id = str(run.get("run_id") or "")
        if not run_id:
            raise ValueError("run_id is required")
        existing = self.get_run(run_id)
        if existing is not None:
            if existing == run:
                return
            raise ValueError(f"run metadata already exists: {run_id}")

        created_at_raw = run.get("created_at")
        created_at = _parse_datetime(created_at_raw) if created_at_raw else _utc_now()
        updated_at = _utc_now()
        revision = _revision()
        self._client().insert(
            RUN_METADATA_TABLE,
            [
                [
                    run_id,
                    str(run.get("project_id") or ""),
                    str(run.get("job_id") or ""),
                    str(run.get("artifact_id")) if run.get("artifact_id") is not None else None,
                    str(run.get("scheduler_run_id")) if run.get("scheduler_run_id") is not None else None,
                    created_at,
                    json.dumps(run, ensure_ascii=False, sort_keys=True),
                    revision,
                    updated_at,
                ]
            ],
            column_names=[
                "run_id",
                "project_id",
                "job_id",
                "artifact_id",
                "scheduler_run_id",
                "created_at",
                "run_json",
                "revision",
                "updated_at",
            ],
        )

    def update_run(self, run_id: str, patch: dict[str, Any]) -> None:
        if not run_id:
            raise ValueError("run_id is required")
        existing = self.get_run(run_id)
        if existing is None:
            raise ValueError(f"run metadata not found: {run_id}")
        updated = {**existing, **patch, "run_id": run_id}
        created_at_raw = updated.get("created_at")
        created_at = _parse_datetime(created_at_raw) if created_at_raw else _utc_now()
        updated_at = _utc_now()
        revision = _revision()
        self._client().insert(
            RUN_METADATA_TABLE,
            [
                [
                    run_id,
                    str(updated.get("project_id") or ""),
                    str(updated.get("job_id") or ""),
                    str(updated.get("artifact_id")) if updated.get("artifact_id") is not None else None,
                    str(updated.get("scheduler_run_id")) if updated.get("scheduler_run_id") is not None else None,
                    created_at,
                    json.dumps(updated, ensure_ascii=False, sort_keys=True),
                    revision,
                    updated_at,
                ]
            ],
            column_names=[
                "run_id",
                "project_id",
                "job_id",
                "artifact_id",
                "scheduler_run_id",
                "created_at",
                "run_json",
                "revision",
                "updated_at",
            ],
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT run_json
            FROM {RUN_METADATA_TABLE}
            WHERE run_id = {{run_id:String}}
            ORDER BY revision DESC, updated_at DESC
            LIMIT 1
            """,
                parameters={"run_id": run_id},
            )
            .result_rows
        )
        if not rows:
            return None
        return _parse_run_json(rows[0][0])

    def list_runs(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT tupleElement(latest, 1) AS run_json
            FROM (
                SELECT
                    run_id,
                    argMax(
                        tuple(run_json, project_id, job_id, created_at, updated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {RUN_METADATA_TABLE}
                GROUP BY run_id
            )
            WHERE ({{project_id:Nullable(String)}} IS NULL OR tupleElement(latest, 2) = {{project_id:Nullable(String)}})
              AND ({{job_id:Nullable(String)}} IS NULL OR tupleElement(latest, 3) = {{job_id:Nullable(String)}})
            ORDER BY tupleElement(latest, 4) DESC, tupleElement(latest, 5) DESC, run_id DESC
            LIMIT {{limit:UInt32}}
            """,
                parameters={"project_id": project_id, "job_id": job_id, "limit": int(limit)},
            )
            .result_rows
        )
        return [_parse_run_json(row[0]) for row in rows]


class ClickHouseStepExecutionRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

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
        updated_at = _utc_now()
        revision = _revision()
        self._client().insert(
            STEP_EXECUTION_TABLE,
            [
                [
                    project_id,
                    job_id,
                    run_id,
                    step_id,
                    task_id,
                    step_type,
                    int(attempt),
                    status,
                    _parse_datetime(started_at) if started_at else None,
                    _parse_datetime(ended_at) if ended_at else None,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    revision,
                    updated_at,
                ]
            ],
            column_names=[
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "task_id",
                "step_type",
                "attempt",
                "status",
                "started_at",
                "ended_at",
                "metadata_json",
                "revision",
                "updated_at",
            ],
        )

    def list_executions(
        self,
        *,
        project_id: str | None = None,
        job_id: str | None = None,
        run_id: str,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                run_id,
                step_id,
                task_id,
                attempt,
                tupleElement(latest, 1) AS step_type,
                tupleElement(latest, 2) AS status,
                tupleElement(latest, 3) AS started_at,
                tupleElement(latest, 4) AS ended_at,
                tupleElement(latest, 5) AS metadata_json,
                tupleElement(latest, 6) AS revision,
                tupleElement(latest, 7) AS updated_at
            FROM (
                SELECT
                    project_id,
                    job_id,
                    run_id,
                    step_id,
                    task_id,
                    attempt,
                    argMax(
                        tuple(step_type, status, started_at, ended_at, metadata_json, revision, updated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {STEP_EXECUTION_TABLE}
                WHERE run_id = {{run_id:String}}
                  AND ({{project_id:Nullable(String)}} IS NULL OR project_id = {{project_id:Nullable(String)}})
                  AND ({{job_id:Nullable(String)}} IS NULL OR job_id = {{job_id:Nullable(String)}})
                GROUP BY project_id, job_id, run_id, step_id, task_id, attempt
            )
            ORDER BY step_id ASC, task_id ASC, attempt ASC
            """,
                parameters={"project_id": project_id, "job_id": job_id, "run_id": run_id},
            )
            .result_rows
        )
        return [
            {
                "project_id": str(project_),
                "job_id": str(job_id_),
                "run_id": str(run_id_),
                "step_id": str(step_id),
                "task_id": str(task_id),
                "attempt": int(attempt),
                "step_type": str(step_type),
                "status": str(status),
                "started_at": _datetime_iso(started_at) if started_at is not None else None,
                "ended_at": _datetime_iso(ended_at) if ended_at is not None else None,
                "metadata": _parse_metadata_json(metadata_json),
                "revision": int(revision),
                "updated_at": _datetime_iso(updated_at),
            }
            for (
                project_,
                job_id_,
                run_id_,
                step_id,
                task_id,
                attempt,
                step_type,
                status,
                started_at,
                ended_at,
                metadata_json,
                revision,
                updated_at,
            ) in rows
        ]


class ClickHouseStepCheckpointRepository:
    _SELECT_COLUMNS = """
        project_id, job_id, run_id, step_id, task_id, attempt, sequence,
        unit_id, storage_uri, table_identifier, snapshot_id, continuation_json,
        schema_fingerprint, rows, bytes, created_at, revision
    """

    def __init__(self, adapter: ClickHouseMetastoreAdapter) -> None:
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    def append_checkpoint(self, checkpoint: StepCheckpoint) -> None:
        existing = self._checkpoint_for_sequence(checkpoint)
        if existing is not None:
            if existing != checkpoint:
                raise ValueError(
                    "conflicting step checkpoint: "
                    f"{checkpoint.run_id}/{checkpoint.step_id}/{checkpoint.unit_id}/{checkpoint.sequence}"
                )
            return
        self._client().insert(
            STEP_CHECKPOINT_TABLE,
            [
                [
                    checkpoint.project_id,
                    checkpoint.job_id,
                    checkpoint.run_id,
                    checkpoint.step_id,
                    checkpoint.task_id,
                    checkpoint.attempt,
                    checkpoint.sequence,
                    checkpoint.unit_id,
                    checkpoint.storage_uri,
                    checkpoint.table_identifier,
                    checkpoint.snapshot_id,
                    json.dumps(checkpoint.continuation, ensure_ascii=False, sort_keys=True),
                    checkpoint.schema_fingerprint,
                    checkpoint.rows,
                    checkpoint.bytes,
                    _parse_datetime(checkpoint.created_at),
                    _revision(),
                ]
            ],
            column_names=[
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "task_id",
                "attempt",
                "sequence",
                "unit_id",
                "storage_uri",
                "table_identifier",
                "snapshot_id",
                "continuation_json",
                "schema_fingerprint",
                "rows",
                "bytes",
                "created_at",
                "revision",
            ],
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
        rows = (
            self._client()
            .query(
                f"""
            SELECT {self._SELECT_COLUMNS}
            FROM {STEP_CHECKPOINT_TABLE}
            WHERE project_id = {{project_id:String}}
              AND job_id = {{job_id:String}}
              AND run_id = {{run_id:String}}
              AND step_id = {{step_id:String}}
              AND task_id = {{task_id:String}}
              AND unit_id = {{unit_id:String}}
            ORDER BY sequence DESC, revision DESC
            LIMIT 1
            """,
                parameters={
                    "project_id": project_id,
                    "job_id": job_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "task_id": task_id,
                    "unit_id": unit_id,
                },
            )
            .result_rows
        )
        return self._checkpoint(rows[0]) if rows else None

    def list_checkpoints(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str,
    ) -> list[StepCheckpoint]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT {self._SELECT_COLUMNS}
            FROM {STEP_CHECKPOINT_TABLE}
            WHERE project_id = {{project_id:String}}
              AND job_id = {{job_id:String}}
              AND run_id = {{run_id:String}}
              AND step_id = {{step_id:String}}
            ORDER BY unit_id ASC, sequence ASC, revision DESC
            LIMIT 1 BY unit_id, sequence
            """,
                parameters={
                    "project_id": project_id,
                    "job_id": job_id,
                    "run_id": run_id,
                    "step_id": step_id,
                },
            )
            .result_rows
        )
        return [self._checkpoint(row) for row in rows]

    def _checkpoint_for_sequence(self, checkpoint: StepCheckpoint) -> StepCheckpoint | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT {self._SELECT_COLUMNS}
            FROM {STEP_CHECKPOINT_TABLE}
            WHERE project_id = {{project_id:String}}
              AND job_id = {{job_id:String}}
              AND run_id = {{run_id:String}}
              AND step_id = {{step_id:String}}
              AND task_id = {{task_id:String}}
              AND unit_id = {{unit_id:String}}
              AND sequence = {{sequence:UInt32}}
            ORDER BY revision DESC
            LIMIT 1
            """,
                parameters={
                    "project_id": checkpoint.project_id,
                    "job_id": checkpoint.job_id,
                    "run_id": checkpoint.run_id,
                    "step_id": checkpoint.step_id,
                    "task_id": checkpoint.task_id,
                    "unit_id": checkpoint.unit_id,
                    "sequence": checkpoint.sequence,
                },
            )
            .result_rows
        )
        return self._checkpoint(rows[0]) if rows else None

    @staticmethod
    def _checkpoint(row: tuple[Any, ...]) -> StepCheckpoint:
        return StepCheckpoint(
            project_id=str(row[0]),
            job_id=str(row[1]),
            run_id=str(row[2]),
            step_id=str(row[3]),
            task_id=str(row[4]),
            attempt=int(row[5]),
            sequence=int(row[6]),
            unit_id=str(row[7]),
            storage_uri=str(row[8]),
            table_identifier=str(row[9]),
            snapshot_id=int(row[10]),
            continuation=dict(json.loads(str(row[11]))),
            schema_fingerprint=str(row[12]),
            rows=int(row[13]),
            bytes=int(row[14]),
            created_at=_datetime_iso(row[15]),
        )


class ClickHouseStepStateRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

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
        updated_at = _utc_now()
        self._client().insert(
            STEP_STATE_TABLE,
            [
                [
                    project_id,
                    job_id,
                    step_id,
                    state_key,
                    state_type,
                    json.dumps(state_value, sort_keys=True),
                    run_id,
                    _revision(),
                    updated_at,
                ]
            ],
            column_names=[
                "project_id",
                "job_id",
                "step_id",
                "state_key",
                "state_type",
                "state_value_json",
                "run_id",
                "revision",
                "updated_at",
            ],
        )

    def get_state(
        self,
        *,
        project_id: str,
        job_id: str,
        step_id: str,
        state_key: str,
    ) -> dict[str, Any] | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                step_id,
                state_key,
                state_type,
                state_value_json,
                run_id,
                revision,
                updated_at
            FROM {STEP_STATE_TABLE}
            WHERE project_id = {{project_id:String}}
              AND job_id = {{job_id:String}}
              AND step_id = {{step_id:String}}
              AND state_key = {{state_key:String}}
            ORDER BY revision DESC, updated_at DESC
            LIMIT 1
            """,
                parameters={
                    "project_id": project_id,
                    "job_id": job_id,
                    "step_id": step_id,
                    "state_key": state_key,
                },
            )
            .result_rows
        )
        if not rows:
            return None
        return self._row_to_state(rows[0])

    def list_states(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                step_id,
                state_key,
                tupleElement(latest, 1) AS state_type,
                tupleElement(latest, 2) AS state_value_json,
                tupleElement(latest, 3) AS run_id,
                tupleElement(latest, 4) AS revision,
                tupleElement(latest, 5) AS updated_at
            FROM (
                SELECT
                    project_id,
                    job_id,
                    step_id,
                    state_key,
                    argMax(
                        tuple(state_type, state_value_json, run_id, revision, updated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {STEP_STATE_TABLE}
                WHERE project_id = {{project_id:String}}
                  AND ({{job_id:Nullable(String)}} IS NULL OR job_id = {{job_id:Nullable(String)}})
                  AND ({{step_id:Nullable(String)}} IS NULL OR step_id = {{step_id:Nullable(String)}})
                GROUP BY project_id, job_id, step_id, state_key
            )
            ORDER BY job_id ASC, step_id ASC, state_key ASC
            """,
                parameters={"project_id": project_id, "job_id": job_id, "step_id": step_id},
            )
            .result_rows
        )
        return [self._row_to_state(row) for row in rows]

    def _row_to_state(self, row: tuple[Any, ...]) -> dict[str, Any]:
        (
            project_id,
            job_id,
            step_id,
            state_key,
            state_type,
            state_value_json,
            run_id,
            revision,
            updated_at,
        ) = row
        return {
            "project_id": str(project_id),
            "job_id": str(job_id),
            "step_id": str(step_id),
            "state_key": str(state_key),
            "state_type": str(state_type),
            "state_value": _parse_json_value(state_value_json),
            "run_id": str(run_id),
            "revision": int(revision),
            "updated_at": _datetime_iso(updated_at),
        }


class ClickHouseStepEventRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

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
        event_at = _parse_datetime(created_at) if created_at else _utc_now()
        updated_at = _utc_now()
        self._client().insert(
            STEP_EVENT_TABLE,
            [
                [
                    project_id,
                    job_id,
                    run_id,
                    step_id,
                    task_id,
                    event_type,
                    status,
                    json.dumps(event, ensure_ascii=False, sort_keys=True),
                    event_at,
                    _revision(),
                    updated_at,
                ]
            ],
            column_names=[
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "task_id",
                "event_type",
                "status",
                "event_json",
                "created_at",
                "revision",
                "updated_at",
            ],
        )

    def list_events(
        self,
        *,
        project_id: str,
        job_id: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                run_id,
                step_id,
                task_id,
                event_type,
                status,
                event_json,
                created_at,
                revision,
                updated_at
            FROM {STEP_EVENT_TABLE}
            WHERE project_id = {{project_id:String}}
              AND ({{job_id:Nullable(String)}} IS NULL OR job_id = {{job_id:Nullable(String)}})
              AND ({{event_type:Nullable(String)}} IS NULL OR event_type = {{event_type:Nullable(String)}})
            ORDER BY created_at DESC, revision DESC
            LIMIT {{limit:UInt32}}
            """,
                parameters={
                    "project_id": project_id,
                    "job_id": job_id,
                    "event_type": event_type,
                    "limit": max(1, min(int(limit), 500)),
                },
            )
            .result_rows
        )
        return [self._row_to_event(row) for row in rows]

    def _row_to_event(self, row: tuple[Any, ...]) -> dict[str, Any]:
        (
            project_id,
            job_id,
            run_id,
            step_id,
            task_id,
            event_type,
            status,
            event_json,
            created_at,
            revision,
            updated_at,
        ) = row
        return {
            "project_id": str(project_id),
            "job_id": str(job_id),
            "run_id": str(run_id),
            "step_id": str(step_id),
            "task_id": str(task_id),
            "event_type": str(event_type),
            "status": str(status),
            "event": _parse_metadata_json(event_json),
            "created_at": _datetime_iso(created_at),
            "revision": int(revision),
            "updated_at": _datetime_iso(updated_at),
        }


class ClickHouseStepOutputBindingRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

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
        updated_at = _utc_now()
        self._client().insert(
            STEP_OUTPUT_BINDING_TABLE,
            [
                [
                    project_id,
                    job_id,
                    run_id,
                    step_id,
                    output_name,
                    output_kind,
                    json.dumps(binding, ensure_ascii=False, sort_keys=True),
                    _revision(),
                    updated_at,
                ]
            ],
            column_names=[
                "project_id",
                "job_id",
                "run_id",
                "step_id",
                "output_name",
                "output_kind",
                "binding_json",
                "revision",
                "updated_at",
            ],
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
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                run_id,
                step_id,
                output_name,
                output_kind,
                binding_json,
                revision,
                updated_at
            FROM {STEP_OUTPUT_BINDING_TABLE}
            WHERE project_id = {{project_id:String}}
              AND job_id = {{job_id:String}}
              AND run_id = {{run_id:String}}
              AND step_id = {{step_id:String}}
              AND output_name = {{output_name:String}}
            ORDER BY revision DESC, updated_at DESC
            LIMIT 1
            """,
                parameters={
                    "project_id": project_id,
                    "job_id": job_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "output_name": output_name,
                },
            )
            .result_rows
        )
        if not rows:
            return None
        return self._row_to_binding(rows[0])

    def list_bindings(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        step_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                job_id,
                run_id,
                step_id,
                output_name,
                tupleElement(latest, 1) AS output_kind,
                tupleElement(latest, 2) AS binding_json,
                tupleElement(latest, 3) AS revision,
                tupleElement(latest, 4) AS updated_at
            FROM (
                SELECT
                    project_id,
                    job_id,
                    run_id,
                    step_id,
                    output_name,
                    argMax(
                        tuple(output_kind, binding_json, revision, updated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {STEP_OUTPUT_BINDING_TABLE}
                WHERE project_id = {{project_id:String}}
                  AND job_id = {{job_id:String}}
                  AND run_id = {{run_id:String}}
                  AND ({{step_id:Nullable(String)}} IS NULL OR step_id = {{step_id:Nullable(String)}})
                GROUP BY project_id, job_id, run_id, step_id, output_name
            )
            ORDER BY step_id ASC, output_name ASC
            """,
                parameters={"project_id": project_id, "job_id": job_id, "run_id": run_id, "step_id": step_id},
            )
            .result_rows
        )
        return [self._row_to_binding(row) for row in rows]

    def _row_to_binding(self, row: tuple[Any, ...]) -> dict[str, Any]:
        (
            project_id,
            job_id,
            run_id,
            step_id,
            output_name,
            output_kind,
            binding_json,
            revision,
            updated_at,
        ) = row
        return {
            "project_id": str(project_id),
            "job_id": str(job_id),
            "run_id": str(run_id),
            "step_id": str(step_id),
            "output_name": str(output_name),
            "output_kind": str(output_kind),
            "binding": _parse_metadata_json(binding_json),
            "revision": int(revision),
            "updated_at": _datetime_iso(updated_at),
        }


class ClickHouseBackendRegistryRepository:
    def __init__(self, adapter: ClickHouseMetastoreAdapter):
        self.adapter = adapter

    def _client(self):
        return self.adapter.client()

    def upsert_backend(
        self,
        *,
        project_id: str,
        backend_id: str,
        backend_type: str,
        backend: dict[str, Any],
        status: str,
    ) -> None:
        updated_at = _utc_now()
        self._client().insert(
            BACKEND_REGISTRY_TABLE,
            [
                [
                    project_id,
                    backend_id,
                    backend_type,
                    json.dumps(backend, ensure_ascii=False, sort_keys=True),
                    status,
                    _revision(),
                    updated_at,
                ]
            ],
            column_names=[
                "project_id",
                "backend_id",
                "backend_type",
                "backend_json",
                "status",
                "revision",
                "updated_at",
            ],
        )

    def get_backend(
        self,
        *,
        project_id: str,
        backend_id: str,
    ) -> dict[str, Any] | None:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                backend_id,
                backend_type,
                backend_json,
                status,
                revision,
                updated_at
            FROM {BACKEND_REGISTRY_TABLE}
            WHERE project_id = {{project_id:String}}
              AND backend_id = {{backend_id:String}}
            ORDER BY revision DESC, updated_at DESC
            LIMIT 1
            """,
                parameters={"project_id": project_id, "backend_id": backend_id},
            )
            .result_rows
        )
        if not rows:
            return None
        return self._row_to_backend(rows[0])

    def list_backends(
        self,
        *,
        project_id: str,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = (
            self._client()
            .query(
                f"""
            SELECT
                project_id,
                backend_id,
                tupleElement(latest, 1) AS backend_type,
                tupleElement(latest, 2) AS backend_json,
                tupleElement(latest, 3) AS status,
                tupleElement(latest, 4) AS revision,
                tupleElement(latest, 5) AS updated_at
            FROM (
                SELECT
                    project_id,
                    backend_id,
                    argMax(
                        tuple(backend_type, backend_json, status, revision, updated_at),
                        tuple(revision, updated_at)
                    ) AS latest
                FROM {BACKEND_REGISTRY_TABLE}
                WHERE project_id = {{project_id:String}}
                GROUP BY project_id, backend_id
            )
            WHERE ({{status:Nullable(String)}} IS NULL OR tupleElement(latest, 3) = {{status:Nullable(String)}})
            ORDER BY backend_id ASC
            """,
                parameters={"project_id": project_id, "status": status},
            )
            .result_rows
        )
        return [self._row_to_backend(row) for row in rows]

    def _row_to_backend(self, row: tuple[Any, ...]) -> dict[str, Any]:
        (
            project_id,
            backend_id,
            backend_type,
            backend_json,
            status,
            revision,
            updated_at,
        ) = row
        return {
            "project_id": str(project_id),
            "backend_id": str(backend_id),
            "backend_type": str(backend_type),
            "backend": _parse_metadata_json(backend_json),
            "status": str(status),
            "revision": int(revision),
            "updated_at": _datetime_iso(updated_at),
        }


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _parse_run_json(value: Any) -> dict[str, Any]:
    data = json.loads(str(value))
    if not isinstance(data, dict):
        raise ValueError("run metadata JSON must be an object")
    return data


def _parse_metadata_json(value: Any) -> dict[str, Any]:
    data = json.loads(str(value or "{}"))
    return data if isinstance(data, dict) else {}


def _parse_json_value(value: Any) -> Any:
    return json.loads(str(value))
