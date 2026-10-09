"""zeta4s API 서버."""

from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationError
import uvicorn

from zeta4s.api.services.artifact_store import (
    ZETA4S_API_HOME,
    artifact_root,
    decode_bundle,
    ensure_artifact_runtime_permissions,
    extract_bundle,
    record_artifact_metadata,
)
from zeta4s.api.services.registration_store import (
    load_registrations,
    remove_project_registration,
    upsert_project_registration,
)
from zeta4s.api.services.locks import OperationLockTimeout, project_operation_lock
from zeta4s.api.services.run_store import (
    create_run,
    get_run,
    list_runs,
    new_run_id,
    new_run_metadata_base,
    run_display_fields,
)
from zeta4s.api.services.scheduler_runs import RunCapabilityUnsupported, adapter_for_registration
from zeta4s.airflow.rest_client import AirflowRestTimeout
from zeta4s.api.metrics import family_header, render_prometheus_text, sample
from zeta4s.common.time_display import (
    display_timezone,
    display_timezone_name,
    format_display_time,
    localize_log_timestamps,
)
from zeta4s.config.profile_config import scheduler_backend_from_profile, validate_profile
from zeta4s.metastore.factory import metastore_adapter_factory
from zeta4s.dbt.graph import dbt_executable, dbt_parse_env, selected_dbt_step_selections, sync_dbt_graph_cache
from zeta4s.dbt.model_contract import validate_dbt_model_contract
from zeta4s.project.bundle import inspect_project, validate_project_configs
from zeta4s.project.loader import load_project_context
from zeta4s.project.step_graph import validate_step_graph_configs
from zeta4s.project.step_types import (
    register_installed_step_types,
    reset_step_type_registry,
)
from zeta4s.runtime.connection_policy import (
    airflow_connection_projection,
    password_ref,
    runtime_connection_policy_from_profile,
)
from cryptography.exceptions import InvalidTag

from zeta4s.runtime.dbt_profiles import dbt_profile_connection_from_profile, render_dbt_profiles_yml
from zeta4s.runtime.secrets import (
    EncryptedSecretStore,
    check_master_key_file,
    master_key_file_path,
)
from zeta4s.runtime.task_result import load_task_results


logger = logging.getLogger(__name__)
_METRICS_TEXT_CACHE: tuple[float, str] | None = None
_METRICS_TEXT_LOCK = threading.Lock()
_PROGRESS_LOCAL = threading.local()


class DeployRequest(BaseModel):
    project_id: str
    bundle_base64: str
    profile_id: str
    profile: dict[str, Any]


class ProfileCheckRequest(BaseModel):
    profile: dict[str, Any]


class SecretSetRequest(BaseModel):
    secret_key: str
    value: str


class SecretCheckRequest(BaseModel):
    secret_key: str


class RunCreateRequest(BaseModel):
    parameters: dict[str, Any] = Field(default_factory=dict)


class RunNoteSyncRequest(BaseModel):
    adapter_job_id: str
    scheduler_run_id: str
    result_run_id: str


class RuntimeStepExecuteRequest(BaseModel):
    project_id: str
    artifact_id: str
    profile_id: str
    job_id: str
    step_id: str
    run_id: str
    attempt: int = 1
    parameters: dict[str, Any] = Field(default_factory=dict)


class RuntimeRunFinalizeRequest(BaseModel):
    project_id: str
    artifact_id: str
    profile_id: str
    job_id: str
    run_id: str


def _server_report_id(prefix: str) -> str:
    timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def _operation_report(
    *,
    command: str,
    project: str | None,
    status: str = "passed",
    summary: dict[str, Any] | None = None,
    steps: list[dict[str, Any]] | None = None,
    issues: list[dict[str, Any]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "operation_id": extra.pop("operation_id", _server_report_id("op")),
        "status": status,
        "command": command,
        "project": project,
        "project_id": project,
        "summary": summary or {},
        "steps": steps or [],
        "issues": issues or [],
        "next_commands": [],
        **extra,
    }


def _compact_detail(detail: Any) -> str:
    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        for key in ("message", "error", "reason"):
            value = detail.get(key)
            if isinstance(value, str) and value:
                return value
        if len(detail) == 1:
            key, value = next(iter(detail.items()))
            if isinstance(value, str):
                return f"{key}: {value}"
            if isinstance(value, dict):
                for nested_key in ("stderr", "stdout", "reason", "status"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, str) and nested_value:
                        return f"{key}: {nested_value}"
                return f"{key}: {json.dumps(value, ensure_ascii=False, sort_keys=True)}"
        return json.dumps(detail, ensure_ascii=False, sort_keys=True)
    return str(detail)


def _inspect_metastore_schema(adapter: Any) -> dict[str, Any]:
    inspect_schema = getattr(adapter, "inspect_schema", None)
    if not callable(inspect_schema):
        return {"status": "unknown"}
    schema = inspect_schema()
    if not isinstance(schema, dict):
        return {"status": "unknown"}
    return schema


def _metastore_not_ready_report(command: str, project: str | None) -> dict[str, Any] | None:
    adapter = metastore_adapter_factory()
    try:
        schema = _inspect_metastore_schema(adapter)
    except Exception as e:
        schema = {"status": "error", "error": str(e)}
    schema_status = str(schema.get("status") or "unknown")
    if schema_status == "ok":
        return None
    issue = {
        "code": "Z4S_METASTORE_NOT_BOOTSTRAPPED",
        "severity": "error",
        "message": "Metastore is not ready. Run z4s api bootstrap before this operation.",
        "step": "metastore_ready",
        "details": {"schema": schema},
    }
    return _operation_report(
        command=command,
        project=project,
        status="failed",
        summary={
            "bootstrap_status": "not_ready",
            "schema_status": schema_status,
            "hint": "Run z4s api bootstrap before this operation.",
        },
        steps=[_step("metastore_ready", "failed", {"schema": schema}, [issue["code"]])],
        issues=[issue],
        schema=schema,
    )


def _runtime_request_error(
    *,
    command: str,
    project: str | None,
    code: str,
    step: str,
    detail: Any,
    status_code: int = 400,
) -> HTTPException:
    message = _compact_detail(detail)
    report = _operation_report(
        command=command,
        project=project,
        status="failed",
        summary={"detail": detail},
        steps=[_step(step, "failed", {"detail": detail}, [code])],
        issues=[
            {
                "code": code,
                "severity": "error",
                "message": message,
                "step": step,
                "details": {"detail": detail},
            }
        ],
    )
    return HTTPException(status_code=status_code, detail=_emit_operation_report(report))


def _runtime_http_error(
    *,
    command: str,
    project: str | None,
    code: str,
    step: str,
    error: HTTPException,
) -> HTTPException:
    return _runtime_request_error(
        command=command,
        project=project,
        code=code,
        step=step,
        detail=error.detail,
        status_code=error.status_code,
    )


def _require_runtime_internal_token(authorization: str | None) -> None:
    expected = os.environ.get("ZETA4S_RUNTIME_INTERNAL_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="runtime internal token is not configured")
    if authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="unauthorized")


def _runtime_connection_policy_by_conn_id(conn_id: str) -> dict[str, Any]:
    from zeta4s.api.services.registration_store import load_registrations

    matches: list[dict[str, Any]] = []
    registrations = load_registrations().get("registrations") or []
    for registration in registrations:
        if not isinstance(registration, dict):
            continue
        # This endpoint projects the native Connection shape consumed by the Airflow adapter.
        # Its public route and authentication are runtime-neutral, while projection ownership
        # remains adapter-specific.
        if registration.get("scheduler_backend") != "airflow":
            continue
        artifact_id = str(registration.get("artifact_id") or "")
        if not artifact_id:
            continue
        data = _load_artifact_metadata(artifact_id)
        for policy in data.get("runtime_connections") or []:
            if isinstance(policy, dict) and str(policy.get("conn_id") or "") == conn_id:
                matches.append(dict(policy))
    if not matches:
        raise HTTPException(status_code=404, detail=f"runtime connection is not active: {conn_id}")
    if len(matches) > 1:
        raise HTTPException(
            status_code=409, detail=f"runtime connection is ambiguous across active deployments: {conn_id}"
        )
    return matches[0]


def _resolve_runtime_connection(conn_id: str) -> dict[str, Any]:
    policy = _runtime_connection_policy_by_conn_id(conn_id)
    ref = password_ref(policy)
    password = EncryptedSecretStore().resolve_secret(ref) if ref else None
    return airflow_connection_projection(policy, password=password)


def _active_runtime_execution(
    *,
    project_id: str,
    artifact_id: str,
    profile_id: str,
    job_id: str,
) -> tuple[Path, Any, Any, dict[str, Any]]:
    registration = _active_project_registration(project_id)
    if registration is None:
        raise HTTPException(status_code=404, detail=f"active deployment not found: {project_id}")
    expected = {
        "artifact_id": artifact_id,
        "profile_id": profile_id,
    }
    for key, value in expected.items():
        if str(registration.get(key) or "") != value:
            raise HTTPException(status_code=409, detail=f"stale deployment identity: {project_id} {key}")
    active_jobs = {
        str(item.get("job_id"))
        for item in registration.get("dags") or []
        if isinstance(item, dict) and item.get("job_id")
    }
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail=f"active job not found: {project_id}/{job_id}")

    from zeta4s.prefect.runtime import load_scheduled_plan

    # 설치된 외부 step type 을 plan 빌드(StepGraphJob 검증) 이전에 등록한다. runtime-step API
    # 경로는 profile 을 재구성하지 않으므로 설치 discovery 로 외부 type 을 해결한다.
    register_installed_step_types()
    project_root = _project_root_for_artifact(artifact_id, project_id)
    plan, project = load_scheduled_plan(project_root, job_id)
    metadata = _load_artifact_metadata(artifact_id)
    connections = {}
    for policy in metadata.get("runtime_connections") or []:
        if not isinstance(policy, dict) or not policy.get("conn_id"):
            continue
        connection = dict(policy)
        extra = connection.get("extra") or {}
        if isinstance(extra, dict) and extra.get("password_ref"):
            connection["password_ref"] = extra["password_ref"]
        connections[str(policy["conn_id"])] = connection
    return project_root, plan, project, {"connections": connections}


def _execute_runtime_step(request: RuntimeStepExecuteRequest) -> dict[str, Any]:
    from zeta4s.prefect.runtime import run_scheduled_step, start_scheduled_run

    try:
        _, plan, project, profile_data = _active_runtime_execution(
            project_id=request.project_id,
            artifact_id=request.artifact_id,
            profile_id=request.profile_id,
            job_id=request.job_id,
        )
        if request.step_id not in plan.step_by_id:
            raise HTTPException(status_code=404, detail=f"step not found: {request.step_id}")
        registration = _active_project_registration(request.project_id) or {}
        scheduler = str(registration.get("scheduler_backend") or "internal")
        start_scheduled_run(
            project,
            plan,
            request.run_id,
            request.profile_id,
            parameters=request.parameters,
            scheduler=scheduler,
            scheduler_run_id=request.run_id,
        )
        return run_scheduled_step(
            project=project,
            plan=plan,
            step_id=request.step_id,
            run_id=request.run_id,
            profile=request.profile_id,
            profile_data=profile_data,
            attempt=max(1, request.attempt),
            adapter_attempt=1,
            adapter=scheduler,
            parameters=request.parameters,
        )
    except (KeyError, RuntimeError, ValueError) as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


def _finalize_runtime_run(request: RuntimeRunFinalizeRequest) -> dict[str, Any]:
    from zeta4s.prefect.runtime import finalize_scheduled_run

    _, plan, project, _ = _active_runtime_execution(
        project_id=request.project_id,
        artifact_id=request.artifact_id,
        profile_id=request.profile_id,
        job_id=request.job_id,
    )
    return finalize_scheduled_run(project, plan, request.run_id, request.profile_id)


def _report_plan_fields(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if key != "project"}


def _report_summary(report: dict[str, Any]) -> dict[str, Any]:
    summary = report.get("summary")
    return summary if isinstance(summary, dict) else {}


def _step(
    name: str,
    status: str = "passed",
    summary: dict[str, Any] | None = None,
    issue_codes: list[str] | None = None,
) -> dict[str, Any]:
    return {"name": name, "status": status, "summary": summary or {}, "issue_codes": issue_codes or []}


def _inspect_iceberg_rowset_store() -> dict[str, str]:
    from zeta4s.runtime.rowset_stores.iceberg import IcebergRowsetStore

    return IcebergRowsetStore.from_environment().inspect()


def _steps_with_remaining(steps: list[dict[str, Any]], step_names: list[str]) -> list[dict[str, Any]]:
    covered = {str(step.get("name") or "") for step in steps}
    return [
        *steps,
        *[_step(name, "skipped") for name in step_names if name not in covered],
    ]


def _deploy_steps_with_remaining(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _steps_with_remaining(steps, DEPLOY_PROGRESS_STEPS)


def _undeploy_steps_with_remaining(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _steps_with_remaining(steps, UNDEPLOY_PROGRESS_STEPS)


def _api_log(identifier: str, operation: str, project: str | None, step: str, status: str, message: str) -> None:
    print(f"{identifier} {operation} {project or '-'} {step} {status} {message}", flush=True)


_SECRET_REPORT_KEYS = {
    "authorization",
    "bearer_token",
    "encrypted_connection",
    "extra",
    "key",
    "password",
    "secret",
    "token",
}
_REDACTED = "<redacted>"


def _sanitize_operation_report_for_persistence(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return _sanitize_operation_report_for_persistence(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return _sanitize_operation_report_for_persistence(asdict(value))
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            key_lower = key_text.lower()
            if any(secret_key in key_lower for secret_key in _SECRET_REPORT_KEYS):
                sanitized[key_text] = _REDACTED
                continue
            sanitized[key_text] = _sanitize_operation_report_for_persistence(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_operation_report_for_persistence(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_operation_report_for_persistence(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _emit_operation_report(report: dict[str, Any]) -> dict[str, Any]:
    report = _sanitize_operation_report_for_persistence(report)
    identifier = str(report.get("operation_id") or "-")
    command = str(report.get("command") or "-")
    project = str(report.get("project") or "")
    steps = report.get("steps") or report.get("probes") or []
    if isinstance(steps, list):
        for step in steps:
            if not isinstance(step, dict):
                continue
            _api_log(
                identifier,
                command,
                project,
                str(step.get("name") or "-"),
                str(step.get("status") or report.get("status") or "-"),
                json.dumps(step.get("summary") or {}, ensure_ascii=False, sort_keys=True),
            )
    _api_log(
        identifier,
        command,
        project,
        "complete",
        str(report.get("status") or "-"),
        json.dumps(report.get("summary") or {}, ensure_ascii=False, sort_keys=True),
    )
    try:
        metastore_adapter_factory().operation_report_repository.save_report(report)
    except Exception:
        logger.exception("failed to persist operation report to metastore")
    return report


def _event_time() -> str:
    return datetime.now(timezone.utc).isoformat()


def _progress_event(
    event: str,
    *,
    name: str | None = None,
    status: str | None = None,
    summary: dict[str, Any] | None = None,
    report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = getattr(_PROGRESS_LOCAL, "plan", {}) or {}
    steps = plan.get("steps") or []
    index_by_name = plan.get("index_by_name") or {}
    step_index = index_by_name.get(name) if name else None
    payload: dict[str, Any] = {
        "event": event,
        "event_time": _event_time(),
        "command": plan.get("command"),
        "project": plan.get("project"),
        "operation": plan.get("operation"),
        "step": name,
        "status": status,
        "summary": summary or {},
        "index": step_index,
        "total": len(steps),
    }
    if report is not None:
        payload["report"] = report
    return payload


def _progress_emit(event: dict[str, Any]) -> None:
    emit = getattr(_PROGRESS_LOCAL, "emit", None)
    if emit is not None:
        emit(event)


def _progress_running(name: str, summary: dict[str, Any] | None = None) -> None:
    _progress_emit(_progress_event("step", name=name, status="running", summary=summary))


def _progress_step(name: str, status: str = "passed", summary: dict[str, Any] | None = None) -> None:
    _progress_emit(_progress_event("step", name=name, status=status, summary=summary))


def _stream_operation(
    *,
    command: str,
    project: str | None,
    operation: str,
    steps: list[str],
    fn,
) -> StreamingResponse:
    events: queue.Queue[dict[str, Any] | None] = queue.Queue()
    plan = {
        "command": command,
        "project": project,
        "operation": operation,
        "steps": steps,
        "index_by_name": {name: index for index, name in enumerate(steps, start=1)},
    }

    def emit(event: dict[str, Any]) -> None:
        events.put(event)

    def worker() -> None:
        _PROGRESS_LOCAL.emit = emit
        _PROGRESS_LOCAL.plan = plan
        try:
            report = fn()
            emit(_progress_event("complete", status=str(report.get("status") or "passed"), report=report))
        except HTTPException as e:
            detail = e.detail
            if isinstance(detail, dict) and isinstance(detail.get("issues"), list):
                report = detail
            else:
                report = _operation_report(
                    command=command,
                    project=project,
                    status="failed",
                    summary={"detail": detail},
                    steps=[_step("request", "failed", {"detail": detail})],
                    issues=[
                        {"code": "Z4E_RUNTIME_STREAM_001", "severity": "error", "message": _compact_detail(detail)}
                    ],
                )
            emit(_progress_event("complete", status="failed", report=report))
        except Exception as e:
            report = _operation_report(
                command=command,
                project=project,
                status="failed",
                summary={"detail": str(e)},
                steps=[_step("request", "failed", {"detail": str(e)})],
                issues=[{"code": "Z4E_RUNTIME_STREAM_001", "severity": "error", "message": str(e)}],
            )
            emit(_progress_event("complete", status="failed", report=report))
        finally:
            _PROGRESS_LOCAL.emit = None
            _PROGRESS_LOCAL.plan = None
            events.put(None)

    def body():
        yield (
            json.dumps(
                {
                    "event": "plan",
                    "event_time": _event_time(),
                    "command": command,
                    "project": project,
                    "operation": operation,
                    "step": None,
                    "status": None,
                    "summary": {"steps": steps},
                    "index": None,
                    "total": len(steps),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        while True:
            event = events.get()
            if event is None:
                break
            yield json.dumps(event, ensure_ascii=False) + "\n"

    return StreamingResponse(body(), media_type="application/x-ndjson; charset=utf-8")


DEPLOY_PROGRESS_STEPS = [
    "bundle_build",
    "project_check",
    "profile_check",
    "dag_pause",
    "active_run_terminate",
    "airflow_register_prepare",
    "connections_apply",
    "project_pools_apply",
    "dbt_validate",
    "artifact_register",
    "scheduler_deploy",
    "dag_discovery",
    "dag_unpause",
]

UNDEPLOY_PROGRESS_STEPS = [
    "project_registration",
    "dag_pause",
    "active_run_terminate",
    "scheduler_cleanup",
    "registration_remove",
]

REDEPLOY_PROGRESS_STEPS = ["undeploy", "deploy"]


def _require_token(authorization: str | None) -> None:
    expected = os.environ.get("ZETA4S_API_TOKEN")
    if not expected:
        return
    if authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="invalid API token")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _deploy_dag_discovery_timeout_seconds() -> float:
    return _env_float("ZETA4S_DEPLOY_DAG_DISCOVERY_TIMEOUT_SECONDS", 120.0)


def _deploy_dag_state_timeout_seconds() -> float:
    return _env_float("ZETA4S_DEPLOY_DAG_STATE_TIMEOUT_SECONDS", 60.0)


def _undeploy_dag_state_timeout_seconds() -> float:
    return _env_float("ZETA4S_UNDEPLOY_DAG_STATE_TIMEOUT_SECONDS", 60.0)


def _undeploy_active_terminate_timeout_seconds() -> float:
    return _env_float("ZETA4S_UNDEPLOY_ACTIVE_TERMINATE_TIMEOUT_SECONDS", 300.0)


def _undeploy_dag_delete_timeout_seconds() -> float:
    return _env_float("ZETA4S_UNDEPLOY_DAG_DELETE_TIMEOUT_SECONDS", 120.0)


def _airflow_operation_poll_seconds() -> float:
    return _env_float("ZETA4S_AIRFLOW_OPERATION_POLL_SECONDS", 2.0)


def _undeploy_active_terminate_poll_seconds() -> float:
    return _env_float("ZETA4S_AIRFLOW_ACTIVE_TERMINATE_POLL_SECONDS", 10.0)


def _project_root_for_artifact(artifact_id: str, project_id: str) -> Path:
    return artifact_root(ZETA4S_API_HOME, artifact_id) / "projects" / project_id


def _load_artifact_metadata(artifact_id: str) -> dict[str, Any]:
    artifact = metastore_adapter_factory().artifact_repository.get_artifact(artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"artifact metadata not found: {artifact_id}")
    return {
        "artifact_id": artifact.artifact_id,
        "project_id": artifact.project_id,
        "storage_uri": artifact.storage_uri,
        "runtime_connections": artifact.runtime_connections,
        "dags": artifact.dags,
        "created_at": artifact.created_at,
    }


def _artifact_id_for_project(project_id: str) -> str | None:
    registration = _active_project_registration(project_id)
    if registration is None:
        return None
    artifact_id = registration.get("artifact_id")
    return str(artifact_id) if artifact_id else None


def _active_project_registration(project_id: str) -> dict[str, Any] | None:
    data = load_registrations()
    for item in data.get("registrations", []):
        if isinstance(item, dict) and item.get("project_id") == project_id:
            return dict(item)
    return None


def _deploy_prefect_jobs(
    *,
    plan: dict[str, Any],
    artifact_id: str,
    project_root: Path,
    profile_id: str,
) -> list[Any]:
    from zeta4s.prefect import ScheduleIdentity
    from zeta4s.prefect.prefect_engine import delete_prefect_job, deploy_prefect_job, sync_prefect_concurrency_limits
    from zeta4s.prefect.runtime import load_scheduled_plan

    sync_prefect_concurrency_limits(str(project_root))

    deployments = []
    attempted = []
    try:
        for dag in plan["dags"]:
            job_id = str(dag["job_id"])
            scheduled_plan, project = load_scheduled_plan(project_root, job_id)
            identity = ScheduleIdentity(project.project_id, job_id, profile_id)
            attempted.append(identity)
            deployments.append(
                deploy_prefect_job(
                    identity=identity,
                    plan=scheduled_plan,
                    artifact_id=artifact_id,
                )
            )
    except Exception as deploy_error:
        rollback_errors = []
        for identity in reversed(attempted):
            try:
                delete_prefect_job(identity)
            except Exception as rollback_error:
                rollback_errors.append(f"{identity.key}: {rollback_error}")
        if rollback_errors:
            raise RuntimeError(
                "Prefect deploy failed and rollback did not converge: " + "; ".join(rollback_errors)
            ) from deploy_error
        raise
    return deployments


def _airflow_register_prepare(
    profile: dict[str, Any], project_root: Path | None = None, *, replace: bool = True
) -> dict[str, Any]:
    from zeta4s.airflow.assets import apply_project_pools

    project_pool_payloads = apply_project_pools(project_root) if project_root else []
    return {
        "connection_count": 0,
        "pool_count": 0,
        "project_pool_count": len(project_pool_payloads),
        "project_pools": project_pool_payloads,
        "status": "applied",
    }


def _airflow_pause_and_terminate(
    project: str,
    *,
    dag_state_timeout_seconds: float,
    terminate_timeout_seconds: float,
    dag_poll_seconds: float,
    terminate_poll_seconds: float,
) -> dict[str, Any]:
    """project 의 DAG 을 모두 멈추고 살아 있는 run 을 끝낸다.

    undeploy 와 runtime reset 이 같은 수렴을 쓴다.
    """
    from zeta4s.airflow.dags import (
        converge_project_active_runs_terminated,
        converge_project_dags_paused,
        list_zeta4s_dags,
    )

    dag_ids = list_zeta4s_dags(project=project)
    dag_pause = converge_project_dags_paused(
        project,
        dag_ids,
        paused=True,
        timeout_seconds=dag_state_timeout_seconds,
        poll_interval_seconds=dag_poll_seconds,
    )
    active_run_terminate = (
        {"status": "skipped", "remaining_runs": [], "remaining_task_instances": []}
        if dag_pause.get("status") != "passed"
        else converge_project_active_runs_terminated(
            project,
            dag_ids,
            timeout_seconds=terminate_timeout_seconds,
            poll_interval_seconds=terminate_poll_seconds,
        )
    )
    return {"dag_ids": dag_ids, "dag_pause": dag_pause, "active_run_terminate": active_run_terminate}


def _airflow_discover_and_unpause(
    project: str,
    expected_dag_ids: list[str],
    *,
    expected_artifact_id: str,
    dag_discovery_timeout_seconds: float,
    dag_state_timeout_seconds: float,
    dag_poll_seconds: float,
) -> dict[str, Any]:
    """DAG 이 dagbag 에 올라오길 기다렸다가 푼다. 올라오지 않으면 풀 것이 없다."""
    from zeta4s.airflow.dags import converge_project_dag_discovery, converge_project_dags_paused

    dag_discovery = converge_project_dag_discovery(
        project,
        expected_dag_ids,
        expected_artifact_id=expected_artifact_id,
        timeout_seconds=dag_discovery_timeout_seconds,
        poll_interval_seconds=dag_poll_seconds,
    )
    dag_unpause = (
        {"status": "skipped", "requested_paused": False, "not_converged_dags": expected_dag_ids}
        if dag_discovery.get("status") != "passed"
        else converge_project_dags_paused(
            project,
            expected_dag_ids,
            paused=False,
            timeout_seconds=dag_state_timeout_seconds,
            poll_interval_seconds=dag_poll_seconds,
        )
    )
    return {"dag_discovery": dag_discovery, "dag_unpause": dag_unpause}


def _airflow_prepare_steps(summary: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    summary = summary or {}
    return [
        _step("connections_apply", summary={"connection_count": summary.get("connection_count", 0)}),
        _step(
            "project_pools_apply",
            summary={
                "pool_count": summary.get("project_pool_count", 0),
                "pools": summary.get("project_pools", []),
            },
        ),
    ]


def _config_connection_ids(config: dict[str, Any]) -> set[str]:
    conn_ids: set[str] = set()
    for step in config.get("steps") or []:
        if not isinstance(step, dict):
            continue
        if step.get("conn"):
            conn_ids.add(str(step["conn"]))
        api = step.get("api")
        if isinstance(api, dict) and api.get("conn"):
            conn_ids.add(str(api["conn"]))
    return conn_ids


def _dbt_conn_ids(config_items: list[tuple[Path, dict[str, Any]]]) -> list[str]:
    return sorted({selection.conn_id for selection in selected_dbt_step_selections(config_items)})


def _dbt_models_by_conn(config_items: list[tuple[Path, dict[str, Any]]]) -> dict[str, tuple[list[str], list[str]]]:
    by_conn: dict[str, tuple[list[str], list[str]]] = {}
    for _, config in config_items:
        steps = config.get("steps") if isinstance(config, dict) else None
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict) or step.get("type") not in {"dbt.run", "dbt.test"}:
                continue
            conn_id = str(step.get("conn") or "").strip()
            if not conn_id:
                continue
            run_models, test_models = by_conn.setdefault(conn_id, ([], []))
            target = run_models if step.get("type") == "dbt.run" else test_models
            for model in step.get("models") or []:
                model_name = str(model).strip()
                if model_name and model_name not in target:
                    target.append(model_name)
    return by_conn


def _project_check_report(
    project_root: Path,
    *,
    profile_id: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    project = None
    issues: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    config_items: list[tuple[Path, dict[str, Any]]] = []

    def gate(name: str, fn) -> dict[str, Any]:
        try:
            summary = fn() or {}
            steps.append(_step(name, "passed", summary))
            return summary
        except Exception as e:
            code = f"Z4S_PROJECT_CHECK_{name.upper()}_FAILED"
            issue = {
                "code": code,
                "severity": "error",
                "message": str(e),
                "step": name,
                "details": {"detail": str(e)},
            }
            issues.append(issue)
            steps.append(_step(name, "failed", issue["details"], [code]))
            return {}

    def project_structure() -> dict[str, Any]:
        nonlocal project
        project = load_project_context(project_root)
        if not project.jobs_dir.exists():
            raise ValueError(f"jobs directory is required: {project.jobs_dir}")
        return {"project": project.project_id}

    def step_types() -> dict[str, Any]:
        try:
            validate_profile(profile)
            # 설치된 zeta4s.step_types 플러그인을 config 검증 전에 등록한다. Airflow/Prefect
            # 어느 backend 든 설치가 곧 등록이므로 backend 별 분기가 없다.
            registered_count = register_installed_step_types()
        except Exception:
            # 실패 시 이전 registry 가 뒤 gate 에 새지 않게 한다.
            reset_step_type_registry()
            raise
        return {"registered": registered_count}

    def yaml_schema() -> dict[str, Any]:
        nonlocal config_items
        config_items = validate_project_configs(project_root)
        return {"configs": len(config_items)}

    def step_graph_schema() -> dict[str, Any]:
        return validate_step_graph_configs(config_items, project_root)

    def dbt_model_contract() -> dict[str, Any]:
        if project is None:
            return {"models": 0, "required": False}
        if not _project_requires_dbt(config_items):
            return {"models": 0, "required": False}
        checked = 0
        models_by_conn = _dbt_models_by_conn(config_items)
        for conn_id in _dbt_conn_ids(config_items):
            run_models, test_models = models_by_conn.get(conn_id, ([], []))
            checked += validate_dbt_model_contract(
                project.dbt_project_dir(conn_id),
                run_models=run_models,
                test_models=test_models,
            ).checked
        return {"models": checked, "required": True}

    def profile_connections() -> dict[str, Any]:
        required_connections: set[str] = set()
        for _, config in config_items:
            required_connections.update(_config_connection_ids(config))
        defined_connections = set((profile.get("connections") or {}).keys())
        missing_connections = sorted(required_connections - defined_connections)
        if missing_connections:
            raise ValueError("missing profile connections: " + ", ".join(missing_connections))
        return {
            "profile": profile_id,
            "connections": len(defined_connections),
            "required_connections": sorted(required_connections),
        }

    gate("step_types", step_types)
    gate("project_structure", project_structure)
    gate("yaml_schema", yaml_schema)
    gate("step_graph_schema", step_graph_schema)
    gate("dbt_model_contract", dbt_model_contract)
    gate("profile_connections", profile_connections)
    return {
        "status": "failed" if issues else "passed",
        "summary": {
            "gate_count": len(steps),
            "error_count": len(issues),
            "profile": profile_id,
        },
        "steps": steps,
        "issues": issues,
    }


def _profile_check_report(profile: dict[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    try:
        validate_profile(profile, source=None)
    except ValueError as e:
        issue = {
            "code": "Z4S_PROFILE_SCHEMA_INVALID",
            "severity": "error",
            "message": str(e),
            "step": "profile_validate",
            "details": {"detail": str(e)},
        }
        report = _operation_report(
            command="z4s profile check",
            project=None,
            status="failed",
            summary={"connection_count": 0, "secret_ref_count": 0},
            steps=[_step("profile_validate", "failed", {"detail": str(e)}, [issue["code"]])],
            issues=[issue],
        )
        report.pop("project", None)
        report.pop("project_id", None)
        return report

    policies = runtime_connection_policy_from_profile(profile)
    steps.append(_step("profile_validate", "passed", {"connection_count": len(policies)}))

    secret_checks: list[dict[str, Any]] = []
    store = EncryptedSecretStore()
    for policy in policies:
        conn_id = str(policy.get("conn_id") or "")
        ref = password_ref(policy)
        if not ref:
            continue
        try:
            status = store.check_secret(ref)
        except Exception as e:  # noqa: BLE001 - keep checking the rest of the profile
            status = {"secret_key": ref, "active": False, "decryptable": False, "error": str(e)}
        item = {
            "conn_id": conn_id,
            "secret_key": ref,
            "active": bool(status.get("active")),
            "decryptable": bool(status.get("decryptable")),
        }
        if status.get("error"):
            item["error"] = status["error"]
        secret_checks.append(item)
        if not item["active"] or not item["decryptable"]:
            issues.append(
                {
                    "code": "Z4S_PROFILE_SECRET_INVALID",
                    "severity": "error",
                    "message": f"profile secret check failed: {conn_id} ({ref})",
                    "step": "secrets_check",
                    "details": item,
                }
            )
    failed_secret_count = sum(1 for item in secret_checks if not item["active"] or not item["decryptable"])
    steps.append(
        _step(
            "secrets_check",
            "failed" if failed_secret_count else "passed",
            {"secret_count": len(secret_checks), "failed_count": failed_secret_count, "secrets": secret_checks},
            ["Z4S_PROFILE_SECRET_INVALID"] if failed_secret_count else [],
        )
    )

    from zeta4s.airflow.runtime_check import check_profile_api

    connection_checks = [check.__dict__ for check in check_profile_api(policies)]
    failed_connection_count = sum(1 for check in connection_checks if not check.get("ok"))
    for check in connection_checks:
        if check.get("ok"):
            continue
        issues.append(
            {
                "code": "Z4S_PROFILE_CONNECTION_FAILED",
                "severity": "error",
                "message": (
                    f"profile connection check failed: {check.get('conn_id')} "
                    f"({check.get('kind')}): {check.get('detail')}"
                ),
                "step": "connections_check",
                "details": check,
            }
        )
    steps.append(
        _step(
            "connections_check",
            "failed" if failed_connection_count else "passed",
            {
                "connection_count": len(connection_checks),
                "failed_count": failed_connection_count,
                "checks": connection_checks,
            },
            ["Z4S_PROFILE_CONNECTION_FAILED"] if failed_connection_count else [],
        )
    )

    report = _operation_report(
        command="z4s profile check",
        project=None,
        status="failed" if issues else "passed",
        summary={
            "connection_count": len(policies),
            "secret_ref_count": len(secret_checks),
            "secret_failed_count": failed_secret_count,
            "connection_failed_count": failed_connection_count,
        },
        steps=steps,
        issues=issues,
    )
    report.pop("project", None)
    report.pop("project_id", None)
    return report


def _sync_backend_registry(project_id: str, runtime_connections: list[dict[str, Any]]) -> dict[str, int]:
    repository = metastore_adapter_factory().backend_registry_repository
    active_before = {
        str(item.get("backend_id")): item
        for item in repository.list_backends(project_id=project_id, status="active")
        if item.get("backend_id")
    }
    current_ids: set[str] = set()
    for policy in runtime_connections:
        backend_id = str(policy.get("conn_id") or "")
        backend_type = str(policy.get("type") or policy.get("conn_type") or "")
        if not backend_id or not backend_type:
            raise ValueError(f"runtime backend requires conn_id and type: {policy}")
        current_ids.add(backend_id)
        repository.upsert_backend(
            project_id=project_id,
            backend_id=backend_id,
            backend_type=backend_type,
            backend=dict(policy),
            status="active",
        )
    removed_count = 0
    for backend_id, item in active_before.items():
        if backend_id in current_ids:
            continue
        repository.upsert_backend(
            project_id=project_id,
            backend_id=backend_id,
            backend_type=str(item.get("backend_type") or ""),
            backend=dict(item.get("backend") or {}),
            status="removed",
        )
        removed_count += 1
    return {"active_backend_count": len(current_ids), "removed_backend_count": removed_count}


def _inspect_project_or_400(project_root: Path) -> dict[str, Any]:
    try:
        return inspect_project(project_root)
    except (ValueError, ValidationError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _dbt_validate_report(project_root: Path, profile: dict[str, Any]) -> dict[str, Any]:
    project = load_project_context(project_root)
    config_items = validate_project_configs(project_root)
    if not _project_requires_dbt(config_items):
        return {"status": "skipped", "reason": "project has no dbt transform"}
    env = dbt_parse_env()
    profiles_yml_by_conn = _dbt_profiles_yml_by_profile(profile)
    checks = []
    for conn_id in sorted({selection.conn_id for selection in selected_dbt_step_selections(config_items)}):
        dbt_project_dir = project.dbt_project_dir(conn_id)
        if not (dbt_project_dir / "dbt_project.yml").exists():
            raise HTTPException(status_code=400, detail={"dbt_project": f"{dbt_project_dir}/dbt_project.yml not found"})
        with tempfile.TemporaryDirectory(prefix="zeta4s-dbt-") as work_dir:
            profiles_dir = Path(work_dir) / "profiles"
            profiles_dir.mkdir(parents=True, exist_ok=True)
            (profiles_dir / "profiles.yml").write_text(
                _dbt_parse_profiles_yml(conn_id, profiles_yml_by_conn),
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    dbt_executable(),
                    "--log-path",
                    str(Path(work_dir) / "logs"),
                    "parse",
                    "--profiles-dir",
                    str(profiles_dir),
                    "--project-dir",
                    str(dbt_project_dir),
                    "--target-path",
                    str(Path(work_dir) / "target"),
                ],
                cwd=str(dbt_project_dir),
                capture_output=True,
                text=True,
                timeout=300,
                env=env,
            )
        check = {
            "conn": conn_id,
            "cwd": str(dbt_project_dir),
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        checks.append(check)
        if result.returncode != 0:
            raise HTTPException(status_code=400, detail={"dbt_project": check})
    try:
        graph_paths, graph_count = sync_dbt_graph_cache(
            project_root,
            config_items,
            env=env,
            profiles_yml_by_conn=profiles_yml_by_conn,
        )
    except (RuntimeError, ValueError) as e:
        raise HTTPException(status_code=400, detail={"dbt_graph_cache": str(e)}) from e
    return {
        "status": "ok",
        "check": "dbt_validate",
        "projects": checks,
        "graph_cache": {
            "paths": {conn: str(path) for conn, path in graph_paths.items()},
            "graphs": graph_count,
        },
    }


def _dbt_profiles_yml_by_profile(profile: dict[str, Any]) -> dict[str, str]:
    connections = profile.get("connections") or {}
    if not isinstance(connections, dict):
        raise HTTPException(status_code=400, detail="profile.connections must be a mapping")
    return {
        str(conn_id): render_dbt_profiles_yml(dbt_profile_connection_from_profile(str(conn_id), connection))
        for conn_id, connection in connections.items()
        if isinstance(connection, dict) and connection.get("type") in {"clickhouse", "oracle"}
    }


def _dbt_parse_profiles_yml(conn_id: str, profiles_yml_by_conn: dict[str, str]) -> str:
    try:
        return profiles_yml_by_conn[conn_id]
    except KeyError as e:
        raise HTTPException(
            status_code=400,
            detail={"dbt_project": f"dbt connection must be defined as a supported dbt asset connection: {conn_id}"},
        ) from e


def _project_requires_dbt(config_items: list[tuple[Path, dict[str, Any]]]) -> bool:
    return any(
        isinstance(step, dict) and step.get("type") in {"dbt.run", "dbt.test"}
        for _, config in config_items
        for step in (config.get("steps") or [])
    )


def _timezone_or_400(timezone: str | None) -> str:
    try:
        name = display_timezone_name(timezone)
        display_timezone(name)
        return name
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _airflow_probe_sentinel(error: Exception, key: str) -> dict[str, Any]:
    """조회 실패를 sentinel 상태로 바꾼다.

    Airflow 가 느린 것(timeout)과 붙을 수 없는 것(unavailable)은 다른 상황이라 나눈다.
    조회는 관측용이므로 실패해도 응답 자체를 깨뜨리지 않는다.
    """
    return {key: "timeout" if isinstance(error, AirflowRestTimeout) else "unavailable"}


def _airflow_run_state(run: dict[str, Any], timezone: str | None = None) -> dict[str, Any]:
    project_id = run.get("project_id")
    if project_id:
        reg = _active_project_registration(project_id)
        if reg and reg.get("scheduler_backend") == "prefect":
            from zeta4s.prefect.prefect_engine import prefect_run_state

            try:
                return prefect_run_state(run, timezone, format_display_time)
            except Exception as e:
                return _airflow_probe_sentinel(e, "airflow_state")

    from zeta4s.airflow import runs as airflow_runs

    try:
        data = airflow_runs.dag_run_state(run["dag_id"], _airflow_run_id(run))
    except Exception as e:  # noqa: BLE001 - 조회 실패는 sentinel 로 보고한다
        return _airflow_probe_sentinel(e, "airflow_state")
    if data is None:
        return {"airflow_state": "not_found"}
    start_date = data.get("airflow_start_date")
    end_date = data.get("airflow_end_date")
    return {
        **data,
        "airflow_duration_seconds": _duration_seconds(start_date, end_date),
        "airflow_start_date_display": format_display_time(start_date, timezone),
        "airflow_end_date_display": format_display_time(end_date, timezone),
    }


def _run_with_airflow_state(run: dict[str, Any], timezone: str | None = None) -> dict[str, Any]:
    display_timezone = _timezone_or_400(timezone)
    return {
        **run,
        "display_timezone": display_timezone,
        **run_display_fields(run, display_timezone),
        **_airflow_run_state(run, display_timezone),
    }


def _task_terminal_state(state: str | None) -> bool:
    return state in {"success", "failed", "skipped", "upstream_failed", "removed"}


def _airflow_task_instances(run: dict[str, Any], timezone: str | None = None) -> dict[str, Any]:
    from zeta4s.airflow import runs as airflow_runs

    try:
        tasks = airflow_runs.task_instances(run["dag_id"], _airflow_run_id(run))
    except Exception as e:  # noqa: BLE001 - 조회 실패는 sentinel 로 보고한다
        return {**_airflow_probe_sentinel(e, "task_state"), "tasks": []}
    if tasks is None:
        return {"task_state": "not_found", "tasks": []}
    for task in tasks:
        task["started_at"] = task.get("start_date")
        task["ended_at"] = task.get("end_date")
        task["duration_seconds"] = task.get("duration")
        task["start_date_display"] = format_display_time(task.get("start_date"), timezone)
        task["end_date_display"] = format_display_time(task.get("end_date"), timezone)
    return {
        "task_state": "available",
        "tasks": tasks,
        "all_tasks_terminal": bool(tasks) and all(_task_terminal_state(task.get("state")) for task in tasks),
    }


def _project_job_from_dag_id(dag_id: str) -> tuple[str, str]:
    if "__" not in dag_id:
        raise HTTPException(status_code=400, detail="dag_id must be <project_id>__<job_id>")
    project_id, job_id = dag_id.split("__", 1)
    if not project_id or not job_id:
        raise HTTPException(status_code=400, detail="dag_id must be <project_id>__<job_id>")
    return project_id, job_id


def _normalize_zeta4s_run(run: dict[str, Any], dag_id: str | None = None) -> dict[str, Any]:
    stored_dag_id = str(run.get("dag_id") or "")
    if stored_dag_id:
        resolved_dag_id = stored_dag_id
        parsed_project_id, parsed_job_id = _project_job_from_dag_id(stored_dag_id)
    else:
        project_id_from_run = str(run.get("project_id") or "")
        job_id_from_run = str(run.get("job_id") or "")
        if not project_id_from_run or not job_id_from_run:
            raise HTTPException(
                status_code=400, detail="run metadata must include project_id and job_id when dag_id is absent"
            )
        parsed_project_id = project_id_from_run
        parsed_job_id = job_id_from_run
        resolved_dag_id = f"{parsed_project_id}__{parsed_job_id}"
    airflow_run_id = str(run.get("airflow_run_id") or run.get("run_id") or "")
    return {
        **run,
        "source": str(run.get("source") or "zeta4s"),
        "airflow_run_id": airflow_run_id,
        "project_id": str(run.get("project_id") or parsed_project_id),
        "job_id": str(run.get("job_id") or parsed_job_id),
        "dag_id": resolved_dag_id,
    }


def _airflow_dag_run_to_run(dag_id: str, dag_run: dict[str, Any]) -> dict[str, Any]:
    project_id, job_id = _project_job_from_dag_id(dag_id)
    run_id = str(dag_run.get("run_id") or "")
    created_at = (
        dag_run.get("queued_at") or dag_run.get("start_date") or dag_run.get("logical_date") or dag_run.get("run_after")
    )
    return {
        "source": "airflow",
        "run_id": run_id,
        "airflow_run_id": run_id,
        "artifact_id": None,
        "project_id": project_id,
        "job_id": job_id,
        "dag_id": dag_id,
        "conf": dag_run.get("conf") if isinstance(dag_run.get("conf"), dict) else {},
        "created_at": created_at,
        "airflow_state": dag_run.get("state"),
        "airflow_start_date": dag_run.get("start_date"),
        "airflow_end_date": dag_run.get("end_date"),
        "logical_date": dag_run.get("logical_date"),
        "run_type": dag_run.get("run_type"),
        "data_interval_start": dag_run.get("data_interval_start"),
        "data_interval_end": dag_run.get("data_interval_end"),
    }


def _airflow_dag_run(dag_id: str, run_id: str) -> dict[str, Any] | None:
    from zeta4s.airflow import runs as airflow_runs

    row = airflow_runs.dag_run(dag_id, run_id)
    return _airflow_dag_run_to_run(dag_id, row) if row else None


def _airflow_dag_runs(dag_id: str, limit: int) -> list[dict[str, Any]]:
    from zeta4s.airflow import runs as airflow_runs

    return [_airflow_dag_run_to_run(dag_id, row) for row in airflow_runs.dag_runs(dag_id, limit) if row.get("run_id")]


def _resolve_run_or_404(dag_id: str, run_id: str) -> dict[str, Any]:
    run_metadata = get_run(run_id)
    if run_metadata is not None:
        run = _normalize_zeta4s_run(run_metadata, dag_id=dag_id)
        if run.get("dag_id") == dag_id:
            return run

    airflow_run = _airflow_dag_run(dag_id, run_id)
    if airflow_run is not None:
        return airflow_run
    raise HTTPException(status_code=404, detail=f"run not found for dag_id={dag_id}: {run_id}")


def _dag_runs(dag_id: str, limit: int) -> list[dict[str, Any]]:
    project_id, job_id = _project_job_from_dag_id(dag_id)
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for run in _airflow_dag_runs(dag_id, limit):
        merged[(dag_id, _airflow_run_id(run))] = run
    for run in list_runs(limit=limit, project_id=project_id, job_id=job_id):
        normalized = _normalize_zeta4s_run(run, dag_id=dag_id)
        if normalized.get("dag_id") != dag_id:
            continue
        merged[(dag_id, _airflow_run_id(normalized))] = normalized
    runs = sorted(
        merged.values(),
        key=lambda item: str(
            item.get("created_at")
            or item.get("airflow_start_date")
            or item.get("logical_date")
            or item.get("run_id")
            or ""
        ),
        reverse=True,
    )
    return runs[:limit]


def _project_dag_ids(project: str) -> list[str]:
    from zeta4s.airflow.dags import list_zeta4s_dags

    return [str(dag_id) for dag_id in list_zeta4s_dags(project=project)]


def _project_runs(project_id: str, job_id: str | None, limit: int) -> list[dict[str, Any]]:
    dag_ids = [f"{project_id}__{job_id}"] if job_id else _project_dag_ids(project_id)
    runs: list[dict[str, Any]] = []
    for dag_id in dag_ids:
        runs.extend(_dag_runs(dag_id, limit))
    runs.sort(
        key=lambda item: str(
            item.get("created_at")
            or item.get("airflow_start_date")
            or item.get("logical_date")
            or item.get("run_id")
            or ""
        ),
        reverse=True,
    )
    return runs[:limit]


def _resolve_project_run_or_404(project_id: str, run_id: str, job_id: str | None = None) -> dict[str, Any]:
    if job_id:
        return _resolve_run_or_404(f"{project_id}__{job_id}", run_id)
    for run in _project_runs(project_id, job_id, limit=500):
        if run.get("run_id") == run_id or run.get("airflow_run_id") == run_id:
            return run
    raise HTTPException(status_code=404, detail=f"run_id not found for project_id={project_id}: {run_id}")


def _airflow_run_id(run: dict[str, Any]) -> str:
    return str(run.get("airflow_run_id") or run["run_id"])


def _run_registration(project_id: str) -> dict[str, Any]:
    registration = _active_project_registration(project_id)
    if registration is None:
        raise HTTPException(status_code=404, detail=f"active deployment not found: {project_id}")
    return registration


def _registered_job(registration: dict[str, Any], job_id: str) -> bool:
    return any(
        isinstance(item, dict) and str(item.get("job_id") or "") == job_id for item in registration.get("dags") or []
    )


def _canonical_run(run: dict[str, Any], timezone_name: str | None = None) -> dict[str, Any]:
    registration = _run_registration(str(run["project_id"]))
    adapter = adapter_for_registration(registration)
    try:
        snapshot = adapter.get_run(run).as_dict()
    except Exception as error:  # noqa: BLE001 - observation failures become explicit sentinel states
        state = "timeout" if isinstance(error, AirflowRestTimeout) else "unavailable"
        snapshot = {
            "project_id": str(run["project_id"]),
            "job_id": str(run["job_id"]),
            "run_id": str(run["run_id"]),
            "scheduler": str(registration.get("scheduler_backend") or ""),
            "scheduler_run_id": str(run.get("scheduler_run_id") or run["run_id"]),
            "state": state,
            "created_at": run.get("created_at"),
            "started_at": run.get("started_at"),
            "ended_at": run.get("ended_at"),
            "duration_seconds": run.get("duration_seconds"),
            "parameters": dict(run.get("parameters") or {}),
            "adapter_metadata": {"observation_error": type(error).__name__},
        }
    stored = {
        key: value
        for key, value in run.items()
        if key
        not in {
            "status",
            "profile",
            "scheduler",
            "scheduler_run_id",
            "state",
            "created_at",
            "started_at",
            "ended_at",
            "duration_seconds",
            "parameters",
            "adapter_metadata",
        }
    }
    display_tz = _timezone_or_400(timezone_name)
    return {
        **stored,
        **snapshot,
        "display_timezone": display_tz,
        "created_at_display": format_display_time(snapshot.get("created_at"), display_tz),
        "started_at_display": format_display_time(snapshot.get("started_at"), display_tz),
        "ended_at_display": format_display_time(snapshot.get("ended_at"), display_tz),
    }


def _canonical_project_runs(
    project_id: str, job_id: str | None, limit: int, timezone_name: str | None
) -> list[dict[str, Any]]:
    _run_registration(project_id)
    rows = list_runs(limit=limit, project_id=project_id, job_id=job_id)
    return [_canonical_run(row, timezone_name) for row in rows]


def _canonical_run_or_404(project_id: str, run_id: str, job_id: str | None = None) -> dict[str, Any]:
    run = get_run(run_id)
    if run is None or str(run.get("project_id") or "") != project_id:
        raise HTTPException(status_code=404, detail=f"run_id not found for project_id={project_id}: {run_id}")
    if job_id and str(run.get("job_id") or "") != job_id:
        raise HTTPException(
            status_code=404, detail=f"run_id not found for project_id={project_id} job_id={job_id}: {run_id}"
        )
    return run


def _run_capability_error(error: RunCapabilityUnsupported) -> HTTPException:
    return HTTPException(
        status_code=501,
        detail={"code": error.code, "operation": error.operation, "scheduler": error.scheduler, "message": str(error)},
    )


def _canonical_tasks(run: dict[str, Any]) -> list[dict[str, Any]]:
    registration = _run_registration(str(run["project_id"]))
    adapter = adapter_for_registration(registration)
    try:
        return adapter.list_tasks(run)
    except RunCapabilityUnsupported as error:
        raise _run_capability_error(error) from error


def _canonical_summary(run: dict[str, Any], timezone_name: str | None) -> dict[str, Any]:
    canonical = _canonical_run(run, timezone_name)
    tasks = _canonical_tasks(run)
    counts = {
        state: sum(1 for task in tasks if task.get("state") == state) for state in ("succeeded", "failed", "skipped")
    }
    return {
        "operation_id": f"summary_{run['run_id']}",
        "command": "z4s api run summary",
        "project": run["project_id"],
        "project_id": run["project_id"],
        "job_id": run["job_id"],
        "run_id": run["run_id"],
        "status": "failed" if canonical["state"] == "failed" else "passed",
        "summary": {
            "state": canonical["state"],
            "started_at": canonical.get("started_at"),
            "ended_at": canonical.get("ended_at"),
            "duration_seconds": canonical.get("duration_seconds"),
            "tasks": len(tasks),
            **counts,
        },
        "run": canonical,
        "tasks": tasks,
        "issues": [],
    }


def _filtered_log_entries(
    run: dict[str, Any],
    *,
    task_id: str | None = None,
    failed_only: bool = False,
    latest_attempt_only: bool = False,
    tail: int | None = None,
    timezone: str | None = None,
) -> list[dict[str, Any]]:
    """Airflow task log 를 REST 로 읽는다.

    REST 는 시도를 하나씩 지목해야 하므로 task instance 의 `try_number` 로 시도를 열거한다
    — 그것이 마지막 시도 번호다.
    """
    from zeta4s.airflow import runs as airflow_runs

    task_state = _airflow_task_instances(run, timezone)
    tasks = [task for task in task_state.get("tasks") or [] if task.get("task_id")]
    failed_states = {"failed", "upstream_failed", "up_for_retry"}
    airflow_run_id = _airflow_run_id(run)

    entries: list[dict[str, Any]] = []
    for task in tasks:
        current_task_id = str(task.get("task_id"))
        if task_id and current_task_id != task_id:
            continue
        if failed_only and task.get("state") not in failed_states:
            continue
        latest_attempt = int(task.get("try_number") or 0)
        if latest_attempt <= 0:
            continue
        attempts = [latest_attempt] if latest_attempt_only else list(range(1, latest_attempt + 1))
        for attempt in attempts:
            try:
                lines = airflow_runs.task_log_lines(
                    run["dag_id"],
                    airflow_run_id,
                    current_task_id,
                    attempt,
                    map_index=task.get("map_index"),
                )
            except Exception:  # noqa: BLE001 - 로그는 관측용이라 한 시도가 실패해도 나머지를 준다
                logger.warning(
                    "Failed to read Airflow task log: dag_id=%r task_id=%r attempt=%r",
                    run["dag_id"],
                    current_task_id,
                    attempt,
                    exc_info=True,
                )
                continue
            if lines is None:
                continue
            entries.append(
                {
                    "task_id": current_task_id,
                    "try_number": attempt,
                    "map_index": task.get("map_index"),
                    "state": task.get("state"),
                    "content": localize_log_timestamps(_tail_lines(lines, tail), timezone),
                }
            )
    return entries


def _tail_lines(lines: list[str], tail: int | None) -> str:
    if tail is not None and tail > 0:
        lines = lines[-tail:]
    return "\n".join(lines)


def _numeric_metric(value: Any) -> int | float:
    return value if isinstance(value, (int, float)) else 0


def _timestamp_seconds(value: Any) -> float | None:
    if not value:
        return None
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _duration_seconds(start: Any, end: Any) -> float | None:
    start_ts = _timestamp_seconds(start)
    end_ts = _timestamp_seconds(end)
    if start_ts is None or end_ts is None or end_ts < start_ts:
        return None
    return round(end_ts - start_ts, 6)


def _earliest_timestamp_value(values: list[Any]) -> Any:
    candidates = [(ts, value) for value in values if (ts := _timestamp_seconds(value)) is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])[1]


def _latest_timestamp_value(values: list[Any]) -> Any:
    candidates = [(ts, value) for value in values if (ts := _timestamp_seconds(value)) is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _run_execution_summary(run_state: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = run_state.get("airflow_start_date") or _earliest_timestamp_value(
        [task.get("start_date") for task in tasks]
    )
    ended_at = run_state.get("airflow_end_date")
    if ended_at is None and tasks and _all_tasks_terminal(tasks):
        ended_at = _latest_timestamp_value([task.get("end_date") for task in tasks])
    return {
        "status": _effective_run_status(str(run_state.get("airflow_state") or ""), tasks),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": _duration_seconds(started_at, ended_at),
    }


def _all_tasks_terminal(tasks: list[dict[str, Any]]) -> bool:
    terminal_states = {"success", "failed", "skipped", "upstream_failed", "removed"}
    task_states = [str(task.get("state") or "") for task in tasks if isinstance(task, dict)]
    return bool(task_states) and all(state in terminal_states for state in task_states)


def _effective_run_status(run_state: str, tasks: list[dict[str, Any]]) -> str:
    if run_state in {"success", "failed"}:
        return run_state
    task_states = [str(task.get("state") or "") for task in tasks if isinstance(task, dict) and task.get("state")]
    if not task_states:
        return run_state or "-"
    terminal_states = {"success", "failed", "skipped", "upstream_failed", "removed"}
    if any(state not in terminal_states for state in task_states):
        return run_state or "-"
    if any(state in {"failed", "upstream_failed"} for state in task_states):
        return "failed"
    return "success"


def _registered_runtime_jobs() -> list[dict[str, Any]]:
    data = load_registrations()
    jobs: list[dict[str, Any]] = []
    for registration in data.get("registrations", []):
        if not isinstance(registration, dict):
            continue
        project = str(registration.get("project_id") or "")
        artifact_id = str(registration.get("artifact_id") or "")
        registered_at = registration.get("registered_at")
        scheduler = str(registration.get("scheduler_backend") or "")
        for job in registration.get("dags") or []:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("job_id") or "")
            if not job_id:
                continue
            jobs.append(
                {
                    "project": project,
                    "job": job_id,
                    "scheduler": scheduler,
                    "artifact_id": artifact_id,
                    "registered_at": registered_at,
                }
            )
    return jobs


def _metric_headers() -> list[str]:
    families = [
        ("zeta4s_project_registered", "Registered z4s project. Value is always 1 for registered projects."),
        ("zeta4s_job_registered", "Registered zeta4s job. Value is always 1 for registered jobs."),
        ("zeta4s_artifact_registered_mtime_seconds", "Project artifact registration timestamp as Unix seconds."),
        ("zeta4s_latest_run_state", "Latest run state. Exactly one state label has value 1 for a latest run."),
        ("zeta4s_latest_run_duration_seconds", "Latest run duration in seconds."),
        ("zeta4s_latest_run_failed_tasks", "Failed task count in the latest run."),
        ("zeta4s_latest_run_success_tasks", "Successful task count in the latest run."),
        ("zeta4s_latest_run_input_rows", "Input row total in the latest run."),
        ("zeta4s_latest_run_output_rows", "Output row total in the latest run."),
        ("zeta4s_latest_run_failed_rows", "Failed row total in the latest run."),
        ("zeta4s_task_result_state", "Latest task state. Value is always 1 for the observed task state."),
        ("zeta4s_task_duration_seconds", "Latest task duration in seconds."),
        ("zeta4s_task_input_rows", "Latest task input rows."),
        ("zeta4s_task_output_rows", "Latest task output rows."),
        ("zeta4s_task_failed_rows", "Latest task failed rows."),
        ("zeta4s_metrics_scrape_error", "zeta4s-api metrics collection error marker. 1 means a scrape section failed."),
    ]
    lines: list[str] = []
    for name, help_text in families:
        lines.extend(family_header(name, help_text))
    return lines


def _metrics_cache_ttl_seconds() -> float:
    return float(os.environ.get("ZETA4S_METRICS_CACHE_SECONDS", "30"))


def _latest_run_metric_lines(job: dict[str, Any], run: dict[str, Any] | None, tasks: list[dict[str, Any]]) -> list[str]:
    project = str(job["project"])
    job_id = str(job["job"])
    base_labels = {"project": project, "job": job_id}
    if run is None:
        return []

    lines: list[str] = []
    task_results = _task_results_by_task_id(str(run["run_id"]))

    state = str(run.get("state") or "unknown")
    lines.append(sample("zeta4s_latest_run_state", 1, {**base_labels, "state": state}))
    duration = run.get("duration_seconds")
    if duration is None:
        duration = _duration_seconds(run.get("started_at"), run.get("ended_at"))
    if duration is not None:
        lines.append(sample("zeta4s_latest_run_duration_seconds", duration, base_labels))

    success_tasks = 0
    failed_tasks = 0
    row_totals = {"input_rows": 0, "output_rows": 0, "failed_rows": 0}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_id = str(task.get("task_id") or "")
        task_run_state = str(task.get("state") or "unknown")
        result = task_results.get(task_id) or {}
        stage = str(result.get("stage") or "unknown")
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        if task_run_state == "succeeded":
            success_tasks += 1
        elif task_run_state == "failed":
            failed_tasks += 1
        for key in row_totals:
            row_totals[key] += _numeric_metric(metrics.get(key))
        task_labels = {**base_labels, "task_id": task_id, "stage": stage}
        lines.append(sample("zeta4s_task_result_state", 1, {**task_labels, "state": task_run_state}))
        task_duration = _numeric_metric(task.get("duration_seconds"))
        if task_duration:
            lines.append(sample("zeta4s_task_duration_seconds", task_duration, task_labels))
        lines.append(sample("zeta4s_task_input_rows", _numeric_metric(metrics.get("input_rows")), task_labels))
        lines.append(sample("zeta4s_task_output_rows", _numeric_metric(metrics.get("output_rows")), task_labels))
        lines.append(sample("zeta4s_task_failed_rows", _numeric_metric(metrics.get("failed_rows")), task_labels))

    lines.append(sample("zeta4s_latest_run_failed_tasks", failed_tasks, base_labels))
    lines.append(sample("zeta4s_latest_run_success_tasks", success_tasks, base_labels))
    lines.append(sample("zeta4s_latest_run_input_rows", row_totals["input_rows"], base_labels))
    lines.append(sample("zeta4s_latest_run_output_rows", row_totals["output_rows"], base_labels))
    lines.append(sample("zeta4s_latest_run_failed_rows", row_totals["failed_rows"], base_labels))
    return lines


def _prometheus_metrics_text() -> str:
    global _METRICS_TEXT_CACHE
    now = time.monotonic()
    if _METRICS_TEXT_CACHE and now - _METRICS_TEXT_CACHE[0] < _metrics_cache_ttl_seconds():
        return _METRICS_TEXT_CACHE[1]

    with _METRICS_TEXT_LOCK:
        now = time.monotonic()
        if _METRICS_TEXT_CACHE and now - _METRICS_TEXT_CACHE[0] < _metrics_cache_ttl_seconds():
            return _METRICS_TEXT_CACHE[1]

        lines = _metric_headers()
        jobs = _registered_runtime_jobs()
        projects: dict[str, dict[str, Any]] = {}
        for job in jobs:
            project = str(job["project"])
            projects.setdefault(project, job)
            lines.append(sample("zeta4s_job_registered", 1, {"project": project, "job": job["job"]}))
            run = None
            tasks: list[dict[str, Any]] = []
            try:
                rows = list_runs(limit=1, project_id=project, job_id=str(job["job"]))
                if rows:
                    run = _canonical_run(rows[0])
                    tasks = _canonical_tasks(rows[0])
            except Exception:
                lines.append(
                    sample(
                        "zeta4s_metrics_scrape_error",
                        1,
                        {"section": "run_observation", "project": project, "job": job["job"]},
                    )
                )
            lines.extend(_latest_run_metric_lines(job, run, tasks))

        for project, job in sorted(projects.items()):
            lines.append(sample("zeta4s_project_registered", 1, {"project": project}))
            registered_at = _timestamp_seconds(job.get("registered_at"))
            if registered_at is not None:
                lines.append(sample("zeta4s_artifact_registered_mtime_seconds", registered_at, {"project": project}))
        text = render_prometheus_text(lines)
        _METRICS_TEXT_CACHE = (now, text)
        return text


def _sync_airflow_task_notes(run: dict[str, Any], task_results: dict[str, dict[str, Any]]) -> None:
    try:
        _sync_airflow_run_notes_by_ids(
            dag_id=run["dag_id"],
            airflow_run_id=run["airflow_run_id"],
            result_run_id=run["run_id"],
        )
    except Exception:
        logger.warning("Failed to sync zeta4s Airflow notes", exc_info=True)


def _sync_airflow_run_notes_by_ids(*, dag_id: str, airflow_run_id: str, result_run_id: str) -> dict[str, Any]:
    from zeta4s.airflow.run_notes import sync_airflow_run_notes

    result = sync_airflow_run_notes(
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
        result_run_id=result_run_id,
        home=str(ZETA4S_API_HOME),
    )
    _sync_run_metadata_from_airflow_summary(
        result_run_id,
        result,
        dag_id=dag_id,
        airflow_run_id=airflow_run_id,
    )
    return result


def _sync_run_metadata_from_airflow_summary(
    run_id: str,
    sync_result: dict[str, Any],
    *,
    dag_id: str | None = None,
    airflow_run_id: str | None = None,
) -> None:
    dag_summary = sync_result.get("dag_summary") if isinstance(sync_result.get("dag_summary"), dict) else {}
    status = str(dag_summary.get("status") or "").strip().lower()
    if status not in {"success", "failed"}:
        return
    adapter = metastore_adapter_factory()
    repository = adapter.run_metadata_repository
    if repository.get_run(run_id) is None and dag_id:
        metadata_base = new_run_metadata_base()
        project_id, job_id = _project_job_from_dag_id(dag_id)
        repository.create_run(
            {
                **metadata_base,
                "run_id": run_id,
                "scheduler": "airflow",
                "scheduler_run_id": airflow_run_id or run_id,
                "project_id": project_id,
                "job_id": job_id,
                "adapter_metadata": {"native_job_id": dag_id},
                "source": "scheduler",
                "created_at": dag_summary.get("started_at") or metadata_base["created_at"],
                "parameters": {},
            }
        )
    patch: dict[str, Any] = {
        "status": "succeeded" if status == "success" else "failed",
        "state": "succeeded" if status == "success" else "failed",
        "started_at": dag_summary.get("started_at"),
        "ended_at": dag_summary.get("ended_at"),
        "finished_at": dag_summary.get("ended_at") or datetime.now(timezone.utc).isoformat(),
        "dag_summary": dag_summary,
    }
    repository.update_run(run_id, patch)


def _task_results_by_task_id(run_id: str) -> dict[str, dict[str, Any]]:
    return {
        str(result.get("task_id")): result
        for result in load_task_results(run_id, ZETA4S_API_HOME)
        if isinstance(result, dict) and result.get("task_id")
    }


def _run_summary(run: dict[str, Any], timezone: str | None = None) -> dict[str, Any]:
    run_state = _run_with_airflow_state(run, timezone)
    task_state = _airflow_task_instances(run, timezone)
    airflow_tasks = [task for task in task_state.get("tasks") or [] if isinstance(task, dict)]
    task_results = _task_results_by_task_id(run["run_id"])
    # 로그가 있는 task 다. `try_number` 가 0 이면 한 번도 시도하지 않아 로그가 없다
    # — upstream_failed 가 그렇다.
    log_task_ids = {str(task.get("task_id")) for task in airflow_tasks if int(task.get("try_number") or 0) >= 1}
    tasks: list[dict[str, Any]] = []
    counts = {"success": 0, "failed": 0, "skipped": 0}
    row_totals = {
        "input_rows": 0,
        "output_rows": 0,
        "success_rows": 0,
        "failed_rows": 0,
        "skipped_rows": 0,
        "error_rows": 0,
    }
    for task in airflow_tasks:
        task_id = str(task.get("task_id") or "")
        result = task_results.get(task_id) or {}
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        for key in row_totals:
            row_totals[key] += _numeric_metric(metrics.get(key))
        state = str(task.get("state") or "")
        if state == "success":
            counts["success"] += 1
        elif state in {"failed", "upstream_failed", "up_for_retry"}:
            counts["failed"] += 1
        elif state == "skipped":
            counts["skipped"] += 1
        tasks.append(
            {
                **task,
                "step_id": task_id,
                "execution": {
                    "state": task.get("state"),
                    "started_at": task.get("start_date"),
                    "ended_at": task.get("end_date"),
                    "duration_seconds": task.get("duration"),
                    "try_number": task.get("try_number"),
                },
                "stage": result.get("stage"),
                "result_status": result.get("status"),
                "metrics": metrics,
                "details": result.get("details") if isinstance(result.get("details"), dict) else {},
                "error": result.get("error") if isinstance(result.get("error"), dict) else {},
                "result_available": bool(result),
                "log_available": task_id in log_task_ids,
            }
        )
    execution_summary = _run_execution_summary(run_state, airflow_tasks)
    summary = {
        **execution_summary,
        "task_count": len(tasks),
        "success_task_count": counts["success"],
        "failed_task_count": counts["failed"],
        "skipped_task_count": counts["skipped"],
        **row_totals,
        "result_task_count": len(task_results),
        "task_state": task_state.get("task_state"),
    }
    payload = {"run": run_state, "summary": summary, "tasks": tasks}
    _sync_airflow_task_notes(run, task_results)
    if run.get("source") != "airflow":
        summary_path = ZETA4S_API_HOME / "runs" / run["run_id"] / "summary.json"
        try:
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        except OSError:
            pass
    return payload


def _terminal_state(state: str | None) -> bool:
    return state in {"success", "failed", "canceled", "cancelled", "not_found"}


def _stream_run_logs(
    run: dict[str, Any],
    *,
    tail: int | None,
    interval: float,
    timezone: str | None = None,
    task_id: str | None = None,
    failed_only: bool = False,
    latest_attempt_only: bool = False,
):
    display_timezone = _timezone_or_400(timezone)
    offsets: dict[str, int] = {}
    emitted_any = False
    while True:
        entries = _filtered_log_entries(
            run,
            task_id=task_id,
            failed_only=failed_only,
            latest_attempt_only=latest_attempt_only,
            tail=tail,
            timezone=display_timezone,
        )
        for entry in entries:
            path = Path(entry["path"])
            key = str(path)
            offset = offsets.get(key)
            if offset is None:
                content = str(entry.get("content") or "")
                offsets[key] = path.stat().st_size
            else:
                with path.open("rb") as handle:
                    handle.seek(offset)
                    data = handle.read()
                    offsets[key] = handle.tell()
                content = data.decode("utf-8", errors="replace")
            if content:
                emitted_any = True
                task_label = f" task={entry.get('task_id')}" if entry.get("task_id") else ""
                yield f"==> {path}{task_label} state={entry.get('state') or '-'} <==\n"
                yield localize_log_timestamps(content, display_timezone)
                if not content.endswith("\n"):
                    yield "\n"

        state = _run_with_airflow_state(run, display_timezone).get("airflow_state")
        if run.get("cancelled") or _terminal_state(state):
            if not emitted_any:
                yield "no logs found\n"
            return
        time.sleep(interval)


def _stream_canonical_run_logs(
    run: dict[str, Any],
    *,
    tail: int | None,
    interval: float,
    task_id: str | None = None,
    failed_only: bool = False,
    latest_attempt_only: bool = False,
):
    adapter = adapter_for_registration(_run_registration(str(run["project_id"])))
    offsets: dict[tuple[str, Any], int] = {}
    emitted_any = False
    while True:
        entries = adapter.read_logs(
            run,
            task_id=task_id,
            failed_only=failed_only,
            latest_attempt_only=latest_attempt_only,
            tail=tail,
        )
        for entry in entries:
            key = (str(entry.get("task_id") or "__run__"), entry.get("attempt"))
            lines = str(entry.get("content") or "").splitlines()
            offset = offsets.get(key, 0)
            new_lines = lines[offset:]
            offsets[key] = len(lines)
            if not new_lines:
                continue
            emitted_any = True
            yield f"==> task={key[0]} attempt={key[1] or '-'} state={entry.get('state') or '-'} <==\n"
            yield "\n".join(new_lines) + "\n"
        state = adapter.get_run(run).state
        if _terminal_state(state) or state in {"succeeded", "skipped"}:
            if not emitted_any:
                yield "no logs found\n"
            return
        time.sleep(interval)


def create_app() -> FastAPI:
    app = FastAPI(title="zeta4s-api", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> PlainTextResponse:
        return PlainTextResponse(_prometheus_metrics_text(), media_type="text/plain; version=0.0.4")

    @app.post("/api/v1/platform/bootstrap")
    def platform_bootstrap(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        adapter = metastore_adapter_factory()
        steps: list[dict[str, Any]] = []
        metastore_type = os.environ.get("ZETA4S_METASTORE_TYPE", "postgres")
        database = getattr(adapter, "database", None)
        steps.append(_step("metastore_adapter", "passed", {"type": metastore_type, "database": database}))
        try:
            adapter.bootstrap()
            schema = _inspect_metastore_schema(adapter)
            steps.append(
                _step("schema_bootstrap", "passed", {"database": database, "schema_status": schema.get("status")})
            )
        except Exception as e:
            steps.append(_step("schema_bootstrap", "failed", {"detail": str(e)}))
            return _emit_operation_report(
                _operation_report(
                    command="z4s api bootstrap",
                    project=None,
                    status="failed",
                    summary={"metastore_type": metastore_type, "database": database, "bootstrap": "failed"},
                    steps=steps,
                    issues=[{"code": "Z4S_METASTORE_BOOTSTRAP_001", "severity": "error", "message": str(e)}],
                )
            )
        try:
            iceberg = _inspect_iceberg_rowset_store()
            steps.append(_step("iceberg_rowset_store", "passed", iceberg))
        except Exception as e:
            steps.append(_step("iceberg_rowset_store", "failed", {"detail": str(e)}))
            return _emit_operation_report(
                _operation_report(
                    command="z4s api bootstrap",
                    project=None,
                    status="failed",
                    summary={"metastore_type": metastore_type, "database": database, "bootstrap": "failed"},
                    steps=steps,
                    issues=[{"code": "Z4S_ICEBERG_BOOTSTRAP_001", "severity": "error", "message": str(e)}],
                )
            )
        key_path = master_key_file_path()
        try:
            # keyring 은 운영자 소유 입력이다. API 는 만들지 않고 상태만 본다.
            # Kubernetes 는 Secret 을 read-only 로 mount 하므로 생성 시도 자체가
            # 그 배포 형태에서 실패한다.
            key_status = check_master_key_file(key_path)
            if not key_status.get("exists"):
                raise ValueError(
                    f"secret master keyring is missing: {key_path}. "
                    "Create it with the operator procedure before bootstrap."
                )
            if not key_status.get("valid"):
                raise ValueError(f"secret master keyring file is not valid: {key_path}")
            steps.append(
                _step(
                    "secret_master_key",
                    "passed",
                    {
                        "path": str(key_path),
                        "exists": key_status.get("exists"),
                        "readable": key_status.get("readable"),
                        "mode": key_status.get("mode"),
                        "secure_permissions": key_status.get("secure_permissions"),
                        "active_key_id": key_status.get("active_key_id"),
                        "generation_count": key_status.get("generation_count"),
                    },
                )
            )
        except Exception as e:
            steps.append(_step("secret_master_key", "failed", {"path": str(key_path), "detail": str(e)}))
            return _emit_operation_report(
                _operation_report(
                    command="z4s api bootstrap",
                    project=None,
                    status="failed",
                    summary={"metastore_type": metastore_type, "database": database, "bootstrap": "failed"},
                    steps=steps,
                    issues=[{"code": "Z4S_SECRET_MASTER_KEY_001", "severity": "error", "message": str(e)}],
                )
            )
        report = _emit_operation_report(
            _operation_report(
                command="z4s api bootstrap",
                project=None,
                status="passed",
                summary={"metastore_type": metastore_type, "database": database, "bootstrap": "passed"},
                steps=steps,
                schema=schema,
            )
        )
        return report

    @app.get("/api/v1/platform/status")
    def platform_status(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        metastore_type = os.environ.get("ZETA4S_METASTORE_TYPE", "postgres")
        adapter = metastore_adapter_factory()
        database = getattr(adapter, "database", None)
        try:
            schema = _inspect_metastore_schema(adapter)
        except Exception as e:
            return {
                "api_version": app.version,
                "metastore": {"type": metastore_type, "database": database},
                "bootstrap_status": "not_ready",
                "schema_status": "error",
                "scheduler_snapshot_status": "unknown",
                "issues": [{"code": "Z4S_PLATFORM_STATUS_001", "severity": "error", "message": str(e)}],
            }
        schema_status = str(schema.get("status") or "unknown")
        bootstrap_status = "ready" if schema_status == "ok" else "not_ready"
        return {
            "api_version": app.version,
            "metastore": {"type": metastore_type, "database": database},
            "bootstrap_status": bootstrap_status,
            "schema_status": schema_status,
            "scheduler_snapshot_status": "unknown",
            "schema": schema,
            "issues": [],
        }

    @app.post("/api/v1/runs/sync-notes")
    def run_sync_notes(
        request: RunNoteSyncRequest,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _require_token(authorization)
        return _sync_airflow_run_notes_by_ids(
            dag_id=request.adapter_job_id,
            airflow_run_id=request.scheduler_run_id,
            result_run_id=request.result_run_id,
        )

    @app.post("/api/v1/profiles/check")
    def profile_check(request: ProfileCheckRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        not_ready = _metastore_not_ready_report("z4s profile check", None)
        if not_ready is not None:
            not_ready.pop("project", None)
            not_ready.pop("project_id", None)
            return _emit_operation_report(not_ready)
        return _emit_operation_report(_profile_check_report(request.profile))

    @app.post("/api/v1/secrets")
    def secret_set(request: SecretSetRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        not_ready = _metastore_not_ready_report("z4s api secret set", None)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        try:
            return EncryptedSecretStore().set_secret(request.secret_key, request.value)
        except NotImplementedError as e:
            # ClickHouse metastore 는 쓰기 직렬화 구간을 제공하지 못한다.
            raise HTTPException(status_code=409, detail=str(e)) from e
        except (FileExistsError, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/v1/secrets")
    def secret_list(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        not_ready = _metastore_not_ready_report("z4s api secret list", None)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        try:
            return {"secrets": EncryptedSecretStore().list_metadata()}
        except NotImplementedError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.post("/api/v1/secrets/check")
    def secret_check(request: SecretCheckRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        not_ready = _metastore_not_ready_report("z4s api secret check", None)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        try:
            return EncryptedSecretStore().check_secret(request.secret_key)
        except NotImplementedError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/v1/secrets/keyring")
    def secret_keyring_status(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        """keyring 파일 상태를 보고한다. key 내용은 내보내지 않는다."""
        _require_token(authorization)
        status = check_master_key_file()
        return {
            "path": status["path"],
            "exists": status["exists"],
            "valid": status["valid"],
            "secure_permissions": status["secure_permissions"],
            "regular_file": status["regular_file"],
            "owner_trusted": status["owner_trusted"],
            "active_key_id": status["active_key_id"],
            "generation_count": status["generation_count"],
        }

    @app.post("/api/v1/secrets/keyring/rotate")
    def secret_keyring_rotate(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        """저장된 ciphertext 를 활성 세대로 다시 암호화한다.

        keyring 파일 자체는 운영자가 갱신한다. 이 연산은 파일을 쓰지 않는다.
        """
        _require_token(authorization)
        not_ready = _metastore_not_ready_report("z4s api secret rotate", None)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        try:
            return EncryptedSecretStore().rotate_to_active_generation()
        except NotImplementedError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except (InvalidTag, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/internal/v1/runtime/connections/{conn_id}")
    def runtime_internal_connection(conn_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_runtime_internal_token(authorization)
        try:
            return _resolve_runtime_connection(conn_id)
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.post("/internal/v1/runtime/steps/execute")
    def runtime_internal_step_execute(
        request: RuntimeStepExecuteRequest,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _require_runtime_internal_token(authorization)
        return _execute_runtime_step(request)

    @app.post("/internal/v1/runtime/runs/finalize")
    def runtime_internal_run_finalize(
        request: RuntimeRunFinalizeRequest,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        _require_runtime_internal_token(authorization)
        return _finalize_runtime_run(request)

    def _prepare_deploy_request(request: DeployRequest) -> tuple[str, Path]:
        bundle, artifact_id = decode_bundle(request.bundle_base64)
        extract_bundle(bundle, artifact_id)
        project_root = _project_root_for_artifact(artifact_id, request.project_id)
        return artifact_id, project_root

    def _lock_conflict(project: str, operation: str, e: OperationLockTimeout) -> HTTPException:
        report = _operation_report(
            command=operation,
            project=project,
            status="failed",
            summary={"lock_name": e.lock_name, "lock_timeout_seconds": e.timeout_seconds},
            steps=[_step("operation_lock", "failed", {"lock_name": e.lock_name}, ["runtime_operation_locked"])],
            issues=[
                {
                    "code": "runtime_operation_locked",
                    "severity": "error",
                    "message": "Another runtime operation is already running for this project.",
                    "details": {"lock_name": e.lock_name, "timeout_seconds": e.timeout_seconds},
                }
            ],
        )
        return HTTPException(status_code=409, detail=report)

    def _project_runtime_reset(project: str) -> dict[str, Any]:
        dag_state_timeout_seconds = _undeploy_dag_state_timeout_seconds()
        terminate_timeout_seconds = _undeploy_active_terminate_timeout_seconds()
        dag_poll_seconds = _airflow_operation_poll_seconds()
        terminate_poll_seconds = _undeploy_active_terminate_poll_seconds()
        airflow_result = _airflow_pause_and_terminate(
            project,
            dag_state_timeout_seconds=dag_state_timeout_seconds,
            terminate_timeout_seconds=terminate_timeout_seconds,
            dag_poll_seconds=dag_poll_seconds,
            terminate_poll_seconds=terminate_poll_seconds,
        )
        dag_ids = airflow_result.get("dag_ids", []) if isinstance(airflow_result, dict) else []
        dag_pause = airflow_result.get("dag_pause", {}) if isinstance(airflow_result, dict) else {}
        terminate_result = airflow_result.get("active_run_terminate", {}) if isinstance(airflow_result, dict) else {}
        if dag_pause.get("status") != "passed":
            return {
                "status": "failed",
                "dag_ids": dag_ids,
                "dag_pause": dag_pause,
                "active_run_terminate": terminate_result,
                "issues": [
                    {
                        "code": "Z4E_AIRFLOW_DAG_PAUSE_TIMEOUT",
                        "severity": "error",
                        "message": "Airflow DAG pause state did not converge before runtime reset.",
                        "step": "dag_pause",
                        "details": dag_pause,
                    }
                ],
            }
        remaining_active = terminate_result.get("remaining_runs", []) + terminate_result.get(
            "remaining_task_instances", []
        )
        if remaining_active:
            return {
                "status": "failed",
                "dag_ids": dag_ids,
                "dag_pause": dag_pause,
                "active_run_terminate": terminate_result,
                "issues": [
                    {
                        "code": "Z4E_AIRFLOW_ACTIVE_TERMINATE_TIMEOUT",
                        "severity": "error",
                        "message": "Active DAG runs remained after terminate timeout.",
                        "step": "active_run_terminate",
                        "details": {"remaining_active": remaining_active},
                    }
                ],
            }
        return {
            "status": "passed",
            "dag_ids": dag_ids,
            "dag_pause": dag_pause,
            "active_run_terminate": terminate_result,
            "issues": [],
        }

    def _deploy_apply_report(request: DeployRequest) -> dict[str, Any]:
        not_ready = _metastore_not_ready_report("z4s api deploy", request.project_id)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        _progress_running("bundle_build")
        artifact_id, project_root = _prepare_deploy_request(request)
        profile = request.profile
        _progress_step("bundle_build", summary={"artifact_id": artifact_id})

        _progress_running("project_check")
        project_check = _project_check_report(
            project_root,
            profile_id=request.profile_id,
            profile=profile,
        )
        _progress_step("project_check", project_check["status"], project_check["summary"])
        if project_check["status"] != "passed":
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=request.project_id,
                    status="failed",
                    summary={"artifact_id": artifact_id},
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step(
                                "project_check",
                                "failed",
                                project_check["summary"],
                                [issue["code"] for issue in project_check["issues"]],
                            ),
                        ]
                    ),
                    issues=project_check["issues"],
                    checks={"project_check": project_check},
                )
            )
        plan = _inspect_project_or_400(project_root)
        ensure_artifact_runtime_permissions(artifact_id)

        _progress_running("profile_check")
        profile_check = _profile_check_report(profile)
        _progress_step("profile_check", profile_check["status"], profile_check["summary"])
        if profile_check["status"] != "passed":
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={"artifact_id": artifact_id},
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step(
                                "profile_check",
                                "failed",
                                profile_check["summary"],
                                [issue["code"] for issue in profile_check["issues"]],
                            ),
                        ]
                    ),
                    issues=profile_check["issues"],
                    checks={"project_check": project_check, "profile_check": profile_check},
                    **_report_plan_fields(plan),
                )
            )

        scheduler_backend = scheduler_backend_from_profile(profile)
        if scheduler_backend == "airflow":
            _progress_running("dag_pause")
            reset_result = _project_runtime_reset(str(plan["project"]))
            _progress_step(
                "dag_pause",
                "failed" if reset_result.get("dag_pause", {}).get("status") != "passed" else "passed",
                reset_result.get("dag_pause"),
            )
            _progress_step(
                "active_run_terminate",
                reset_result.get("active_run_terminate", {}).get("status", "skipped"),
                reset_result.get("active_run_terminate"),
            )
        else:
            reset_result = {
                "status": "passed",
                "dag_pause": {"status": "skipped"},
                "active_run_terminate": {"status": "skipped"},
            }
            _progress_step("dag_pause", "skipped")
            _progress_step("active_run_terminate", "skipped")
        if reset_result.get("status") != "passed":
            issues = reset_result.get("issues") or []
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={"artifact_id": artifact_id},
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step(
                                "dag_pause",
                                "failed" if reset_result.get("dag_pause", {}).get("status") != "passed" else "passed",
                                reset_result.get("dag_pause"),
                                [issue["code"] for issue in issues if issue.get("step") == "dag_pause"],
                            ),
                            _step(
                                "active_run_terminate",
                                "failed"
                                if any(issue.get("step") == "active_run_terminate" for issue in issues)
                                else reset_result.get("active_run_terminate", {}).get("status", "skipped"),
                                reset_result.get("active_run_terminate"),
                                [issue["code"] for issue in issues if issue.get("step") == "active_run_terminate"],
                            ),
                        ]
                    ),
                    issues=issues,
                    checks={"project_check": project_check, "profile_check": profile_check},
                    **_report_plan_fields(plan),
                )
            )
        try:
            if scheduler_backend == "airflow":
                _progress_running("airflow_register_prepare")
                airflow_prepare_summary = _airflow_register_prepare(profile, project_root)
                _progress_step("airflow_register_prepare", summary=airflow_prepare_summary)
                airflow_prepare_steps = _airflow_prepare_steps(airflow_prepare_summary)
                for step in airflow_prepare_steps:
                    name = str(step.get("name") or "")
                    _progress_step(
                        name,
                        str(step.get("status") or "passed"),
                        step.get("summary") if isinstance(step.get("summary"), dict) else {},
                    )
            else:
                airflow_prepare_summary = {}
                airflow_prepare_steps = [
                    _step("connections_apply", "skipped"),
                    _step("project_pools_apply", "skipped"),
                ]
                _progress_step("airflow_register_prepare", "skipped")
                _progress_step("connections_apply", "skipped")
                _progress_step("project_pools_apply", "skipped")
        except (HTTPException, RuntimeError, ValueError) as e:
            detail = e.detail if isinstance(e, HTTPException) else str(e)
            _progress_step("airflow_register_prepare", "failed", {"detail": detail})
            issue = {
                "code": "Z4E_AIRFLOW_REGISTER_PREPARE_001",
                "severity": "error",
                "message": "Airflow registration preparation failed.",
                "step": "airflow_register_prepare",
                "details": {"detail": detail},
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={"artifact_id": artifact_id},
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step("dag_pause", summary=reset_result.get("dag_pause")),
                            _step("active_run_terminate", summary=reset_result.get("active_run_terminate")),
                            _step("airflow_register_prepare", "failed", issue["details"], [issue["code"]]),
                        ]
                    ),
                    issues=[issue],
                    checks={"project_check": project_check, "profile_check": profile_check},
                    **_report_plan_fields(plan),
                )
            )

        try:
            _progress_running("dbt_validate")
            dbt_validate = _dbt_validate_report(project_root, profile)
            dbt_status = "skipped" if dbt_validate.get("status") == "skipped" else "passed"
            _progress_step("dbt_validate", dbt_status, dbt_validate)
        except HTTPException as e:
            detail = e.detail
            _progress_step("dbt_validate", "failed", {"detail": detail})
            issue = {
                "code": "Z4E_DBT_VALIDATE_001",
                "severity": "error",
                "message": "dbt validation failed.",
                "step": "dbt_validate",
                "details": {"detail": detail},
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={"artifact_id": artifact_id},
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step("dag_pause", summary=reset_result.get("dag_pause")),
                            _step("active_run_terminate", summary=reset_result.get("active_run_terminate")),
                            _step(
                                "airflow_register_prepare",
                                "skipped" if scheduler_backend == "prefect" else "passed",
                                airflow_prepare_summary,
                            ),
                            *airflow_prepare_steps,
                            _step("dbt_validate", "failed", issue["details"], [issue["code"]]),
                        ]
                    ),
                    issues=[issue],
                    checks={"project_check": project_check, "profile_check": profile_check},
                    **_report_plan_fields(plan),
                )
            )

        _progress_running("artifact_register")
        runtime_connections = runtime_connection_policy_from_profile(profile)
        record_artifact_metadata(
            artifact_id=artifact_id,
            project_id=plan["project_id"],
            runtime_connections=runtime_connections,
            dags=plan["dags"],
        )
        backend_registry = _sync_backend_registry(plan["project_id"], runtime_connections)
        if scheduler_backend == "prefect":
            artifact_summary = {"artifact_id": artifact_id, **backend_registry}
            _progress_step("artifact_register", summary=artifact_summary)
            deployments = _deploy_prefect_jobs(
                plan=plan,
                artifact_id=artifact_id,
                project_root=project_root,
                profile_id=request.profile_id,
            )
            registered_path = upsert_project_registration(
                project_id=plan["project_id"],
                artifact_id=artifact_id,
                profile_id=request.profile_id,
                scheduler_backend="prefect",
                dags=plan["dags"],
            )
            scheduler_summary = {
                "scheduler_backend": "prefect",
                "deployment_count": len(deployments),
            }
            _progress_step("scheduler_deploy", summary=scheduler_summary)
            _progress_step("dag_discovery", "skipped")
            _progress_step("dag_unpause", "skipped")
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    summary={
                        "artifact_id": artifact_id,
                        "scheduler_backend": "prefect",
                        "deployment_count": len(deployments),
                        "registered": str(registered_path),
                    },
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step("dag_pause", "skipped"),
                            _step("active_run_terminate", "skipped"),
                            _step("airflow_register_prepare", "skipped"),
                            *airflow_prepare_steps,
                            _step(
                                "dbt_validate",
                                "skipped" if dbt_validate.get("status") == "skipped" else "passed",
                                dbt_validate,
                            ),
                            _step("artifact_register", summary=artifact_summary),
                            _step("scheduler_deploy", summary=scheduler_summary),
                            _step("dag_discovery", "skipped"),
                            _step("dag_unpause", "skipped"),
                        ]
                    ),
                    artifact_id=artifact_id,
                    registered=str(registered_path),
                    checks={
                        "project_check": project_check,
                        "profile_check": profile_check,
                        "dbt_validate": dbt_validate,
                    },
                    **_report_plan_fields(plan),
                )
            )

        registered_path = upsert_project_registration(
            project_id=plan["project_id"],
            artifact_id=artifact_id,
            profile_id=request.profile_id,
            scheduler_backend=scheduler_backend,
            dags=plan["dags"],
        )
        _progress_step(
            "artifact_register",
            summary={"artifact_id": artifact_id, "registered": str(registered_path), **backend_registry},
        )
        _progress_step("scheduler_deploy", summary={"dag_count": len(plan["dags"])})
        expected_dag_ids = [str(dag["dag_id"]) for dag in plan["dags"]]
        dag_discovery_timeout_seconds = _deploy_dag_discovery_timeout_seconds()
        dag_state_timeout_seconds = _deploy_dag_state_timeout_seconds()
        dag_poll_seconds = _airflow_operation_poll_seconds()
        _progress_running("dag_discovery", {"expected": len(expected_dag_ids)})
        dag_result = _airflow_discover_and_unpause(
            str(plan["project"]),
            expected_dag_ids,
            expected_artifact_id=artifact_id,
            dag_discovery_timeout_seconds=dag_discovery_timeout_seconds,
            dag_state_timeout_seconds=dag_state_timeout_seconds,
            dag_poll_seconds=dag_poll_seconds,
        )
        dag_discovery = dag_result.get("dag_discovery") if isinstance(dag_result, dict) else {}
        dag_unpause = dag_result.get("dag_unpause") if isinstance(dag_result, dict) else {}
        if dag_discovery.get("status") != "passed":
            _progress_step("dag_discovery", "failed", dag_discovery)
            _progress_step("dag_unpause", "skipped")
            issue = {
                "code": "Z4E_AIRFLOW_DAG_DISCOVERY_TIMEOUT",
                "severity": "error",
                "message": "Airflow did not register all project DAGs before deploy timeout.",
                "step": "dag_discovery",
                "details": dag_discovery,
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={
                        "artifact_id": artifact_id,
                        "dag_count": len(plan["dags"]),
                        "registered": str(registered_path),
                        **dag_discovery,
                    },
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step("dag_pause", summary=reset_result.get("dag_pause")),
                            _step("active_run_terminate", summary=reset_result.get("active_run_terminate")),
                            _step("airflow_register_prepare", summary=airflow_prepare_summary),
                            *airflow_prepare_steps,
                            _step(
                                "dbt_validate",
                                "skipped" if dbt_validate.get("status") == "skipped" else "passed",
                                dbt_validate,
                            ),
                            _step("artifact_register", summary={"artifact_id": artifact_id, **backend_registry}),
                            _step("scheduler_deploy", summary={"dag_count": len(plan["dags"])}),
                            _step("dag_discovery", "failed", dag_discovery, ["Z4E_AIRFLOW_DAG_DISCOVERY_TIMEOUT"]),
                            _step("dag_unpause", "skipped"),
                        ]
                    ),
                    issues=[issue],
                    artifact_id=artifact_id,
                    registered=str(registered_path),
                    checks={
                        "project_check": project_check,
                        "profile_check": profile_check,
                        "dbt_validate": dbt_validate,
                    },
                    **_report_plan_fields(plan),
                )
            )
        if dag_unpause.get("status") != "passed":
            _progress_step("dag_discovery", summary=dag_discovery)
            _progress_step("dag_unpause", "failed", dag_unpause)
            issue = {
                "code": "Z4E_AIRFLOW_DAG_UNPAUSE_TIMEOUT",
                "severity": "error",
                "message": "Airflow DAG unpause state did not converge before deploy timeout.",
                "step": "dag_unpause",
                "details": dag_unpause,
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api deploy",
                    project=plan["project"],
                    status="failed",
                    summary={
                        "artifact_id": artifact_id,
                        "dag_count": len(plan["dags"]),
                        "registered": str(registered_path),
                        **dag_unpause,
                    },
                    steps=_deploy_steps_with_remaining(
                        [
                            _step("bundle_build", summary={"artifact_id": artifact_id}),
                            _step("project_check", summary=project_check["summary"]),
                            _step("profile_check", summary=profile_check["summary"]),
                            _step("dag_pause", summary=reset_result.get("dag_pause")),
                            _step("active_run_terminate", summary=reset_result.get("active_run_terminate")),
                            _step("airflow_register_prepare", summary=airflow_prepare_summary),
                            *airflow_prepare_steps,
                            _step(
                                "dbt_validate",
                                "skipped" if dbt_validate.get("status") == "skipped" else "passed",
                                dbt_validate,
                            ),
                            _step("artifact_register", summary={"artifact_id": artifact_id, **backend_registry}),
                            _step("scheduler_deploy", summary={"dag_count": len(plan["dags"])}),
                            _step("dag_discovery", summary=dag_discovery),
                            _step("dag_unpause", "failed", dag_unpause, ["Z4E_AIRFLOW_DAG_UNPAUSE_TIMEOUT"]),
                        ]
                    ),
                    issues=[issue],
                    artifact_id=artifact_id,
                    registered=str(registered_path),
                    checks={
                        "project_check": project_check,
                        "profile_check": profile_check,
                        "dbt_validate": dbt_validate,
                    },
                    **_report_plan_fields(plan),
                )
            )
        _progress_step("dag_discovery", summary=dag_discovery)
        _progress_step("dag_unpause", summary=dag_unpause)
        report = _emit_operation_report(
            _operation_report(
                command="z4s api deploy",
                project=plan["project"],
                summary={
                    "artifact_id": artifact_id,
                    "dag_count": len(plan["dags"]),
                    "registered": str(registered_path),
                    **dag_discovery,
                },
                steps=[
                    _step("bundle_build", summary={"artifact_id": artifact_id}),
                    _step("project_check", summary=project_check["summary"]),
                    _step("profile_check", summary=profile_check["summary"]),
                    _step("dag_pause", summary=reset_result.get("dag_pause")),
                    _step("active_run_terminate", summary=reset_result.get("active_run_terminate")),
                    _step("airflow_register_prepare", summary=airflow_prepare_summary),
                    *airflow_prepare_steps,
                    _step(
                        "dbt_validate", "skipped" if dbt_validate.get("status") == "skipped" else "passed", dbt_validate
                    ),
                    _step("artifact_register", summary={"artifact_id": artifact_id, **backend_registry}),
                    _step("scheduler_deploy", summary={"dag_count": len(plan["dags"])}),
                    _step("dag_discovery", summary=dag_discovery),
                    _step("dag_unpause", summary=dag_unpause),
                ],
                artifact_id=artifact_id,
                registered=str(registered_path),
                checks={"project_check": project_check, "profile_check": profile_check, "dbt_validate": dbt_validate},
                dag_discovery=dag_discovery,
                dag_unpause=dag_unpause,
                **_report_plan_fields(plan),
            )
        )
        return report

    @app.post("/api/v1/deploy")
    def deploy_apply(request: DeployRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        try:
            with project_operation_lock(request.project_id):
                return _deploy_apply_report(request)
        except OperationLockTimeout as e:
            raise _lock_conflict(request.project_id, "z4s api deploy", e) from e
        except HTTPException as e:
            if e.status_code == 400:
                raise _runtime_http_error(
                    command="z4s api deploy",
                    project=request.project_id,
                    code="Z4E_RUNTIME_DEPLOY_REQUEST_001",
                    step="request_prepare",
                    error=e,
                ) from e
            raise

    @app.post("/api/v1/deploy/stream")
    def deploy_apply_stream(
        request: DeployRequest, authorization: str | None = Header(default=None)
    ) -> StreamingResponse:
        _require_token(authorization)

        def run() -> dict[str, Any]:
            try:
                with project_operation_lock(request.project_id):
                    return _deploy_apply_report(request)
            except OperationLockTimeout as e:
                raise _lock_conflict(request.project_id, "z4s api deploy", e) from e
            except HTTPException as e:
                if e.status_code == 400:
                    raise _runtime_http_error(
                        command="z4s api deploy",
                        project=request.project_id,
                        code="Z4E_RUNTIME_DEPLOY_REQUEST_001",
                        step="request_prepare",
                        error=e,
                    ) from e
                raise

        return _stream_operation(
            command="z4s api deploy",
            project=request.project_id,
            operation="deploy",
            steps=DEPLOY_PROGRESS_STEPS,
            fn=run,
        )

    def _deploy_unregister_report(payload: dict[str, Any]) -> dict[str, Any]:
        from zeta4s.project.loader import validate_project_id

        project = validate_project_id(str(payload.get("project_id") or ""))
        not_ready = _metastore_not_ready_report("z4s api undeploy", project)
        if not_ready is not None:
            return _emit_operation_report(not_ready)
        active_registration = _active_project_registration(project)
        if active_registration is None:
            issue = {
                "code": "Z4S_PROJECT_NOT_REGISTERED",
                "severity": "error",
                "message": f"Project is not registered: {project}",
                "step": "project_registration",
                "details": {"project": project},
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    status="failed",
                    summary={"registered": False},
                    steps=_undeploy_steps_with_remaining(
                        [
                            _step("project_registration", "failed", {"registered": False}, [issue["code"]]),
                        ]
                    ),
                    issues=[issue],
                )
            )
        registration_step = _step(
            "project_registration", summary={"artifact_id": active_registration.get("artifact_id")}
        )
        scheduler_backend = str(active_registration.get("scheduler_backend") or "")
        if scheduler_backend not in {"airflow", "prefect"}:
            issue = {
                "code": "Z4E_SCHEDULER_BACKEND_001",
                "severity": "error",
                "message": f"Unsupported stored scheduler backend: {scheduler_backend or '<missing>'}",
                "step": "project_registration",
                "details": {"project": project, "scheduler_backend": scheduler_backend},
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    status="failed",
                    summary={"scheduler_backend": scheduler_backend},
                    steps=_undeploy_steps_with_remaining(
                        [
                            _step(
                                "project_registration",
                                "failed",
                                {"scheduler_backend": scheduler_backend},
                                [issue["code"]],
                            )
                        ]
                    ),
                    issues=[issue],
                )
            )
        if scheduler_backend == "prefect":
            from zeta4s.prefect import ScheduleIdentity
            from zeta4s.prefect.prefect_engine import delete_prefect_job

            profile_id = str(active_registration["profile_id"])
            deleted = 0
            job_count = 0
            for dag in active_registration.get("dags") or []:
                if not isinstance(dag, dict):
                    continue
                job_id = str(dag.get("job_id") or "")
                if not job_id:
                    continue
                job_count += 1
                if delete_prefect_job(ScheduleIdentity(project, job_id, profile_id)):
                    deleted += 1
            registered_path, removed = remove_project_registration(project)
            cleanup_summary = {
                "scheduler_backend": "prefect",
                "job_count": job_count,
                "deleted_count": deleted,
            }
            artifact_id = str(removed.get("artifact_id")) if removed and removed.get("artifact_id") else None
            report = _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    summary={"artifact_id": artifact_id, **cleanup_summary},
                    steps=_undeploy_steps_with_remaining(
                        [
                            registration_step,
                            _step("dag_pause", "skipped"),
                            _step("active_run_terminate", "skipped"),
                            _step("scheduler_cleanup", summary=cleanup_summary),
                            _step("registration_remove", summary={"removed": bool(removed)}),
                        ]
                    ),
                    registered=str(registered_path),
                    removed_registration=removed,
                )
            )
            return report

        dag_state_timeout_seconds = _undeploy_dag_state_timeout_seconds()
        terminate_timeout_seconds = _undeploy_active_terminate_timeout_seconds()
        dag_delete_timeout_seconds = _undeploy_dag_delete_timeout_seconds()
        dag_poll_seconds = _airflow_operation_poll_seconds()
        terminate_poll_seconds = _undeploy_active_terminate_poll_seconds()
        airflow_result = _airflow_pause_and_terminate(
            project,
            dag_state_timeout_seconds=dag_state_timeout_seconds,
            terminate_timeout_seconds=terminate_timeout_seconds,
            dag_poll_seconds=dag_poll_seconds,
            terminate_poll_seconds=terminate_poll_seconds,
        )
        dag_ids = airflow_result.get("dag_ids", []) if isinstance(airflow_result, dict) else []
        dag_pause = airflow_result.get("dag_pause", {}) if isinstance(airflow_result, dict) else {}
        terminate_result = airflow_result.get("active_run_terminate", {}) if isinstance(airflow_result, dict) else {}
        if dag_pause.get("status") != "passed":
            issue = {
                "code": "Z4E_AIRFLOW_DAG_PAUSE_TIMEOUT",
                "severity": "error",
                "message": "Airflow DAG pause state did not converge before undeploy timeout.",
                "step": "dag_pause",
                "details": dag_pause,
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    status="failed",
                    summary={
                        "dag_count": len(dag_ids),
                        "not_converged_dag_count": dag_pause.get("not_converged_count"),
                    },
                    steps=_undeploy_steps_with_remaining(
                        [
                            registration_step,
                            _step("dag_pause", "failed", dag_pause, ["Z4E_AIRFLOW_DAG_PAUSE_TIMEOUT"]),
                        ]
                    ),
                    issues=[issue],
                    dag_ids=dag_ids,
                    dag_pause=dag_pause,
                )
            )
        remaining_active = terminate_result["remaining_runs"] + terminate_result["remaining_task_instances"]
        if remaining_active:
            return _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    status="failed",
                    summary={
                        "dag_count": len(dag_ids),
                        "remaining_active_run_count": terminate_result["remaining_run_count"],
                        "remaining_active_task_instance_count": terminate_result["remaining_task_instance_count"],
                    },
                    steps=_undeploy_steps_with_remaining(
                        [
                            registration_step,
                            _step("dag_pause", summary=dag_pause),
                            _step(
                                "active_run_terminate",
                                "failed",
                                terminate_result,
                                ["Z4E_AIRFLOW_ACTIVE_TERMINATE_TIMEOUT"],
                            ),
                        ]
                    ),
                    issues=[
                        {
                            "code": "Z4E_AIRFLOW_ACTIVE_TERMINATE_TIMEOUT",
                            "severity": "error",
                            "message": "Active DAG runs remained after terminate timeout.",
                            "step": "active_run_terminate",
                            "details": {"remaining_active": remaining_active},
                        }
                    ],
                    dag_ids=dag_ids,
                    dag_pause=dag_pause,
                    active_run_terminate=terminate_result,
                )
            )
        from zeta4s.airflow.dags import converge_project_dags_deleted

        delete_result = converge_project_dags_deleted(
            project,
            dag_ids,
            timeout_seconds=dag_delete_timeout_seconds,
            poll_interval_seconds=dag_poll_seconds,
        )
        if delete_result.get("status") != "passed":
            issue = {
                "code": "Z4E_AIRFLOW_DAG_DELETE_TIMEOUT",
                "severity": "error",
                "message": "Airflow DAG metadata delete did not converge before undeploy timeout.",
                "step": "scheduler_cleanup",
                "details": delete_result,
            }
            return _emit_operation_report(
                _operation_report(
                    command="z4s api undeploy",
                    project=project,
                    status="failed",
                    summary={
                        "dag_count": len(dag_ids),
                        "remaining_dag_count": delete_result.get("remaining_count"),
                    },
                    steps=_undeploy_steps_with_remaining(
                        [
                            registration_step,
                            _step("dag_pause", summary=dag_pause),
                            _step("active_run_terminate", summary=terminate_result),
                            _step("scheduler_cleanup", "failed", delete_result, ["Z4E_AIRFLOW_DAG_DELETE_TIMEOUT"]),
                        ]
                    ),
                    issues=[issue],
                    dag_ids=dag_ids,
                    dag_pause=dag_pause,
                    active_run_terminate=terminate_result,
                    dag_delete=delete_result,
                )
            )
        registered_path, removed = remove_project_registration(project)
        deleted_dags = delete_result.get("deleted_dags", []) if isinstance(delete_result, dict) else []
        artifact_id = str(removed.get("artifact_id")) if removed and removed.get("artifact_id") else None
        report = _emit_operation_report(
            _operation_report(
                command="z4s api undeploy",
                project=project,
                summary={
                    "deleted_dag_count": len(deleted_dags),
                    "artifact_id": artifact_id,
                    "terminated_active_run_count": terminate_result["terminated_run_count"],
                    "terminated_active_task_instance_count": terminate_result["terminated_task_instance_count"],
                },
                steps=[
                    registration_step,
                    _step("dag_pause", summary=dag_pause),
                    _step("active_run_terminate", summary=terminate_result),
                    _step("scheduler_cleanup", summary=delete_result),
                    _step("registration_remove", summary={"removed": bool(removed)}),
                ],
                registered=str(registered_path),
                removed_registration=removed,
                dag_ids=dag_ids,
                dag_pause=dag_pause,
                active_run_terminate=terminate_result,
                dag_delete=delete_result,
                deleted_dags=deleted_dags,
            )
        )
        return report

    @app.post("/api/v1/undeploy")
    def deploy_unregister(payload: dict[str, Any], authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        from zeta4s.project.loader import validate_project_id

        project = validate_project_id(str(payload.get("project_id") or ""))
        try:
            with project_operation_lock(project):
                return _deploy_unregister_report({"project_id": project})
        except OperationLockTimeout as e:
            raise _lock_conflict(project, "z4s api undeploy", e) from e

    @app.post("/api/v1/redeploy")
    def redeploy(request: DeployRequest, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        try:
            with project_operation_lock(request.project_id):
                undeploy_report = _deploy_unregister_report({"project_id": request.project_id})
                if undeploy_report.get("status") == "failed":
                    return _emit_operation_report(
                        _operation_report(
                            command="z4s api redeploy",
                            project=request.project_id,
                            status="failed",
                            steps=[
                                _step("undeploy", "failed", _report_summary(undeploy_report)),
                                _step("deploy", "skipped"),
                            ],
                            issues=undeploy_report.get("issues") or [],
                            nested_operations=[
                                {
                                    "operation": "undeploy",
                                    "status": "failed",
                                    "report": undeploy_report,
                                    "report_name": "api-undeploy",
                                }
                            ],
                        )
                    )
                deploy_report = _deploy_apply_report(request)
        except OperationLockTimeout as e:
            raise _lock_conflict(request.project_id, "z4s api redeploy", e) from e
        except HTTPException as e:
            if e.status_code == 400:
                raise _runtime_http_error(
                    command="z4s api redeploy",
                    project=request.project_id,
                    code="Z4E_RUNTIME_REDEPLOY_REQUEST_001",
                    step="request_prepare",
                    error=e,
                ) from e
            raise
        status = "failed" if deploy_report.get("status") == "failed" else "passed"
        return _emit_operation_report(
            _operation_report(
                command="z4s api redeploy",
                project=request.project_id,
                status=status,
                steps=[
                    _step("undeploy", undeploy_report.get("status", "passed"), _report_summary(undeploy_report)),
                    _step("deploy", deploy_report.get("status", "passed"), _report_summary(deploy_report)),
                ],
                issues=deploy_report.get("issues") or [],
                nested_operations=[
                    {
                        "operation": "undeploy",
                        "status": undeploy_report.get("status"),
                        "report": undeploy_report,
                        "report_name": "api-undeploy",
                    },
                    {
                        "operation": "deploy",
                        "status": deploy_report.get("status"),
                        "report": deploy_report,
                        "report_name": "api-deploy",
                    },
                ],
            )
        )

    @app.post("/api/v1/redeploy/stream")
    def redeploy_stream(request: DeployRequest, authorization: str | None = Header(default=None)) -> StreamingResponse:
        _require_token(authorization)

        def run() -> dict[str, Any]:
            try:
                with project_operation_lock(request.project_id):
                    _progress_running("undeploy")
                    undeploy_report = _deploy_unregister_report({"project_id": request.project_id})
                    _progress_step(
                        "undeploy",
                        str(undeploy_report.get("status") or "passed"),
                        undeploy_report.get("summary") if isinstance(undeploy_report.get("summary"), dict) else {},
                    )
                    if undeploy_report.get("status") == "failed":
                        _progress_step("deploy", "skipped")
                        return _emit_operation_report(
                            _operation_report(
                                command="z4s api redeploy",
                                project=request.project_id,
                                status="failed",
                                steps=[
                                    _step("undeploy", "failed", _report_summary(undeploy_report)),
                                    _step("deploy", "skipped"),
                                ],
                                issues=undeploy_report.get("issues") or [],
                                nested_operations=[
                                    {
                                        "operation": "undeploy",
                                        "status": "failed",
                                        "report": undeploy_report,
                                        "report_name": "api-undeploy",
                                    }
                                ],
                            )
                        )
                    progress_emit = getattr(_PROGRESS_LOCAL, "emit", None)
                    try:
                        _PROGRESS_LOCAL.emit = None
                        deploy_report = _deploy_apply_report(request)
                    finally:
                        _PROGRESS_LOCAL.emit = progress_emit
            except OperationLockTimeout as e:
                raise _lock_conflict(request.project_id, "z4s api redeploy", e) from e
            except HTTPException as e:
                if e.status_code == 400:
                    raise _runtime_http_error(
                        command="z4s api redeploy",
                        project=request.project_id,
                        code="Z4E_RUNTIME_REDEPLOY_REQUEST_001",
                        step="request_prepare",
                        error=e,
                    ) from e
                raise
            _progress_step(
                "deploy",
                str(deploy_report.get("status") or "passed"),
                deploy_report.get("summary") if isinstance(deploy_report.get("summary"), dict) else {},
            )
            status = "failed" if deploy_report.get("status") == "failed" else "passed"
            return _emit_operation_report(
                _operation_report(
                    command="z4s api redeploy",
                    project=request.project_id,
                    status=status,
                    steps=[
                        _step("undeploy", undeploy_report.get("status", "passed"), _report_summary(undeploy_report)),
                        _step("deploy", deploy_report.get("status", "passed"), _report_summary(deploy_report)),
                    ],
                    issues=deploy_report.get("issues") or [],
                    nested_operations=[
                        {
                            "operation": "undeploy",
                            "status": undeploy_report.get("status"),
                            "report": undeploy_report,
                            "report_name": "api-undeploy",
                        },
                        {
                            "operation": "deploy",
                            "status": deploy_report.get("status"),
                            "report": deploy_report,
                            "report_name": "api-deploy",
                        },
                    ],
                )
            )

        return _stream_operation(
            command="z4s api redeploy",
            project=request.project_id,
            operation="redeploy",
            steps=REDEPLOY_PROGRESS_STEPS,
            fn=run,
        )

    @app.get("/api/v1/projects/{project}/graph")
    def project_graph(project: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        from zeta4s.project.loader import validate_project_id

        try:
            project = validate_project_id(project)
            artifact_id = _artifact_id_for_project(project)
            if not artifact_id:
                raise HTTPException(status_code=404, detail=f"project is not registered: {project}")
            _load_artifact_metadata(artifact_id)
            project_root = _project_root_for_artifact(artifact_id, project)
            return _inspect_project_or_400(project_root)
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/v1/projects/{project}/artifact")
    def project_artifact(project: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(authorization)
        from zeta4s.project.loader import validate_project_id

        try:
            project = validate_project_id(project)
            for registration in load_registrations().get("registrations", []):
                if isinstance(registration, dict) and registration.get("project_id") == project:
                    return {
                        "project": project,
                        "project_id": project,
                        "artifact_id": registration.get("artifact_id"),
                        "registered_at": registration.get("registered_at"),
                        "jobs": [
                            {"job_id": item.get("job_id"), "config": item.get("config")}
                            for item in registration.get("dags") or []
                            if isinstance(item, dict)
                        ],
                    }
            raise HTTPException(status_code=404, detail=f"project is not registered: {project}")
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/api/v1/jobs")
    def jobs_list(
        authorization: str | None = Header(default=None),
        project: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        registrations = load_registrations().get("registrations") or []
        jobs = []
        for registration in registrations:
            if not isinstance(registration, dict) or (project and registration.get("project_id") != project):
                continue
            for item in registration.get("dags") or []:
                if isinstance(item, dict) and item.get("job_id"):
                    jobs.append({"project_id": registration.get("project_id"), "job_id": item["job_id"]})
        return {"jobs": jobs}

    @app.get("/api/v1/projects/{project_id}/runs")
    def project_runs(
        project_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        limit: int = 30,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        return {"runs": _canonical_project_runs(project_id, job_id, limit, timezone)}

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}")
    def project_run_detail(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        return _canonical_run(run, timezone)

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}/summary")
    def project_run_summary(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        return _canonical_summary(run, timezone)

    @app.post("/api/v1/projects/{project_id}/jobs/{job_id}/runs")
    def project_job_run(
        project_id: str,
        job_id: str,
        request: RunCreateRequest,
        authorization: str | None = Header(default=None),
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        display_timezone = _timezone_or_400(timezone)
        registration = _run_registration(project_id)
        if not _registered_job(registration, job_id):
            raise HTTPException(status_code=404, detail=f"active job not found: {project_id}/{job_id}")
        run_id = new_run_id(project_id, job_id, display_timezone)
        adapter = adapter_for_registration(registration)
        snapshot = adapter.create_run(
            project_id=project_id,
            job_id=job_id,
            run_id=run_id,
            parameters=request.parameters,
        ).as_dict()
        run = {
            **new_run_metadata_base(display_timezone),
            **snapshot,
            "artifact_id": _artifact_id_for_project(project_id),
            "source": "zeta4s",
        }
        try:
            create_run(run)
        except Exception:
            adapter.cancel_run(run)
            raise
        return _canonical_run(run, display_timezone)

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}/tasks")
    def project_run_tasks(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        tasks = _canonical_tasks(run)
        return {
            "run": _canonical_run(run, timezone),
            "task_state": "available",
            "tasks": tasks,
            "all_tasks_terminal": bool(tasks)
            and all(task.get("state") in {"succeeded", "failed", "skipped", "cancelled"} for task in tasks),
        }

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}/logs")
    def project_run_logs(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        tail: int | None = None,
        task_id: str | None = None,
        failed_only: bool = False,
        latest_attempt_only: bool = False,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        adapter = adapter_for_registration(_run_registration(project_id))
        try:
            logs = adapter.read_logs(
                run,
                task_id=task_id,
                failed_only=failed_only,
                latest_attempt_only=latest_attempt_only,
                tail=tail,
            )
        except RunCapabilityUnsupported as error:
            raise _run_capability_error(error) from error
        return {"run": _canonical_run(run, timezone), "logs": logs}

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}/logs/stream")
    def project_run_logs_stream(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        tail: int | None = 200,
        interval: float = 1.0,
        task_id: str | None = None,
        failed_only: bool = False,
        latest_attempt_only: bool = False,
        timezone: str | None = None,
    ):
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        if interval <= 0:
            raise HTTPException(status_code=400, detail="interval must be greater than 0")
        return StreamingResponse(
            _stream_canonical_run_logs(
                run,
                tail=tail,
                interval=interval,
                task_id=task_id,
                failed_only=failed_only,
                latest_attempt_only=latest_attempt_only,
            ),
            media_type="text/plain; charset=utf-8",
        )

    @app.get("/api/v1/projects/{project_id}/runs/{run_id}/artifacts")
    def project_run_artifacts(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        artifact_id = run.get("artifact_id")
        artifact = _load_artifact_metadata(artifact_id) if artifact_id else None
        run_artifacts_root = ZETA4S_API_HOME / "runs" / run_id / "artifacts"
        files = (
            [
                str(path.relative_to(run_artifacts_root))
                for path in sorted(run_artifacts_root.rglob("*"))
                if path.is_file()
            ]
            if run_artifacts_root.exists()
            else []
        )
        return {"run": _canonical_run(run, timezone), "artifact": artifact, "files": files}

    @app.post("/api/v1/projects/{project_id}/runs/{run_id}/cancel")
    def project_run_cancel(
        project_id: str,
        run_id: str,
        authorization: str | None = Header(default=None),
        job_id: str | None = None,
        timezone: str | None = None,
    ) -> dict[str, Any]:
        _require_token(authorization)
        run = _canonical_run_or_404(project_id, run_id, job_id)
        adapter = adapter_for_registration(_run_registration(project_id))
        try:
            snapshot = adapter.cancel_run(run).as_dict()
        except RunCapabilityUnsupported as error:
            raise _run_capability_error(error) from error
        if snapshot.get("state") == "not_found":
            raise HTTPException(status_code=404, detail=f"scheduler run not found: {run_id}")
        metastore_adapter_factory().run_metadata_repository.update_run(
            run_id,
            {"state": "cancelled", "cancelled_at": datetime.now().astimezone().isoformat()},
        )
        return {**snapshot, "display_timezone": _timezone_or_400(timezone)}

    return app


app = create_app()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="zeta4s-api")
    parser.add_argument("--host", default=os.environ.get("ZETA4S_API_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("ZETA4S_API_PORT", "8088")))
    args = parser.parse_args(argv)
    host = args.host
    port = args.port
    uvicorn.run("zeta4s.api.app:app", host=host, port=port)


if __name__ == "__main__":
    main()
