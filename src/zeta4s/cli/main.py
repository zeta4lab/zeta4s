"""Top-level z4s CLI."""

from __future__ import annotations

import base64
from contextlib import nullcontext
import fcntl
from datetime import datetime
import importlib.metadata
import json
import os
from pathlib import Path
import secrets
import sys
import time
import tomllib
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request
import uuid

import click
import yaml

from zeta4s.common.errors import UserFacingError
from zeta4s.common.messages import render_message
from zeta4s.common.time_display import format_display_time
from zeta4s.config.cli_config import DEFAULT_WORKSPACE_NAME
from zeta4s.config.cli_config import cli_home, ensure_cli_home
from zeta4s.config.cli_config import load_config as load_cli_config
from zeta4s.config.cli_config import remove_api
from zeta4s.config.cli_config import resolve_api, set_api, use_api
from zeta4s.config.cli_config import list_workspaces, set_workspace, use_workspace, workspace_info, workspace_path
from zeta4s.config.profile_config import (
    delete_profile,
    existing_profile_path,
    init_profile,
    list_profiles,
    load_profile,
    read_profile_text,
    select_profile,
)
from zeta4s.core import (
    ExecutionContext,
    LocalRunReporter,
    LocalRunner,
    StepExecutionState,
    step_output_binding_payload,
)
from zeta4s.core.step_executors import built_in_step_executor
from zeta4s.dbt.model_contract import (
    validate_dbt_model_contract,
)
from zeta4s.dbt.graph import selected_dbt_step_selections
from zeta4s.project.bundle import build_project_bundle, inspect_project, validate_project_configs
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import (
    DBT_PROJECT_TEMPLATE_TYPES,
    ProjectContext,
    create_project_skeleton,
    load_project_context,
    project_manifest_path,
    validate_project_id,
)
from zeta4s.project.pools import project_pool_payloads
from zeta4s.project.paths import project_relative_ref
from zeta4s.project.step_graph import (
    is_step_graph_config_path,
    step_graph_config_paths,
    validate_step_graph_config,
    validate_step_graph_config_path,
    validate_step_graph_configs,
)
from zeta4s.project.step_types import register_installed_step_types
from zeta4s.runtime.connections import ProfileConnectionResolver


PathParam = click.Path(path_type=Path)
API_TOKEN_ENV = "ZETA4S_API_TOKEN"
REPORTING_COMMANDS = {
    "profile-check",
    "project-check",
    "api-run-summary",
    "api-bootstrap",
    "api-deploy",
    "api-undeploy",
    "api-redeploy",
    "project-run",
}


def _api_token_path() -> Path:
    return cli_home() / "secrets" / "api.token"


TimezoneOption = click.option(
    "--timezone",
    envvar="ZETA4S_DISPLAY_TIMEZONE",
    help="Display timezone for run times and logs.",
)


class ApiReportError(RuntimeError):
    def __init__(self, status_code: int, report: dict[str, Any]) -> None:
        self.status_code = status_code
        self.report = report
        issues = report.get("issues") or []
        first_issue = issues[0] if issues and isinstance(issues[0], dict) else {}
        message = str(first_issue.get("message") or report.get("message") or f"zeta4s-api failed ({status_code})")
        super().__init__(message)


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml config must be a mapping: {path}")
    return data


def _workspace_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent
    for parent in [current, *current.parents]:
        if (parent / ".git").exists():
            return parent
    return current


def _auto_detect_project() -> Path | None:
    cwd = Path.cwd().resolve()
    for parent in [cwd, *cwd.parents]:
        if project_manifest_path(parent) is not None:
            return parent
    projects_dir = _workspace_root() / "projects"
    if projects_dir.exists():
        candidates = sorted(path for path in projects_dir.iterdir() if project_manifest_path(path) is not None)
        if len(candidates) == 1:
            return candidates[0]
    return None


def _resolve_project(project_root: Path | None) -> Path:
    if project_root:
        return project_root.expanduser().resolve()
    env_project = os.environ.get("ZETA4S_PROJECT")
    if env_project:
        return Path(env_project).expanduser().resolve()
    detected = _auto_detect_project()
    if detected:
        return detected.resolve()
    raise ValueError("project is required")


def _resolve_project_ref(project_ref: Path | None) -> Path:
    if project_ref is None:
        return _resolve_project(None)
    raw = str(project_ref)
    path = project_ref.expanduser()
    if path.exists() or path.is_absolute() or "/" in raw:
        return path.resolve()
    workspace_project = _workspace_root() / "projects" / raw
    if workspace_project.exists():
        return workspace_project.resolve()
    return path.resolve()


def _resolve_workspace_project(project_id: str) -> Path:
    workspace = workspace_path()
    if workspace is None:
        raise ValueError("workspace is not initialized: run `z4s work init`")
    normalized = validate_project_id(project_id)
    root = workspace / "projects" / normalized
    if project_manifest_path(root) is None:
        raise ValueError(f"project is not defined in workspace: {normalized}")
    return root


def _resolve_api_alias(
    api_alias: str | None = None, profile_data: dict[str, Any] | None = None
) -> str | dict[str, str] | None:
    if profile_data and "api_endpoint" in profile_data:
        api = {"url": profile_data["api_endpoint"], "alias": "profile"}
        token_env = profile_data.get("token_env")
        if token_env:
            token = os.environ.get(token_env)
            if token:
                api["token"] = token
        return api
    return api_alias or os.environ.get("ZETA4S_API")


def _resolve_project_api_alias(
    project_root: Path, api_alias: str | None = None, profile_data: dict[str, Any] | None = None
) -> str | dict[str, str] | None:
    return _resolve_api_alias(api_alias, profile_data)


def _config_connection_ids(config: dict[str, Any]) -> set[str]:
    conn_ids: set[str] = set()
    for step in config.get("steps") or []:
        if not isinstance(step, dict):
            continue
        if step.get("conn"):
            conn_ids.add(step["conn"])
        api = step.get("api")
        if isinstance(api, dict) and api.get("conn"):
            conn_ids.add(api["conn"])
    return conn_ids


def _now_stamp() -> tuple[str, str]:
    now = datetime.now().astimezone()
    offset = now.strftime("%z")
    stamp = now.strftime("%Y%m%dT%H%M%S") + (offset or "Z")
    iso = now.isoformat(timespec="seconds")
    return stamp, iso


def _report_id(prefix: str, stamp: str, short_id: str) -> str:
    return f"{prefix}_{stamp}_{short_id}"


def _report_dir(project_name: str) -> Path:
    return cli_home() / "reports" / project_name


def _cli_home_relative_ref(path: Path) -> str:
    try:
        return str(path.relative_to(cli_home()))
    except ValueError:
        return str(path)


def _write_report(
    project_name: str,
    report_name: str,
    report: dict[str, Any],
    *,
    id_field: str | None = None,
    id_prefix: str | None = None,
    include_project_field: bool = True,
) -> tuple[Path, Path]:
    if report_name not in REPORTING_COMMANDS:
        raise ValueError(f"unknown report name: {report_name}")
    stamp, created_at = _now_stamp()
    short_id = uuid.uuid4().hex[:8]
    if id_field and id_prefix and not report.get(id_field):
        report[id_field] = _report_id(id_prefix, stamp, short_id)
    report.setdefault("schema_version", 1)
    report.setdefault("created_at", created_at)
    if include_project_field:
        report.setdefault("project", project_name)
    report_dir = _report_dir(project_name)
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp_path = report_dir / f"{report_name}.{stamp}.{short_id}.json"
    latest_path = report_dir / f"{report_name}.latest.json"
    report["report_path"] = _cli_home_relative_ref(timestamp_path)
    report["latest_report_path"] = _cli_home_relative_ref(latest_path)
    content = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    timestamp_path.write_text(content, encoding="utf-8")
    latest_path.write_text(content, encoding="utf-8")
    return timestamp_path, latest_path


def _save_api_report(
    project_name: str, report_name: str, response: dict[str, Any], command: str
) -> tuple[dict[str, Any], Path]:
    report = dict(response)
    report.setdefault("command", command)
    report.setdefault("status", "passed")
    _, latest = _write_report(
        project_name,
        report_name,
        report,
        id_field="operation_id",
        id_prefix="op",
        include_project_field=report_name != "profile-check",
    )
    return report, latest


def _print_api_report_error(
    prefix: str, project_name: str, report_name: str, command: str, error: ApiReportError
) -> int:
    report, latest = _save_api_report(project_name, report_name, error.report, command)
    return _print_report_summary(prefix, report, latest)


def _progress_summary_text(summary: Any) -> str:
    if not isinstance(summary, dict) or not summary:
        return ""
    parts: list[str] = []
    for key, value in summary.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (list, dict)):
            if len(parts) >= 3:
                continue
            value_text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            value_text = str(value)
        parts.append(f"{key}={value_text}")
        if len(parts) >= 4:
            break
    return " " + " ".join(parts) if parts else ""


def _print_progress_event(prefix: str, event: dict[str, Any], timezone: str | None) -> None:
    if event.get("event") != "step":
        return
    timestamp = format_display_time(event.get("event_time"), timezone) or "-"
    index = event.get("index")
    total = event.get("total")
    position = f"{index}/{total}" if index and total else "-/-"
    step = event.get("step") or "-"
    status = event.get("status") or "-"
    click.echo(f"{timestamp} {prefix} {position} {step} {status}{_progress_summary_text(event.get('summary'))}")


def _stream_api_report(
    *,
    prefix: str,
    project_name: str,
    report_name: str,
    command: str,
    api_alias: str | dict[str, str] | None,
    endpoint: str,
    payload: dict[str, Any],
    timeout: float | None = None,
    timezone: str | None = None,
) -> tuple[dict[str, Any], Path, int]:
    complete_report: dict[str, Any] | None = None
    with _post_api_stream(api_alias, endpoint, payload, timeout=timeout) as response:
        for raw_line in response:
            line = raw_line.decode("utf-8").strip()
            if not line:
                continue
            event = json.loads(line)
            if event.get("event") == "step":
                _print_progress_event(prefix, event, timezone)
            elif event.get("event") == "complete":
                report = event.get("report")
                if isinstance(report, dict):
                    complete_report = report
    if complete_report is None:
        raise RuntimeError("zeta4s-api stream ended without a complete report")
    for nested in complete_report.get("nested_operations") or []:
        if isinstance(nested, dict) and isinstance(nested.get("report"), dict) and nested.get("report_name"):
            nested_report_name = str(nested["report_name"])
            operation = str(nested.get("operation") or "")
            _save_api_report(project_name, nested_report_name, nested["report"], f"z4s api {operation}")
    report, latest = _save_api_report(project_name, report_name, complete_report, command)
    return report, latest, _print_report_summary(prefix, report, latest)


def _format_api_error_detail(status_code: int, detail_text: str) -> str:
    try:
        payload = json.loads(detail_text)
    except json.JSONDecodeError:
        return detail_text
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if isinstance(detail, str):
        return detail
    if not isinstance(detail, dict):
        return detail_text

    issues = detail.get("issues") or []
    first_issue = issues[0] if issues and isinstance(issues[0], dict) else {}
    message = str(first_issue.get("message") or detail.get("message") or f"zeta4s-api failed ({status_code})")
    project = detail.get("project")
    summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
    lock_name = summary.get("lock_name")
    timeout = summary.get("lock_timeout_seconds")
    parts = [message]
    if project:
        parts.append(f"project={project}")
    if lock_name:
        parts.append(f"lock={lock_name}")
    if timeout is not None:
        parts.append(f"timeout={timeout}s")
    return "; ".join(parts)


def _api_error_report(detail_text: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(detail_text)
    except json.JSONDecodeError:
        return None
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if not isinstance(detail, dict):
        return None
    if not isinstance(detail.get("issues"), list):
        return None
    if not detail.get("command"):
        return None
    return detail


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _runtime_operation_timeout_margin_seconds() -> float:
    return _env_float("ZETA4S_RUNTIME_OPERATION_TIMEOUT_MARGIN_SECONDS", 15.0)


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


def _runtime_http_timeout_seconds(operation: str) -> float:
    configured = os.environ.get("ZETA4S_CLI_RUNTIME_OPERATION_TIMEOUT_SECONDS")
    if configured and configured.strip():
        return float(configured)
    margin = _runtime_operation_timeout_margin_seconds()
    deploy_seconds = _deploy_dag_discovery_timeout_seconds() + _deploy_dag_state_timeout_seconds() + 180.0
    undeploy_seconds = (
        _undeploy_dag_state_timeout_seconds()
        + _undeploy_active_terminate_timeout_seconds()
        + _undeploy_dag_delete_timeout_seconds()
        + 120.0
    )
    base_work_seconds = {
        "deploy": deploy_seconds,
        "undeploy": undeploy_seconds,
        "redeploy": deploy_seconds + undeploy_seconds + 120.0,
    }
    return max(60.0, base_work_seconds[operation] + margin)


def _request_json(
    api_alias: str | dict[str, str] | None,
    endpoint: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    api = resolve_api(_resolve_api_alias(api_alias))
    url = api["url"].rstrip("/") + endpoint
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if api.get("token"):
        headers["Authorization"] = f"Bearer {api['token']}"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        report = _api_error_report(detail)
        if report is not None:
            raise ApiReportError(e.code, report) from e
        raise RuntimeError(f"zeta4s-api failed ({e.code}): {_format_api_error_detail(e.code, detail)}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"zeta4s-api unreachable: {url}: {e}") from e
    return json.loads(data or "{}")


def _post_json(
    api_alias: str | dict[str, str] | None, endpoint: str, payload: dict[str, Any], *, timeout: float = 60.0
) -> dict[str, Any]:
    return _request_json(api_alias, endpoint, method="POST", payload=payload, timeout=timeout)


def _get_json(api_alias: str | dict[str, str] | None, endpoint: str, *, timeout: float = 60.0) -> dict[str, Any]:
    return _request_json(api_alias, endpoint, method="GET")


def _open_api_stream(api_alias: str | dict[str, str] | None, endpoint: str, *, timeout: float = 60.0) -> Any:
    api = resolve_api(_resolve_api_alias(api_alias))
    url = api["url"].rstrip("/") + endpoint
    headers: dict[str, str] = {}
    if api.get("token"):
        headers["Authorization"] = f"Bearer {api['token']}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        return urllib.request.urlopen(request, timeout=None)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"zeta4s-api failed ({e.code}): {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"zeta4s-api unreachable: {url}: {e}") from e


def _post_api_stream(api_alias: str | None, endpoint: str, payload: dict[str, Any], *, timeout: float | None = None):
    api = resolve_api(_resolve_api_alias(api_alias))
    url = api["url"].rstrip("/") + endpoint
    headers = {"Content-Type": "application/json"}
    if api.get("token"):
        headers["Authorization"] = f"Bearer {api['token']}"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        report = _api_error_report(detail)
        if report is not None:
            raise ApiReportError(e.code, report) from e
        raise RuntimeError(f"zeta4s-api failed ({e.code}): {_format_api_error_detail(e.code, detail)}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"zeta4s-api unreachable: {url}: {e}") from e


def _query_string(params: dict[str, Any]) -> str:
    clean = {key: value for key, value in params.items() if value is not None}
    return f"?{urllib.parse.urlencode(clean)}" if clean else ""


def _project_payload(project_root: Path) -> dict[str, Any]:
    bundle, artifact_id = build_project_bundle(project_root)
    project = load_project_context(project_root)
    return {
        "project_id": project.project_id,
        "artifact_id": artifact_id,
        "bundle_base64": base64.b64encode(bundle).decode("ascii"),
    }


def _deploy_payload(project_root: Path, profile_id: str, profile_data: dict[str, Any]) -> dict[str, Any]:
    payload = _project_payload(project_root)
    payload["profile_id"] = profile_id
    payload["profile"] = profile_data
    return payload


def _registered_project_artifact(api_alias: str | None, project_name: str) -> dict[str, Any]:
    return _get_json(api_alias, f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/artifact")


def _check_stale_deployment(root: Path, api_alias: str | None, mode: str) -> dict[str, Any]:
    if mode == "ignore":
        return {"status": "ignored"}
    bundle, local_artifact_id = build_project_bundle(root)
    del bundle
    project_name = load_project_context(root).project_id
    remote = _registered_project_artifact(api_alias, project_name)
    remote_artifact_id = remote.get("artifact_id")
    result = {
        "status": "fresh" if remote_artifact_id == local_artifact_id else "stale",
        "local_artifact_id": local_artifact_id,
        "registered_artifact_id": remote_artifact_id,
        "registered_at": remote.get("registered_at"),
    }
    if result["status"] == "stale":
        message = (
            "stale deployment: local bundle artifact_id="
            f"{local_artifact_id}, registered artifact_id={remote_artifact_id}. "
            "Run `z4s api deploy` before triggering the DAG."
        )
        if mode == "fail":
            raise ValueError(message)
        click.echo(f"warning: {message}", err=True)
    return result


def _issue_from_exception(code: str, exc: Exception, **extra: Any) -> dict[str, Any]:
    if isinstance(exc, UserFacingError):
        issue_code = exc.code
        params = dict(exc.params)
        default = exc.default_message
    else:
        issue_code = code
        params = {}
        default = str(exc)
    return {
        "code": issue_code,
        "severity": "error",
        "message": _render_issue_message(issue_code, params, default),
        "params": params,
        **extra,
    }


def _render_issue_message(code: str, params: dict[str, Any], default: str) -> str:
    message = render_message(code, params, default=default)
    if default and default != message:
        return f"{message}\n{default}"
    return message


def _run_static_verify(project_root: Path, profile_id: str, profile_data: dict[str, Any]) -> dict[str, Any]:
    project = load_project_context(project_root)
    report: dict[str, Any] = {
        "status": "passed",
        "command": "z4s project check",
        "summary": {},
        "gates": [],
        "issues": [],
        "next_commands": [],
    }

    def gate(name: str, fn: Callable[[], dict[str, Any] | None]) -> None:
        try:
            summary = fn() or {}
            report["gates"].append({"name": name, "status": "passed", "summary": summary, "issue_codes": []})
        except Exception as e:
            code = f"Z4E_STATIC_{name.upper()}_001"
            report["status"] = "failed"
            report["gates"].append({"name": name, "status": "failed", "summary": {}, "issue_codes": [code]})
            report["issues"].append(_issue_from_exception(code, e, gate=name))

    raw_config_items: list[tuple[Path, dict[str, Any]]] = []
    step_graph_items: list[tuple[Path, dict[str, Any]]] = []
    config_items: list[tuple[Path, dict[str, Any]]] = []

    def raw_yaml_configs() -> list[tuple[Path, dict[str, Any]]]:
        nonlocal raw_config_items
        if not raw_config_items:
            paths = step_graph_config_paths(project.jobs_dir)
            for path in paths:
                validate_step_graph_config_path(path)
            raw_config_items = [(path, _load_yaml(path)) for path in paths]
        return raw_config_items

    def step_graph_configs() -> list[tuple[Path, dict[str, Any]]]:
        nonlocal step_graph_items
        if not step_graph_items:
            step_graph_items = [
                (path, config) for path, config in raw_yaml_configs() if is_step_graph_config_path(path)
            ]
        return step_graph_items

    def project_structure() -> dict[str, Any]:
        required_paths = [project.jobs_dir]
        if _project_requires_dbt(raw_yaml_configs()):
            required_paths.append(project.dbt_dir)
            for conn_id in _dbt_conn_ids(raw_yaml_configs()):
                dbt_project_dir = project.dbt_project_dir(conn_id)
                required_paths.extend([dbt_project_dir, dbt_project_dir / "models"])
        missing = [project_relative_ref(project.root, path) for path in required_paths if not path.exists()]
        if project_manifest_path(project.root) is None:
            missing.insert(0, "project.yml")
        if missing:
            raise UserFacingError("project.structure.missing_entries", params={"entries": ", ".join(missing)})
        return {"project": project.project_id}

    def yaml_schema() -> dict[str, Any]:
        nonlocal config_items
        config_items = validate_project_configs(project.root)
        return {"configs": len(config_items)}

    def step_graph_schema() -> dict[str, Any]:
        return validate_step_graph_configs(step_graph_configs(), project.root)

    def dbt_model_contract() -> dict[str, Any]:
        if not _project_requires_dbt(raw_yaml_configs()):
            return {"models": 0, "required": False}
        checked = 0
        for conn_id in _dbt_conn_ids(raw_yaml_configs()):
            run_models, test_models = _dbt_models_by_conn(raw_yaml_configs()).get(conn_id, ([], []))
            checked += validate_dbt_model_contract(
                project.dbt_project_dir(conn_id),
                run_models=run_models,
                test_models=test_models,
            ).checked
        return {"models": checked, "required": True}

    def profile_connections() -> dict[str, Any]:
        configs = [config for _, config in config_items]
        required_connections: set[str] = set()
        for config in configs:
            required_connections.update(_config_connection_ids(config))
        defined_connections = set((profile_data.get("connections") or {}).keys())
        missing_connections = sorted(required_connections - defined_connections)
        if missing_connections:
            raise ValueError("missing profile connections: " + ", ".join(missing_connections))
        return {
            "profile": profile_id,
            "connections": len(defined_connections),
            "project_pools": project_pool_payloads(project.project_id, project.root),
        }

    def step_types() -> dict[str, Any]:
        # 설치된 외부 step type 을 config 검증 전에 등록해야 뒤따르는 yaml_schema/
        # step_graph_schema 가 외부 type membership 을 통과한다. broken 플러그인(중복/
        # schema위반/runtime_callable 해석 실패)은 이 gate 의 실패로 계약 위반이 보고된다.
        return {"registered": register_installed_step_types()}

    gate("step_types", step_types)
    gate("project_structure", project_structure)
    gate("dbt_model_contract", dbt_model_contract)
    gate("yaml_schema", yaml_schema)
    gate("step_graph_schema", step_graph_schema)
    gate("profile_connections", profile_connections)
    report["summary"] = {
        "gate_count": len(report["gates"]),
        "error_count": len(report["issues"]),
        "profile": profile_id,
    }
    return report


def _run_local_project_job(
    project: ProjectContext, job_id: str, profile_id: str, profile_data: dict[str, Any]
) -> dict[str, Any]:
    config_path, config = _project_job_config(project, job_id)
    job = validate_step_graph_config(config_path, config)
    plan = build_step_graph_execution_plan(job)
    runtime_home = cli_home() / "runtime"
    runtime_home.mkdir(parents=True, exist_ok=True)
    run_id = f"local__{project.project_id}__{plan.job_id}__{uuid.uuid4().hex[:12]}"
    reporter = LocalRunReporter()
    executors = {
        step.id: built_in_step_executor(
            project=project,
            plan=plan,
            plan_step=step,
            runtime_home=str(runtime_home),
        )
        for step in plan.steps
    }
    old_runtime_home = os.environ.get("ZETA4S_API_HOME")
    os.environ["ZETA4S_API_HOME"] = str(runtime_home)
    try:
        result = LocalRunner(executors).run(
            plan,
            ExecutionContext(
                project_id=project.project_id,
                job_id=plan.job_id,
                run_id=run_id,
                profile=profile_id,
                params={"profile": profile_id},
                connection_resolver=ProfileConnectionResolver(profile_data),
                reporter=reporter,
            ),
        )
    finally:
        if old_runtime_home is None:
            os.environ.pop("ZETA4S_API_HOME", None)
        else:
            os.environ["ZETA4S_API_HOME"] = old_runtime_home
    return {
        "status": _project_run_status(result),
        "result_state": result.state.value,
        "command": "z4s run",
        "project": project.project_id,
        "job_id": plan.job_id,
        "run_id": run_id,
        "profile": profile_id,
        "runtime_home": str(runtime_home),
        "config": project_relative_ref(project.root, config_path),
        "summary": {
            "steps": len(result.steps),
            "succeeded": sum(1 for step in result.steps if step.succeeded),
            "failed": sum(1 for step in result.steps if step.failed),
            "skipped": sum(1 for step in result.steps if step.skipped),
            "terminal_steps": list(result.terminal_step_ids),
        },
        "terminal_outputs": {
            step_id: {output_name: _step_output_binding_report(binding) for output_name, binding in outputs.items()}
            for step_id, outputs in result.terminal_outputs.items()
        },
        "steps": [
            {
                "step_id": step.step_id,
                "type": step.step_type,
                "state": step.state.value,
                "outputs": step.outputs,
                "failure": ({"type": step.failure.type, "message": step.failure.message} if step.failure else None),
                "skipped_reason": step.skipped_reason,
            }
            for step in result.steps
        ],
        "events": reporter.events,
        "issues": _run_result_issues(result),
    }


def _project_run_status(result) -> str:
    if result.succeeded:
        return "passed"
    if result.state == StepExecutionState.SKIPPED:
        return "skipped"
    return "failed"


def _step_output_binding_report(binding) -> dict[str, Any]:
    return step_output_binding_payload(binding)


def _project_job_config(project: ProjectContext, job_id: str) -> tuple[Path, dict[str, Any]]:
    matches: list[tuple[Path, dict[str, Any]]] = []
    for config_path in step_graph_config_paths(project.jobs_dir):
        config = _load_yaml(config_path)
        if str(config.get("job_id") or "") == job_id:
            matches.append((config_path, config))
    if not matches:
        raise ValueError(f"job is not defined in project: {project.project_id} {job_id}")
    if len(matches) > 1:
        names = ", ".join(project_relative_ref(project.root, path) for path, _ in matches)
        raise ValueError(f"job_id is duplicated: {job_id} ({names})")
    return matches[0]


def _run_result_issues(result) -> list[dict[str, Any]]:
    issues = []
    for step in result.steps:
        if step.failed:
            failure = step.failure
            issues.append(
                {
                    "code": "Z4E_RUN_STEP_FAILED",
                    "severity": "error",
                    "step": step.step_id,
                    "message": failure.message if failure else f"step failed: {step.step_id}",
                    "type": failure.type if failure else None,
                }
            )
    return issues


def _print_project_run_summary(report: dict[str, Any], report_path: Path, latest_path: Path) -> int:
    status = report.get("status") or "failed"
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    click.echo(
        "[project][run] "
        f"{status}: project={report.get('project')} job={report.get('job_id')} "
        f"profile={report.get('profile')} "
        f"run_id={report.get('run_id')} "
        f"steps={summary.get('steps')} succeeded={summary.get('succeeded')} "
        f"failed={summary.get('failed')} skipped={summary.get('skipped')}"
    )
    for issue in (report.get("issues") or [])[:3]:
        click.echo(f"- {issue.get('code')}: {issue.get('message')}", err=True)
        _print_issue_detail(issue)
    click.echo(f"report: {_cli_home_relative_ref(report_path)}")
    click.echo(f"latest: {_cli_home_relative_ref(latest_path)}")
    return 1 if status == "failed" else 0


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


def _project_requires_dbt(config_items: list[tuple[Path, dict[str, Any]]]) -> bool:
    return any(
        isinstance(step, dict) and step.get("type") in {"dbt.run", "dbt.test"}
        for _, config in config_items
        for step in (config.get("steps") or [])
    )


def _report_summary_message(prefix: str, status: str, issue_count: int) -> str:
    message_codes = {
        ("[project][check]", "passed"): "project.check.passed",
        ("[project][check]", "failed"): "project.check.failed",
    }
    code = message_codes.get((prefix, status))
    if not code:
        return f"{prefix} {status}: {issue_count} issues"
    return f"{prefix} {render_message(code, {'issues': issue_count})}"


def _print_report_summary(prefix: str, report: dict[str, Any], latest_path: Path) -> int:
    status = report.get("status") or "failed"
    issues = report.get("issues") or []
    click.echo(_report_summary_message(prefix, str(status), len(issues)))
    for issue in issues[:3]:
        click.echo(f"- {issue.get('code')}: {issue.get('message')}", err=True)
        _print_issue_detail(issue)
    if len(issues) > 3:
        click.echo(render_message("report.more_issues", {"count": len(issues) - 3}), err=True)
    try:
        report_ref = latest_path.relative_to(cli_home())
    except ValueError:
        report_ref = latest_path
    click.echo(f"path: {report_ref}")
    return 0 if status == "passed" else 1


def _print_issue_detail(issue: Any) -> None:
    if not isinstance(issue, dict):
        return
    fields = [
        ("job", "job"),
        ("write", "write"),
        ("model", "model"),
        ("target_table", "target"),
        ("column", "column"),
        ("clickhouse_type", "ClickHouse type"),
        ("arrow_type", "Arrow type"),
        ("reason", "reason"),
        ("suggestion", "suggestion"),
    ]
    for key, label in fields:
        value = issue.get(key)
        if value:
            click.echo(f"  {label}: {value}", err=True)


def _error(e: Exception, code: int = 1) -> int:
    if isinstance(e, UserFacingError):
        message = render_message(e.code, e.params, default=e.default_message)
    else:
        message = str(e)
    click.echo(render_message("cli.error", {"message": message}), err=True)
    return code


def _catch(fn: Callable[[], int]) -> int:
    try:
        code = fn() or 0
        if code:
            raise click.exceptions.Exit(code)
        return code
    except (click.exceptions.Exit, BrokenPipeError):
        # 출력을 읽는 쪽이 먼저 닫힌 것은 명령 실패가 아니다. main() 이 조용히 끝낸다.
        raise
    except click.UsageError as e:
        click.echo(render_message("cli.error", {"message": str(e)}), err=True)
        raise click.exceptions.Exit(2)
    except (RuntimeError, ValueError, FileExistsError, OSError) as e:
        raise click.exceptions.Exit(_error(e))


def _cli_version() -> str:
    try:
        return importlib.metadata.version("zeta4s")
    except importlib.metadata.PackageNotFoundError:
        pass
    for parent in Path(__file__).resolve().parents:
        pyproject_path = parent / "pyproject.toml"
        if pyproject_path.exists():
            data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
            project = data.get("project") or {}
            if project.get("name") == "zeta4s" and project.get("version"):
                return str(project["version"])
    return "unknown"


def _show_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if value and not ctx.resilient_parsing:
        click.echo(f"z4s, version {_cli_version()}")
        ctx.exit()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--version", is_flag=True, is_eager=True, expose_value=False, callback=_show_version)
def cli() -> None:
    """Operate zeta4s project artifacts and zeta4s-api."""
    ensure_cli_home()


@cli.group("work")
def work() -> None:
    """Manage the registered zeta4s workspace."""


@cli.group("profile")
def profile() -> None:
    """Manage workspace execution profiles."""


@cli.group()
def project() -> None:
    """Create and statically manage project artifact directories."""


@work.command("init")
@click.argument("name", required=False)
def work_init(name: str | None) -> int:
    def run() -> int:
        workspace_name = name or DEFAULT_WORKSPACE_NAME
        workspace_ref = Path(workspace_name)
        if workspace_ref.is_absolute() or len(workspace_ref.parts) != 1:
            raise ValueError("workspace name must be a single directory name")
        workspace = (Path.cwd() / workspace_ref).resolve()
        if not workspace.exists():
            (workspace / "profiles").mkdir(parents=True)
            (workspace / "projects").mkdir()
        path = set_workspace(workspace_name, workspace)
        click.echo(f"registered workspace '{workspace_name}': {workspace}")
        click.echo(f"saved workspace config: {path}")
        return 0

    return _catch(run)


@work.command("list")
def work_list() -> int:
    def run() -> int:
        info = list_workspaces()
        workspaces = info.get("workspaces") or {}
        active = info.get("active_workspace")
        if not workspaces:
            click.echo("no workspaces registered")
            return 0
        for w_name, w_path in workspaces.items():
            prefix = "* " if w_name == active else "  "
            click.echo(f"{prefix}{w_name}: {w_path}")
        return 0

    return _catch(run)


@work.command("use")
@click.argument("name")
def work_use(name: str) -> int:
    def run() -> int:
        use_workspace(name)
        click.echo(f"switched to workspace '{name}'")
        return 0

    return _catch(run)


@work.command("show")
def work_show() -> int:
    def run() -> int:
        click.echo(yaml.safe_dump(workspace_info(), sort_keys=False, allow_unicode=True).rstrip())
        return 0

    return _catch(run)


@profile.command("init")
@click.argument("profile_id")
def profile_init(profile_id: str) -> int:
    def run() -> int:
        path = init_profile(profile_id)
        click.echo(f"created profile: {path}")
        return 0

    return _catch(run)


@profile.command("edit")
@click.argument("profile_id")
def profile_edit(profile_id: str) -> int:
    def run() -> int:
        path = existing_profile_path(profile_id)
        if not path.exists():
            raise ValueError(f"profile is not defined: {profile_id}")
        click.edit(filename=str(path))
        load_profile(profile_id)
        click.echo(f"profile {profile_id}: saved")
        return 0

    return _catch(run)


@profile.command("show")
@click.argument("profile_id")
def profile_show(profile_id: str) -> int:
    def run() -> int:
        _, content = read_profile_text(profile_id)
        click.echo(content.rstrip())
        return 0

    return _catch(run)


@profile.command("check")
@click.argument("profile_id")
@click.option("--api", "api_alias", help="API connection name.")
def profile_check(profile_id: str, api_alias: str | None) -> int:
    def run() -> int:
        profile_data = load_profile(profile_id)
        response = _post_json(
            api_alias,
            "/api/v1/profiles/check",
            {"profile": profile_data},
        )
        report_payload = {"profile": profile_id, **response}
        report, latest = _save_api_report(profile_id, "profile-check", report_payload, "z4s profile check")
        return _print_report_summary(f"[profile][check] {profile_id}", report, latest)

    return _catch(run)


@profile.command("list")
def profile_list() -> int:
    def run() -> int:
        click.echo(yaml.safe_dump({"profiles": list_profiles()}, sort_keys=False, allow_unicode=True).rstrip())
        return 0

    return _catch(run)


@profile.command("delete")
@click.argument("profile_id")
@click.option("-y", "--yes", is_flag=True, help="Delete without confirmation.")
def profile_delete(profile_id: str, yes: bool) -> int:
    def run() -> int:
        path = existing_profile_path(profile_id)
        if not path.exists():
            raise ValueError(f"profile is not defined: {profile_id}")
        if not yes:
            click.confirm(f"Delete profile {profile_id}?", abort=True)
        deleted = delete_profile(profile_id)
        click.echo(f"deleted profile: {deleted}")
        return 0

    return _catch(run)


@project.command("init")
@click.argument("name")
@click.option("--force", is_flag=True, help="Replace an existing project directory.")
@click.option("--profile", "profile_id", help="Workspace profile id used by --with-dbt.")
@click.option("--with-dbt", is_flag=True, help="Create dbt projects for dbt-capable profile connections.")
def project_init(name: str, force: bool, profile_id: str | None, with_dbt: bool) -> int:
    return _catch(lambda: _project_init(name, force, profile_id, with_dbt))


def _project_init(name: str, force: bool, profile_id: str | None, with_dbt: bool) -> int:
    workspace = workspace_path()
    if workspace is None:
        raise ValueError("workspace is not initialized: run `z4s work init`")
    projects_dir = workspace / "projects"
    dbt_conn_ids: list[str] = []
    if with_dbt:
        if not profile_id:
            raise ValueError("--profile is required with --with-dbt")
        _, profile_data = select_profile(profile_id)
        dbt_conn_ids = _dbt_template_conn_ids(profile_data)
        if not dbt_conn_ids:
            raise ValueError("profile has no dbt-capable connections: clickhouse, oracle")
    project_path = create_project_skeleton(name=name, projects_dir=projects_dir, force=force, dbt_conn_ids=dbt_conn_ids)
    click.echo(f"created project: {project_path}")
    for conn_id in dbt_conn_ids:
        click.echo(f"created dbt project: {project_path / 'dbt' / conn_id}")
    return 0


def _dbt_template_conn_ids(profile_data: dict[str, Any]) -> list[str]:
    connections = profile_data.get("connections") or {}
    result = []
    for conn_id, connection in sorted(connections.items()):
        if not isinstance(connection, dict):
            continue
        connection_type = str(connection.get("type") or "").strip().lower()
        if connection_type in DBT_PROJECT_TEMPLATE_TYPES:
            result.append(str(conn_id))
    return result


@project.command("check")
@click.argument("project_id")
@click.option("--profile", "profile_id", help="Workspace profile id.")
def project_check(project_id: str, profile_id: str | None) -> int:
    def run() -> int:
        root = _resolve_workspace_project(project_id)
        project = load_project_context(root)
        selected_profile_id, profile_data = select_profile(profile_id)
        report = _run_static_verify(root, selected_profile_id, profile_data)
        _, latest = _write_report(
            project.project_id, "project-check", report, id_field="check_report_id", id_prefix="pc"
        )
        return _print_report_summary("[project][check]", report, latest)

    return _catch(run)


def _acquire_run_lock(project_id: str, job_id: str) -> int:
    lock_dir = cli_home() / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / f"{project_id}_{job_id}.lock"
    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        raise ValueError(f"Job '{job_id}' in project '{project_id}' is already running locally.")


@cli.command("run")
@click.argument("project_id")
@click.argument("job_id")
@click.option("--profile", "profile_id", help="Workspace profile id.")
def project_run(project_id: str, job_id: str, profile_id: str | None) -> int:
    def run() -> int:
        fd = _acquire_run_lock(project_id, job_id)
        try:
            root = _resolve_workspace_project(project_id)
            project = load_project_context(root)
            selected_profile_id, profile_data = select_profile(profile_id)
            check_report = _run_static_verify(root, selected_profile_id, profile_data)
            if check_report.get("status") != "passed":
                _, check_latest = _write_report(
                    project.project_id, "project-check", check_report, id_field="check_report_id", id_prefix="pc"
                )
                _print_report_summary("[project][check]", check_report, check_latest)
                return 1
            report = _run_local_project_job(project, job_id, selected_profile_id, profile_data)
            report_path, latest = _write_report(
                project.project_id, "project-run", report, id_field="run_report_id", id_prefix="run"
            )
            return _print_project_run_summary(report, report_path, latest)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    return _catch(run)


@project.command("graph")
@click.argument("project_id")
@click.option("--api", "api_alias", help="API connection name. If set, read deployed project graph from the API.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "yaml", "json", "mermaid", "dot"]),
    default="text",
    show_default=True,
)
def project_graph(project_id: str, api_alias: str | None, output_format: str) -> int:
    def run() -> int:
        if api_alias:
            resolved_project_id = validate_project_id(project_id)
            response = _get_json(
                api_alias, f"/api/v1/projects/{urllib.parse.quote(resolved_project_id, safe='')}/graph"
            )
            response["scope"] = "deployed"
            response["api"] = api_alias
        else:
            root = _resolve_workspace_project(project_id)
            response = inspect_project(root)
            response["scope"] = "workspace"
        click.echo(_render_project_graph(response, output_format))
        return 0

    return _catch(run)


def _render_project_graph(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "yaml":
        return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).rstrip()
    if output_format == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False)
    if output_format == "mermaid":
        return _project_graph_mermaid(payload)
    if output_format == "dot":
        return _project_graph_dot(payload)
    return _project_graph_text(payload)


def _project_graph_jobs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    jobs = payload.get("job_graphs")
    return [job for job in jobs if isinstance(job, dict)] if isinstance(jobs, list) else []


def _node_label(node: dict[str, Any]) -> str:
    label = str(node.get("label") or node.get("id") or "")
    step_type = node.get("step_type")
    return f"{label} [{step_type}]" if step_type else label


def _project_graph_text(payload: dict[str, Any]) -> str:
    lines = [
        f"project: {payload.get('project_id') or payload.get('project')}",
        f"scope: {payload.get('scope') or 'workspace'}",
    ]
    for job in _project_graph_jobs(payload):
        lines.append("")
        lines.append(f"job: {job.get('job_id')} ({job.get('config')})")
        nodes = {str(node.get("id")): node for node in job.get("nodes") or [] if isinstance(node, dict)}
        incoming = {str(edge.get("target")) for edge in job.get("edges") or [] if isinstance(edge, dict)}
        roots = [node_id for node_id in nodes if node_id not in incoming]
        if not roots:
            roots = list(nodes)
        for node_id in roots:
            _append_graph_text_node(lines, nodes, job.get("edges") or [], node_id, "", set())
    return "\n".join(lines)


def _append_graph_text_node(
    lines: list[str], nodes: dict[str, dict[str, Any]], edges: list[Any], node_id: str, prefix: str, seen: set[str]
) -> None:
    node = nodes.get(node_id)
    if node is None:
        return
    marker = "*" if node.get("terminal") else "-"
    lines.append(f"{prefix}{marker} {_node_label(node)}")
    if node_id in seen:
        return
    seen.add(node_id)
    children = [
        str(edge.get("target"))
        for edge in edges
        if isinstance(edge, dict) and str(edge.get("source")) == node_id and str(edge.get("target")) in nodes
    ]
    for child_id in children:
        _append_graph_text_node(lines, nodes, edges, child_id, f"{prefix}  -> ", seen)


def _safe_mermaid_id(value: str) -> str:
    return "n_" + "".join(ch if ch.isalnum() else "_" for ch in value)


def _mermaid_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', "'").replace("\n", "<br/>")


def _project_graph_mermaid(payload: dict[str, Any]) -> str:
    lines = ["flowchart TD"]
    for job in _project_graph_jobs(payload):
        lines.append(
            f'  subgraph {_safe_mermaid_id(str(job.get("job_id")))}["{_mermaid_label(str(job.get("job_id")))}"]'
        )
        for node in job.get("nodes") or []:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id"))
            lines.append(f'    {_safe_mermaid_id(node_id)}["{_mermaid_label(_node_label(node))}"]')
        for edge in job.get("edges") or []:
            if not isinstance(edge, dict):
                continue
            lines.append(
                f"    {_safe_mermaid_id(str(edge.get('source')))} --> {_safe_mermaid_id(str(edge.get('target')))}"
            )
        lines.append("  end")
    return "\n".join(lines)


def _dot_id(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _project_graph_dot(payload: dict[str, Any]) -> str:
    project_id = str(payload.get("project_id") or "project")
    lines = [f"digraph {_safe_mermaid_id(project_id)} {{"]
    for job in _project_graph_jobs(payload):
        job_id = str(job.get("job_id"))
        lines.append(f"  subgraph cluster_{_safe_mermaid_id(job_id)} {{")
        lines.append(f"    label={_dot_id(job_id)};")
        for node in job.get("nodes") or []:
            if isinstance(node, dict):
                lines.append(f"    {_dot_id(str(node.get('id')))} [label={_dot_id(_node_label(node))}];")
        for edge in job.get("edges") or []:
            if isinstance(edge, dict):
                lines.append(f"    {_dot_id(str(edge.get('source')))} -> {_dot_id(str(edge.get('target')))};")
        lines.append("  }")
    lines.append("}")
    return "\n".join(lines)


@cli.group("api")
def api() -> None:
    """Operate zeta4s-api project and adapter contract."""


@api.command("connect")
@click.argument("alias", required=False, default="local", metavar="NAME")
@click.option("--url", default="http://127.0.0.1:18088", show_default=True)
@click.option("--env-file", type=PathParam, default=Path(".env"), show_default=True)
@click.option("--token-env", help="Environment variable containing bearer token.")
@click.option("--token-file", type=PathParam, help="API token file. Defaults to z4s home secrets/api.token.")
@click.option("--force", is_flag=True, help="Generate a new API token even when one already exists.")
@click.option("--no-env-file", is_flag=True, help="Do not write ZETA4S_API_TOKEN to an env file.")
def api_connect(
    alias: str, url: str, env_file: Path, token_env: str | None, token_file: Path | None, force: bool, no_env_file: bool
) -> int:
    def run() -> int:
        if token_env:
            if token_file:
                raise ValueError("--token-env and --token-file are mutually exclusive")
            if force:
                raise ValueError("--token-env and --force are mutually exclusive")
            path = set_api(alias, url, token_env=token_env, default=True)
            click.echo(f"default API: {alias} ({path})")
            return 0
        token_path = (token_file or _api_token_path()).expanduser()
        file_token = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else None
        env_token = None if no_env_file else _env_file_value(env_file, API_TOKEN_ENV)
        token = secrets.token_hex(32) if force else (file_token or env_token or secrets.token_hex(32))
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(f"{token}\n", encoding="utf-8")
        try:
            token_path.chmod(0o600)
        except OSError:
            pass
        if not no_env_file:
            _write_env_file_value(env_file, API_TOKEN_ENV, token)
        path = set_api(alias, url, token_file=token_path, default=True)
        click.echo(f"saved API token file: {token_path}")
        click.echo(f"default API: {alias} ({path})")
        return 0

    return _catch(run)


@api.command("list")
def api_list() -> int:
    data = load_cli_config()
    default = data.get("default_api")
    for alias, item in sorted((data.get("apis") or {}).items()):
        marker = "*" if alias == default else " "
        click.echo(f"{marker} {alias}\t{item.get('url')}")
    return 0


@api.command("use")
@click.argument("alias", metavar="NAME")
def api_use(alias: str) -> int:
    return _catch(lambda: _api_use(alias))


def _api_use(alias: str) -> int:
    path = use_api(alias)
    click.echo(f"default API: {alias} ({path})")
    return 0


@api.command("remove")
@click.argument("alias", metavar="NAME")
def api_remove(alias: str) -> int:
    def run() -> int:
        path = remove_api(alias)
        click.echo(f"removed API {alias}: {path}")
        return 0

    return _catch(run)


@api.group("secret")
def api_secret() -> None:
    """Manage encrypted zeta4s-api secrets."""


@api.command("bootstrap")
@click.option("--api", "api_alias", help="API connection name.")
def api_bootstrap(api_alias: str | None) -> int:
    def run() -> int:
        response = _post_json(api_alias, "/api/v1/platform/bootstrap", {})
        report, latest = _save_api_report("_platform", "api-bootstrap", response, "z4s api bootstrap")
        return _print_report_summary("[api][bootstrap]", report, latest)

    return _catch(run)


@api.command("status")
@click.option("--api", "api_alias", help="API connection name.")
def api_status(api_alias: str | None) -> int:
    def run() -> int:
        response = _get_json(api_alias, "/api/v1/platform/status")
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        if response.get("bootstrap_status") != "ready" or response.get("schema_status") != "ok":
            return 1
        return 0

    return _catch(run)


@api_secret.command("set")
@click.argument("secret_key")
@click.option("--api", "api_alias", help="API connection name.")
def api_secret_set(secret_key: str, api_alias: str | None) -> int:
    def run() -> int:
        if not _stdin_is_interactive():
            value = sys.stdin.read()
            if value == "":
                raise ValueError("secret value from stdin must not be empty")
        else:
            value = click.prompt("Secret value", hide_input=True, confirmation_prompt=True)
        response = _post_json(api_alias, "/api/v1/secrets", {"secret_key": secret_key, "value": value})
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        _exit_if_failed_api_response(response)
        return 0

    return _catch(run)


def _stdin_is_interactive() -> bool:
    return sys.stdin.isatty()


def _exit_if_failed_api_response(response: dict[str, Any]) -> None:
    if response.get("status") == "failed":
        raise click.exceptions.Exit(1)


@api_secret.command("list")
@click.option("--api", "api_alias", help="API connection name.")
def api_secret_list(api_alias: str | None) -> int:
    def run() -> int:
        response = _get_json(api_alias, "/api/v1/secrets")
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        _exit_if_failed_api_response(response)
        return 0

    return _catch(run)


@api_secret.command("check")
@click.argument("secret_key")
@click.option("--api", "api_alias", help="API connection name.")
def api_secret_check(secret_key: str, api_alias: str | None) -> int:
    def run() -> int:
        response = _post_json(api_alias, "/api/v1/secrets/check", {"secret_key": secret_key})
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        _exit_if_failed_api_response(response)
        if not response.get("active") or not response.get("decryptable"):
            raise click.exceptions.Exit(1)
        return 0

    return _catch(run)


@api_secret.group("keyring")
def api_secret_keyring() -> None:
    """Inspect and rotate the zeta4s-api master key generations."""


@api_secret_keyring.command("status")
@click.option("--api", "api_alias", help="API connection name.")
def api_secret_keyring_status(api_alias: str | None) -> int:
    def run() -> int:
        response = _get_json(api_alias, "/api/v1/secrets/keyring")
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        _exit_if_failed_api_response(response)
        if not response.get("valid"):
            raise click.exceptions.Exit(1)
        return 0

    return _catch(run)


@api_secret_keyring.command("rotate")
@click.option("--api", "api_alias", help="API connection name.")
def api_secret_keyring_rotate(api_alias: str | None) -> int:
    """Re-encrypt stored secrets to the active master key generation.

    The keyring file itself is operator-owned input. Add or drop generations in
    the file (or the Kubernetes Secret) first, then run this to move ciphertext.
    """

    def run() -> int:
        response = _post_json(api_alias, "/api/v1/secrets/keyring/rotate", {})
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        _exit_if_failed_api_response(response)
        if response.get("remaining_by_key_id") or response.get("without_generation"):
            # 옛 세대 ciphertext 나 세대를 모르는 row 가 남아 있으면 keyring 에서
            # 세대를 지우면 안 된다. 지우는 순간 그 secret 은 복호화 불가가 된다.
            raise click.exceptions.Exit(1)
        return 0

    return _catch(run)


@api.command("deploy")
@click.argument("project_id")
@click.option("--profile", "profile_id", help="Workspace profile id.")
@TimezoneOption
def api_deploy(project_id: str, profile_id: str | None, timezone: str | None) -> int:
    def run() -> int:
        root = _resolve_workspace_project(project_id)
        selected_profile_id, profile_data = select_profile(profile_id)
        register_installed_step_types()
        resolved_api = _resolve_project_api_alias(root, None, profile_data)
        project_name = load_project_context(root).project_id
        payload = _deploy_payload(root, selected_profile_id, profile_data)
        try:
            _report, _latest, rc = _stream_api_report(
                prefix="[api][deploy]",
                project_name=project_name,
                report_name="api-deploy",
                command="z4s api deploy",
                api_alias=resolved_api,
                endpoint="/api/v1/deploy/stream",
                payload=payload,
                timeout=_runtime_http_timeout_seconds("deploy"),
                timezone=timezone,
            )
            return rc
        except ApiReportError as e:
            return _print_api_report_error("[api][deploy]", project_name, "api-deploy", "z4s api deploy", e)

    return _catch(run)


@api.command("undeploy")
@click.argument("project_id")
def api_undeploy(project_id: str) -> int:
    def run() -> int:
        project_name = validate_project_id(project_id)
        resolved_api = _resolve_api_alias(None)
        try:
            response = _post_json(
                resolved_api,
                "/api/v1/undeploy",
                {"project_id": project_name},
                timeout=_runtime_http_timeout_seconds("undeploy"),
            )
        except ApiReportError as e:
            return _print_api_report_error("[api][undeploy]", project_name, "api-undeploy", "z4s api undeploy", e)
        report, latest = _save_api_report(project_name, "api-undeploy", response, "z4s api undeploy")
        return _print_report_summary("[api][undeploy]", report, latest)

    return _catch(run)


@api.command("redeploy")
@click.argument("project_id")
@click.option("--profile", "profile_id", help="Workspace profile id.")
@TimezoneOption
def api_redeploy(project_id: str, profile_id: str | None, timezone: str | None) -> int:
    def run() -> int:
        root = _resolve_workspace_project(project_id)
        selected_profile_id, profile_data = select_profile(profile_id)
        register_installed_step_types()
        resolved_api = _resolve_project_api_alias(root, None, profile_data)
        project_name = load_project_context(root).project_id
        try:
            _report, _latest, rc = _stream_api_report(
                prefix="[api][redeploy]",
                project_name=project_name,
                report_name="api-redeploy",
                command="z4s api redeploy",
                api_alias=resolved_api,
                endpoint="/api/v1/redeploy/stream",
                payload=_deploy_payload(root, selected_profile_id, profile_data),
                timeout=_runtime_http_timeout_seconds("redeploy"),
                timezone=timezone,
            )
            return rc
        except ApiReportError as e:
            return _print_api_report_error("[api][redeploy]", project_name, "api-redeploy", "z4s api redeploy", e)

    return _catch(run)


@api.group("run")
def api_run() -> None:
    """Create and inspect scheduler-projected runs."""


def _resolve_api_project_scope(
    args: tuple[str, ...], *, value_name: str, require_value: bool
) -> tuple[str, str | None]:
    if len(args) > 2:
        raise click.UsageError(f"expected at most 2 arguments: PROJECT {value_name}")
    if len(args) == 2:
        return validate_project_id(args[0]), args[1]
    if len(args) == 1:
        if require_value:
            raise click.UsageError(f"{value_name} is required: pass PROJECT {value_name}")
        return validate_project_id(args[0]), None
    raise click.UsageError("project is required: pass PROJECT" + (f" {value_name}" if require_value else ""))


def _workspace_project_if_available(project_id: str) -> Path | None:
    workspace = workspace_path()
    if workspace is None:
        return None
    root = workspace / "projects" / validate_project_id(project_id)
    return root if project_manifest_path(root) is not None else None


def _check_stale_deployment_if_available(project_id: str, api_alias: str | None, mode: str) -> dict[str, Any]:
    root = _workspace_project_if_available(project_id)
    if root is None:
        if mode == "fail":
            raise ValueError(f"workspace project is required for --stale-deployment fail: {project_id}")
        return {"status": "not_checked", "reason": "workspace_project_not_found"}
    return _check_stale_deployment(root, api_alias, mode)


@api_run.command("create")
@click.argument("args", nargs=-1, metavar="PROJECT JOB_ID")
@click.option("--parameters", "parameters_json", help="JSON object passed to the Step Graph run.")
@click.option(
    "--stale-deployment",
    type=click.Choice(["warn", "fail", "ignore"]),
    default="warn",
    show_default=True,
    help="Compare local bundle artifact_id with registered API artifact before triggering.",
)
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_create(
    args: tuple[str, ...],
    parameters_json: str | None,
    stale_deployment: str,
    api_alias: str | None,
    timezone: str | None,
) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=True)
        resolved_api = _resolve_api_alias(api_alias)
        parameters = json.loads(parameters_json) if parameters_json else {}
        if not isinstance(parameters, dict):
            raise ValueError("--parameters must be a JSON object")
        freshness = _check_stale_deployment_if_available(project_name, resolved_api, stale_deployment)
        response = _post_json(
            resolved_api,
            f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/jobs/{urllib.parse.quote(job_id, safe='')}/runs{_query_string({'timezone': timezone})}",
            {"parameters": parameters},
        )
        response["artifact_freshness"] = freshness
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        return 0

    return _catch(run)


def _project_runs(
    api_alias: str | None, project_name: str, job_id: str | None, timezone: str | None, *, limit: int = 50
) -> list[dict[str, Any]]:
    data = _get_json(
        api_alias,
        f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs{_query_string({'job_id': job_id, 'limit': limit, 'timezone': timezone})}",
    )
    runs = [run for run in data.get("runs") or [] if isinstance(run, dict)]
    runs.sort(key=lambda item: str(item.get("created_at") or item.get("run_id") or ""), reverse=True)
    return runs[:limit]


def _select_project_run(
    api_alias: str | None, project_name: str, job_id: str | None, timezone: str | None
) -> dict[str, Any]:
    runs = _project_runs(api_alias, project_name, job_id, timezone, limit=50)
    if not runs:
        scope = f"project={project_name}" + (f" job_id={job_id}" if job_id else "")
        raise ValueError(f"no runs found for {scope}")
    if len(runs) == 1:
        return runs[0]
    if not sys.stdin.isatty():
        raise ValueError("run_id is required in non-interactive mode when multiple runs match")
    for index, run in enumerate(runs, start=1):
        click.echo(
            f"{index}. {run.get('run_id')} "
            f"job_id={run.get('job_id')} "
            f"source={run.get('source')} "
            f"state={run.get('state')}"
        )
    selected = click.prompt("Select run", type=click.IntRange(1, len(runs)))
    return runs[selected - 1]


def _resolve_project_run_id(
    api_alias: str | None, run_id: str | None, project_name: str, job_id: str | None, timezone: str | None
) -> str:
    if run_id:
        if job_id:
            return run_id
        for run in _project_runs(api_alias, project_name, job_id, timezone, limit=500):
            if run.get("run_id") == run_id:
                return run_id
        raise ValueError(f"run_id not found for project={project_name}: {run_id}")
    selected = _select_project_run(api_alias, project_name, job_id, timezone)
    return str(selected["run_id"])


@api_run.command("list")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--limit", default=30, show_default=True, type=click.IntRange(1, 500))
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_list(args: tuple[str, ...], limit: int, api_alias: str | None, timezone: str | None) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        resolved_api = _resolve_api_alias(api_alias)
        response = {"runs": _project_runs(resolved_api, project_name, job_id, timezone, limit=limit)}
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        return 0

    return _catch(run)


def _api_run_get(
    project_name: str, job_id: str | None, run_id: str | None, api_alias: str | None, timezone: str | None, suffix: str
) -> int:
    resolved_run_id = _resolve_project_run_id(api_alias, run_id, project_name, job_id, timezone)
    path = f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs/{urllib.parse.quote(resolved_run_id, safe='')}"
    if suffix:
        path += f"/{suffix}"
    response = _get_json(api_alias, f"{path}{_query_string({'job_id': job_id, 'timezone': timezone})}")
    click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
    return 0


@api_run.command("status")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_status(args: tuple[str, ...], run_id: str | None, api_alias: str | None, timezone: str | None) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        return _api_run_get(project_name, job_id, run_id, _resolve_api_alias(api_alias), timezone, "")

    return _catch(run)


@api_run.command("tasks")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--watch", is_flag=True, help="Poll task states.")
@click.option("--interval", default=2.0, show_default=True, type=click.FloatRange(min=0.5))
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_tasks(
    args: tuple[str, ...], run_id: str | None, watch: bool, interval: float, api_alias: str | None, timezone: str | None
) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        resolved_api = _resolve_api_alias(api_alias)
        while True:
            rc = _api_run_get(project_name, job_id, run_id, resolved_api, timezone, "tasks")
            if not watch:
                return rc
            time.sleep(interval)

    return _catch(run)


@api_run.command("summary")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_summary(args: tuple[str, ...], run_id: str | None, api_alias: str | None, timezone: str | None) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        resolved_api = _resolve_api_alias(api_alias)
        resolved_run_id = _resolve_project_run_id(resolved_api, run_id, project_name, job_id, timezone)
        path = f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs/{urllib.parse.quote(resolved_run_id, safe='')}/summary"
        response = _get_json(resolved_api, f"{path}{_query_string({'job_id': job_id, 'timezone': timezone})}")
        response.setdefault("command", "z4s api run summary")
        run_state = str(response.get("summary", {}).get("state") or "-")
        response.setdefault("run_state", run_state)
        response.setdefault("status", "failed" if run_state == "failed" else "passed")
        _, latest = _write_report(project_name, "api-run-summary", response, id_field="operation_id", id_prefix="op")
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        try:
            report_ref = latest.relative_to(cli_home())
        except ValueError:
            report_ref = latest
        click.echo(f"path: {report_ref}")
        return 0 if response.get("status") == "passed" else 1

    return _catch(run)


@api_run.command("logs")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--follow", is_flag=True, help="Stream logs.")
@click.option("--tail", type=click.IntRange(1))
@click.option("--interval", default=1.0, show_default=True, type=click.FloatRange(min=0.1))
@click.option("--task", "task_id", help="Show logs for one task_id.")
@click.option("--failed-only", is_flag=True, help="Show failed task logs only.")
@click.option("--latest-attempt-only", is_flag=True, help="Show only the latest attempt log per task.")
@click.option("--output", type=click.Path(path_type=str))
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_logs(
    args: tuple[str, ...],
    run_id: str | None,
    follow: bool,
    tail: int | None,
    interval: float,
    task_id: str | None,
    failed_only: bool,
    latest_attempt_only: bool,
    output: str | None,
    api_alias: str | None,
    timezone: str | None,
) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        resolved_api = _resolve_api_alias(api_alias)
        resolved_run_id = _resolve_project_run_id(resolved_api, run_id, project_name, job_id, timezone)
        suffix = "logs/stream" if follow else "logs"
        query = _query_string(
            {
                "job_id": job_id,
                "tail": tail if tail is not None else (200 if follow else None),
                "interval": interval if follow else None,
                "task_id": task_id,
                "failed_only": failed_only or None,
                "latest_attempt_only": latest_attempt_only or None,
                "timezone": timezone,
            }
        )
        output_path = Path(output) if output else None
        with output_path.open("a", encoding="utf-8") if output_path else nullcontext() as handle:
            if follow:
                with _open_api_stream(
                    resolved_api,
                    f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs/{urllib.parse.quote(resolved_run_id, safe='')}/{suffix}{query}",
                ) as response:
                    while chunk := response.read(8192):
                        text = chunk.decode("utf-8", errors="replace")
                        click.echo(text, nl=False)
                        if handle:
                            handle.write(text)
                return 0
            response = _get_json(
                resolved_api,
                f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs/{urllib.parse.quote(resolved_run_id, safe='')}/{suffix}{query}",
            )
            text = yaml.safe_dump(response, sort_keys=False, allow_unicode=True)
            click.echo(text.rstrip())
            if handle:
                handle.write(text)
            return 0

    return _catch(run)


@api_run.command("artifacts")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_artifacts(args: tuple[str, ...], run_id: str | None, api_alias: str | None, timezone: str | None) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        return _api_run_get(project_name, job_id, run_id, _resolve_api_alias(api_alias), timezone, "artifacts")

    return _catch(run)


@api_run.command("cancel")
@click.argument("args", nargs=-1, metavar="PROJECT [JOB_ID]")
@click.option("--run-id", help="Use a specific canonical run_id.")
@click.option("--api", "api_alias", help="API connection name.")
@TimezoneOption
def api_run_cancel(args: tuple[str, ...], run_id: str | None, api_alias: str | None, timezone: str | None) -> int:
    def run() -> int:
        project_name, job_id = _resolve_api_project_scope(args, value_name="JOB_ID", require_value=False)
        resolved_api = _resolve_api_alias(api_alias)
        resolved_run_id = _resolve_project_run_id(resolved_api, run_id, project_name, job_id, timezone)
        response = _post_json(
            resolved_api,
            f"/api/v1/projects/{urllib.parse.quote(project_name, safe='')}/runs/{urllib.parse.quote(resolved_run_id, safe='')}/cancel{_query_string({'job_id': job_id, 'timezone': timezone})}",
            {},
        )
        click.echo(yaml.safe_dump(response, sort_keys=False, allow_unicode=True).rstrip())
        return 0

    return _catch(run)


@cli.group("report")
def reports() -> None:
    """Manage local JSON reports."""


@reports.command("cleanup")
@click.option("--older-than-days", type=click.IntRange(0), required=True)
@click.option("--project", "project_name")
@click.option("--type", "report_type")
@click.option("--yes", is_flag=True)
def reports_cleanup(older_than_days: int, project_name: str | None, report_type: str | None, yes: bool) -> int:
    def run() -> int:
        root = cli_home() / "reports"
        cutoff = time.time() - older_than_days * 86400
        matched: list[Path] = []
        dirs = [root / project_name] if project_name else sorted(path for path in root.glob("*") if path.is_dir())
        for directory in dirs:
            if not directory.exists():
                continue
            pattern = f"{report_type}.*.json" if report_type else "*.json"
            for path in directory.glob(pattern):
                if path.name.endswith(".latest.json"):
                    continue
                if path.stat().st_mtime < cutoff:
                    matched.append(path)
        if yes:
            for path in matched:
                path.unlink()
        action = "deleted" if yes else "dry-run"
        click.echo(f"[reports][cleanup] {action}: {len(matched)} files matched, {len(matched) if yes else 0} deleted")
        if not yes:
            click.echo("Run with --yes to delete.")
        return 0

    return _catch(run)


def _env_file_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    prefix = f"{key}="
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix) :].strip().strip("\"'")
    return None


def _write_env_file_value(path: Path, key: str, value: str) -> None:
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    prefix = f"{key}="
    replaced = False
    updated: list[str] = []
    for line in lines:
        if line.strip().startswith(prefix):
            updated.append(f"{key}={value}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        if updated and updated[-1].strip():
            updated.append("")
        updated.append(f"{key}={value}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")


def _run_cli(argv: list[str] | None) -> int:
    try:
        result = cli.main(args=argv, prog_name="z4s", standalone_mode=False)
    except click.exceptions.Exit as e:
        return int(e.exit_code or 0)
    except click.ClickException as e:
        e.show(file=sys.stderr)
        return e.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return int(result or 0)


def _silence_stdout() -> None:
    # Python 문서의 SIGPIPE 권장 패턴: 남은 buffer 를 interpreter 종료 시 flush 하다
    # 다시 BrokenPipeError 를 내지 않도록 stdout fd 를 devnull 로 돌린다.
    # signal.SIGPIPE 를 SIG_DFL 로 바꾸는 방식은 Windows 에 SIGPIPE 가 없어 쓰지 않는다.
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.__stdout__.fileno())
        os.close(devnull)
    except (OSError, AttributeError, ValueError):
        pass


def main(argv: list[str] | None = None) -> int:
    try:
        code = _run_cli(argv)
        sys.stdout.flush()
        return code
    except BrokenPipeError:
        # `z4s ... | head -1` 처럼 읽는 쪽이 먼저 닫힌 경우다. 오류를 출력하지 않고
        # Python 이 EPIPE 에서 쓰는 종료 코드 1 로 끝낸다.
        _silence_stdout()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
