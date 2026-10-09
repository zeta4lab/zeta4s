"""Workspace profile contract loading and validation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from zeta4s.config.cli_config import workspace_path

PROFILE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
CONNECTION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
PROFILE_SUFFIXES = {".yml", ".yaml"}
FORBIDDEN_FIELDS = {
    "conn_id",
    "conn_type",
    "connection",
    "login",
    "password",
    "extra",
    "pools",
    "metastore",
}
TOP_LEVEL_FIELDS = {"scheduler", "connections", "variables", "api_endpoint", "token_env"}
SCHEDULER_BACKENDS = {"airflow", "prefect"}
CONNECTION_FIELDS = {
    "type",
    "host",
    "port",
    "url",
    "username",
    "password_ref",
    "database",
    "schema",
    "options",
}


def _validate_profile_id(profile_id: str) -> str:
    value = str(profile_id).strip()
    if not value:
        raise ValueError("profile id is required")
    path = Path(value)
    if path.is_absolute() or len(path.parts) != 1:
        raise ValueError("profile id must be a single file name without directory separators")
    if path.suffix in PROFILE_SUFFIXES:
        raise ValueError("profile id must not include a file extension")
    if not PROFILE_ID_RE.fullmatch(value):
        raise ValueError(f"invalid profile id: {profile_id}")
    return value


def _validate_connection_id(connection_id: str) -> str:
    value = str(connection_id).strip()
    if not value:
        raise ValueError("connection id is required")
    if not CONNECTION_ID_RE.fullmatch(value):
        raise ValueError(f"invalid connection id: {connection_id}")
    return value


def registered_workspace() -> Path:
    workspace = workspace_path()
    if workspace is None:
        raise ValueError("workspace is not initialized: run `z4s work init`")
    return workspace


def profiles_dir() -> Path:
    return registered_workspace() / "profiles"


def profile_path(profile_id: str) -> Path:
    return profiles_dir() / f"{_validate_profile_id(profile_id)}.yml"


def existing_profile_path(profile_id: str) -> Path:
    normalized = _validate_profile_id(profile_id)
    yml_path = profiles_dir() / f"{normalized}.yml"
    if yml_path.exists():
        return yml_path
    yaml_path = profiles_dir() / f"{normalized}.yaml"
    if yaml_path.exists():
        return yaml_path
    return yml_path


def profile_candidates() -> list[Path]:
    directory = profiles_dir()
    if not directory.exists():
        return []
    return sorted(path for path in directory.iterdir() if path.is_file() and path.suffix in PROFILE_SUFFIXES)


def init_profile(profile_id: str) -> Path:
    path = profile_path(profile_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"profile already exists: {path}")
    path.write_text(
        yaml.safe_dump(
            {"scheduler": "prefect", "connections": {}},
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return path


def read_profile_text(profile_id: str) -> tuple[Path, str]:
    path = existing_profile_path(profile_id)
    if not path.exists():
        raise ValueError(f"profile is not defined: {profile_id}")
    load_profile(profile_id)
    return path, path.read_text(encoding="utf-8")


def load_profile(profile_id: str) -> dict[str, Any]:
    path = existing_profile_path(profile_id)
    if not path.exists():
        raise ValueError(f"profile is not defined: {profile_id}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"profile must be a mapping: {path}")
    validate_profile(data, source=path)
    return data


def select_profile(profile_id: str | None = None) -> tuple[str, dict[str, Any]]:
    if profile_id:
        normalized = _validate_profile_id(profile_id)
        return normalized, load_profile(normalized)
    profiles = list_profiles()
    if not profiles:
        raise ValueError("profile is required: no profiles are defined")
    if len(profiles) > 1:
        ids = ", ".join(str(item["profile_id"]) for item in profiles)
        raise ValueError(f"profile is required: choose one with --profile ({ids})")
    selected = str(profiles[0]["profile_id"])
    return selected, load_profile(selected)


def list_profiles() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for path in profile_candidates():
        result.append(
            {
                "profile_id": path.stem,
                "path": str(path),
            }
        )
    return result


def delete_profile(profile_id: str) -> Path:
    path = existing_profile_path(profile_id)
    if not path.exists():
        raise ValueError(f"profile is not defined: {profile_id}")
    path.unlink()
    return path


def scheduler_backend_from_profile(profile: dict[str, Any]) -> str:
    return profile.get("scheduler", "prefect")


def validate_profile(profile: dict[str, Any], *, source: Path | None = None) -> None:
    label = str(source) if source else "profile"
    unknown_top_level = sorted(set(profile) - TOP_LEVEL_FIELDS)
    if unknown_top_level:
        raise ValueError(f"{label} has unsupported top-level fields: {', '.join(unknown_top_level)}")
    forbidden_top_level = sorted(set(profile) & FORBIDDEN_FIELDS)
    if forbidden_top_level:
        raise ValueError(f"{label} has forbidden fields: {', '.join(forbidden_top_level)}")
    if "scheduler" in profile:
        scheduler_backend = profile["scheduler"]
        if not isinstance(scheduler_backend, str) or scheduler_backend not in SCHEDULER_BACKENDS:
            raise ValueError(f"{label} scheduler must be one of: airflow, prefect")
    if "api_endpoint" in profile and not isinstance(profile["api_endpoint"], str):
        raise ValueError(f"{label} api_endpoint must be a string")
    if "token_env" in profile and not isinstance(profile["token_env"], str):
        raise ValueError(f"{label} token_env must be a string")
    connections = profile.get("connections")
    if not isinstance(connections, dict):
        raise ValueError(f"{label} requires connections mapping")
    variables = profile.get("variables")
    if variables is not None and not isinstance(variables, dict):
        raise ValueError(f"{label} variables must be a mapping")
    for connection_id, connection in connections.items():
        normalized_id = _validate_connection_id(str(connection_id))
        if not isinstance(connection, dict):
            raise ValueError(f"connections[{normalized_id}] must be a mapping")
        forbidden = sorted(set(connection) & FORBIDDEN_FIELDS)
        if forbidden:
            raise ValueError(f"connections[{normalized_id}] has forbidden fields: {', '.join(forbidden)}")
        unknown = sorted(set(connection) - CONNECTION_FIELDS)
        if unknown:
            raise ValueError(f"connections[{normalized_id}] has unsupported fields: {', '.join(unknown)}")
        connection_type = connection.get("type")
        if not isinstance(connection_type, str) or not connection_type.strip():
            raise ValueError(f"connections[{normalized_id}] requires type")
        options = connection.get("options")
        if options is not None and not isinstance(options, dict):
            raise ValueError(f"connections[{normalized_id}].options must be a mapping")
