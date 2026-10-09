"""zeta4s CLI local state configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

CLI_HOME_DIRS = ("secrets", "cache", "reports")
DEFAULT_WORKSPACE_NAME = "zeta4s-work"


def cli_home() -> Path:
    configured = os.environ.get("ZETA4S_CLI_HOME")
    if configured and configured.strip():
        return Path(configured).expanduser()
    return Path.home() / ".zeta4s"


def config_path() -> Path:
    return cli_home() / "config.yml"


def ensure_cli_home() -> Path:
    home = cli_home()
    home.mkdir(parents=True, exist_ok=True)
    for name in CLI_HOME_DIRS:
        (home / name).mkdir(parents=True, exist_ok=True)
    path = config_path()
    if not path.exists():
        path.write_text(yaml.safe_dump({"apis": {}}, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return home


def load_config(path: Path | None = None) -> dict[str, Any]:
    resolved = path or config_path()
    if not resolved.exists():
        return {"apis": {}, "workspaces": {}}
    data = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"z4s CLI config file must be a mapping: {resolved}")
    if "workspace" in data:
        raise ValueError(
            "Legacy config format detected. Please remove ~/.zeta4s/config.yml and run `z4s work init` again."
        )
    data.setdefault("apis", {})
    data.setdefault("workspaces", {})
    return data


def save_config(config: dict[str, Any], path: Path | None = None) -> Path:
    resolved = path or config_path()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return resolved


def workspace_path(config: dict[str, Any] | None = None) -> Path | None:
    data = config if config is not None else load_config()
    active = data.get("active_workspace")
    if not active:
        return None
    workspaces = data.get("workspaces", {})
    if active not in workspaces:
        return None
    return Path(str(workspaces[active])).expanduser()


def set_workspace(name: str, path: Path) -> Path:
    resolved = path.expanduser().resolve()
    data = load_config()
    workspaces = data.setdefault("workspaces", {})
    workspaces[name] = str(resolved)
    data["active_workspace"] = name
    return save_config(data)


def use_workspace(name: str) -> Path:
    data = load_config()
    workspaces = data.get("workspaces", {})
    if name not in workspaces:
        raise ValueError(f"z4s workspace is not defined: {name}")
    data["active_workspace"] = name
    return save_config(data)


def list_workspaces() -> dict[str, Any]:
    data = load_config()
    return {
        "workspaces": data.get("workspaces", {}),
        "active_workspace": data.get("active_workspace"),
    }


def workspace_info(config: dict[str, Any] | None = None) -> dict[str, Any]:
    data = config if config is not None else load_config()
    home = cli_home()
    workspace = workspace_path(data)
    profiles_dir = workspace / "profiles" if workspace else None
    projects_dir = workspace / "projects" if workspace else None
    profile_count = 0
    project_count = 0
    if profiles_dir and profiles_dir.exists():
        profile_count = len(
            [path for path in profiles_dir.iterdir() if path.is_file() and path.suffix in {".yml", ".yaml"}]
        )
    if projects_dir and projects_dir.exists():
        project_count = len([path for path in projects_dir.iterdir() if path.is_dir()])
    return {
        "home": str(home),
        "active_workspace_name": data.get("active_workspace"),
        "workspace": str(workspace) if workspace else None,
        "exists": bool(workspace and workspace.exists()),
        "profiles_dir": str(profiles_dir) if profiles_dir else None,
        "projects_dir": str(projects_dir) if projects_dir else None,
        "profile_count": profile_count,
        "project_count": project_count,
    }


def set_api(
    alias: str,
    url: str,
    token_env: str | None = None,
    token_file: Path | None = None,
    *,
    default: bool = False,
) -> Path:
    data = load_config()
    apis = data.setdefault("apis", {})
    apis[alias] = {"url": url.rstrip("/")}
    if token_env:
        apis[alias]["token_env"] = token_env
    if token_file:
        apis[alias]["token_file"] = str(token_file.expanduser().resolve())
    if default or not data.get("default_api"):
        data["default_api"] = alias
    return save_config(data)


def use_api(alias: str) -> Path:
    data = load_config()
    if alias not in data.get("apis", {}):
        raise ValueError(f"API connection name is not defined: {alias}")
    data["default_api"] = alias
    return save_config(data)


def remove_api(alias: str) -> Path:
    data = load_config()
    apis = data.get("apis") or {}
    if alias not in apis:
        raise ValueError(f"API connection name is not defined: {alias}")
    apis.pop(alias)
    if data.get("default_api") == alias:
        data.pop("default_api", None)
    return save_config(data)


def resolve_api(alias_or_dict: str | dict[str, str] | None = None) -> dict[str, str]:
    if isinstance(alias_or_dict, dict):
        return alias_or_dict
    alias = alias_or_dict
    data = load_config()
    apis = data.get("apis") or {}
    selected = alias or data.get("default_api")
    if not selected:
        raise ValueError("API connection name is required: use --api or `z4s api use <name>`")
    if selected not in apis:
        raise ValueError(f"API connection name is not defined: {selected}")
    api = dict(apis[selected])
    api["alias"] = selected
    token_env = api.get("token_env")
    if token_env:
        token = os.environ.get(token_env)
        if token:
            api["token"] = token
    token_file = api.get("token_file")
    if token_file:
        path = Path(str(token_file)).expanduser()
        if path.exists():
            token = path.read_text(encoding="utf-8").strip()
            if token:
                api["token"] = token
    return api
