"""Static dbt model contract validation for zeta4s projects."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import yaml


_CONFIG_RE = re.compile(r"\{\{\s*config\s*\((.*?)\)\s*\}\}", re.DOTALL)
_MATERIALIZED_RE = re.compile(r"\bmaterialized\s*=\s*(['\"])(?P<value>[^'\"]+)\1")


@dataclass(frozen=True)
class DbtModelContractResult:
    checked: int
    materialized: int


def validate_dbt_model_contract(
    dbt_project_dir: Path,
    *,
    run_models: Iterable[str],
    test_models: Iterable[str] = (),
) -> DbtModelContractResult:
    """Validate selected dbt models without invoking dbt or touching files."""
    run_model_names = _unique_model_names(run_models)
    test_model_names = _unique_model_names(test_models)
    all_model_names = sorted(set(run_model_names) | set(test_model_names))
    if not all_model_names:
        return DbtModelContractResult(checked=0, materialized=0)
    project = _load_dbt_project(dbt_project_dir)
    _validate_dbt_project_identity(dbt_project_dir, project)
    _validate_dbt_project_models_scope(dbt_project_dir, project)
    models_dir = dbt_project_dir / "models"
    properties = _load_model_properties(models_dir)
    checked = 0
    materialized = 0
    for model_name in all_model_names:
        model_path = _model_sql_path(models_dir, model_name)
        checked += 1
        if model_name not in run_model_names:
            continue
        state = _resolved_materialization(
            dbt_project_dir=dbt_project_dir,
            project=project,
            model_name=model_name,
            model_path=model_path,
            properties=properties,
        )
        if state != "table":
            raise ValueError(
                "dbt model materialization must be table: "
                f"conn={dbt_project_dir.name} model={model_name} materialized={state or 'missing'}"
            )
        materialized += 1
    return DbtModelContractResult(checked=checked, materialized=materialized)


def _unique_model_names(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        model = str(value).strip()
        if not model or model in seen:
            continue
        if "/" in model or "\\" in model or model.endswith(".sql"):
            raise ValueError(f"dbt model name must be a plain model id: {model}")
        seen.add(model)
        result.append(model)
    return result


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml must be a mapping: {path}")
    return data


def _load_dbt_project(dbt_project_dir: Path) -> dict[str, Any]:
    path = dbt_project_dir / "dbt_project.yml"
    if not path.exists():
        raise ValueError(f"dbt_project.yml is required: {path}")
    return _load_yaml(path)


def _validate_dbt_project_identity(dbt_project_dir: Path, project: dict[str, Any]) -> None:
    conn_id = dbt_project_dir.name
    for key in ("name", "profile"):
        value = str(project.get(key) or "").strip()
        if value != conn_id:
            raise ValueError(f"dbt_project.yml {key} must match conn id: conn={conn_id} {key}={value or 'missing'}")


def _validate_dbt_project_models_scope(dbt_project_dir: Path, project: dict[str, Any]) -> None:
    models_config = project.get("models")
    if not isinstance(models_config, dict):
        return
    project_name = str(project.get("name") or dbt_project_dir.name).strip()
    unsupported = sorted(key for key in models_config if key != project_name)
    if unsupported:
        raise ValueError(
            "dbt_project.yml models config must be scoped under project name: "
            f"conn={dbt_project_dir.name} project={project_name} unsupported={', '.join(map(str, unsupported))}"
        )


def _load_model_properties(models_dir: Path) -> dict[str, dict[str, Any]]:
    properties: dict[str, dict[str, Any]] = {}
    if not models_dir.exists():
        return properties
    for path in sorted([*models_dir.rglob("*.yml"), *models_dir.rglob("*.yaml")]):
        data = _load_yaml(path)
        models = data.get("models")
        if not isinstance(models, list):
            continue
        for item in models:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str) or not name.strip():
                continue
            properties[name.strip()] = item
    return properties


def _model_sql_path(models_dir: Path, model_name: str) -> Path:
    if not models_dir.exists():
        raise ValueError(f"dbt models directory is required: {models_dir}")
    matches = sorted(path for path in models_dir.rglob(f"{model_name}.sql") if path.is_file())
    if not matches:
        raise ValueError(f"dbt model SQL file is required: {models_dir}/{model_name}.sql")
    if len(matches) > 1:
        refs = ", ".join(str(path) for path in matches)
        raise ValueError(f"dbt model name is ambiguous: {model_name} ({refs})")
    return matches[0]


def _resolved_materialization(
    *,
    dbt_project_dir: Path,
    project: dict[str, Any],
    model_name: str,
    model_path: Path,
    properties: dict[str, dict[str, Any]],
) -> str | None:
    sql_state = _sql_materialization(model_path.read_text(encoding="utf-8"))
    if sql_state:
        return sql_state
    yaml_state = _property_materialization(properties.get(model_name) or {})
    if yaml_state:
        return yaml_state
    return _project_materialization(dbt_project_dir, project, model_path)


def _sql_materialization(text: str) -> str | None:
    for match in _CONFIG_RE.finditer(text):
        materialized = _MATERIALIZED_RE.search(match.group(1))
        if materialized:
            return materialized.group("value").strip().lower()
    return None


def _property_materialization(model_property: dict[str, Any]) -> str | None:
    config = model_property.get("config")
    if not isinstance(config, dict):
        return None
    value = config.get("materialized", config.get("+materialized"))
    return str(value).strip().lower() if value is not None and str(value).strip() else None


def _project_materialization(
    dbt_project_dir: Path,
    project: dict[str, Any],
    model_path: Path,
) -> str | None:
    models_config = project.get("models")
    if not isinstance(models_config, dict):
        return None
    project_name = str(project.get("name") or dbt_project_dir.name).strip()
    project_config = models_config.get(project_name)
    if not isinstance(project_config, dict):
        return None
    relative_parts = _model_relative_parts(dbt_project_dir, model_path)
    chain = _config_chain_for_model(project_config, relative_parts)
    return _materialized_from_config_chain(chain)


def _model_relative_parts(dbt_project_dir: Path, model_path: Path) -> list[str]:
    relative = model_path.relative_to(dbt_project_dir / "models")
    posix = PurePosixPath(relative.as_posix())
    return [*posix.parts[:-1], posix.stem]


def _config_chain_for_model(config: dict[str, Any], relative_parts: list[str]) -> list[dict[str, Any]]:
    current: Any = config
    chain: list[dict[str, Any]] = [config]
    for part in relative_parts:
        if not isinstance(current, dict) or part not in current:
            break
        current = current[part]
        if isinstance(current, dict):
            chain.append(current)
        else:
            break
    return chain


def _materialized_from_config_chain(chain: list[dict[str, Any]]) -> str | None:
    resolved: str | None = None
    for item in chain:
        value = item.get("+materialized")
        if value is None:
            config = item.get("config")
            if isinstance(config, dict):
                value = config.get("materialized", config.get("+materialized"))
        if value is not None and str(value).strip():
            resolved = str(value).strip().lower()
    return resolved
