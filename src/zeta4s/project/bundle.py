"""Project artifact bundling and inspection."""

from __future__ import annotations

import hashlib
import gzip
import io
import tarfile
from pathlib import Path
from typing import Any

import yaml

from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext, load_project_context
from zeta4s.project.paths import posix_path, project_relative_ref
from zeta4s.project.step_graph import (
    StepGraphJob,
    step_graph_config_paths,
    unsupported_config_paths,
    validate_step_graph_config,
    validate_step_graph_config_path,
)

EXCLUDED_DIRS = {"target", "dbt_packages", ".git", "__pycache__"}


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml config must be a mapping: {path}")
    return data


def validate_project_configs(project_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    project = load_project_context(project_root)
    config_items: list[tuple[Path, dict[str, Any]]] = []
    validated_step_graph_items: list[tuple[Path, StepGraphJob]] = []
    for config_path in unsupported_config_paths(project.jobs_dir):
        validate_step_graph_config_path(config_path)
    for config_path in step_graph_config_paths(project.jobs_dir):
        config = _load_yaml(config_path)
        validated = validate_step_graph_config(config_path, config)
        config_items.append((config_path, config))
        validated_step_graph_items.append((config_path, validated))
    if not config_items:
        raise ValueError(f"at least one jobs/*.yml file is required: {project.root}")
    _validate_step_graph_job_dependencies(validated_step_graph_items)
    return config_items


def _validate_step_graph_job_dependencies(step_graph_items: list[tuple[Path, StepGraphJob]]) -> None:
    jobs_by_id: dict[str, tuple[Path, StepGraphJob]] = {}
    errors: list[str] = []

    for config_path, job in step_graph_items:
        previous = jobs_by_id.get(job.job_id)
        if previous is not None:
            previous_path, _ = previous
            errors.append(
                f"{config_path.name}: job_id 가 중복된다: {job.job_id} ({previous_path.name}, {config_path.name})"
            )
            continue
        jobs_by_id[job.job_id] = (config_path, job)

    if errors:
        raise ValueError("; ".join(errors))


def _iter_files(project_root: Path) -> list[Path]:
    files: list[Path] = []
    for path in project_root.rglob("*"):
        if any(part in EXCLUDED_DIRS for part in path.relative_to(project_root).parts):
            continue
        if path.is_file():
            files.append(path)
    return sorted(files)


def build_project_bundle(project_root: Path) -> tuple[bytes, str]:
    project = load_project_context(project_root)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gzip_file:
        with tarfile.open(fileobj=gzip_file, mode="w") as archive:
            for path in _iter_files(project.root):
                arcname = posix_path("projects", project.project_id, project_relative_ref(project.root, path))
                info = archive.gettarinfo(str(path), arcname=str(arcname))
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                with path.open("rb") as handle:
                    archive.addfile(info, handle)
    bundle = buffer.getvalue()
    return bundle, "sha256:" + hashlib.sha256(bundle).hexdigest()


def inspect_project(project_root: Path) -> dict[str, Any]:
    project = load_project_context(project_root)
    dags: list[dict[str, Any]] = []
    job_graphs: list[dict[str, Any]] = []
    config_items = validate_project_configs(project.root)
    for config_path, config in config_items:
        validated = validate_step_graph_config(config_path, config)
        job_id = validated.job_id
        job_graphs.append(_step_graph_job_graph(project, config_path, validated))
        dags.append(
            {
                "dag_id": f"{project.project_id}__{job_id}",
                "job_id": job_id,
                "config": project_relative_ref(project.root, config_path),
                "schedule": config.get("schedule"),
            }
        )
    if not dags:
        raise ValueError(f"at least one jobs/*.yml file is required: {project.root}")
    return {
        "project": project.project_id,
        "project_id": project.project_id,
        "dags": dags,
        "graph": _project_graph_summary(project.project_id, job_graphs),
        "job_graphs": job_graphs,
    }


def _graph_node(node_id: str, node_type: str, label: str, **attrs) -> dict[str, Any]:
    payload = {"id": node_id, "type": node_type, "label": label}
    payload.update({key: value for key, value in attrs.items() if value is not None})
    return payload


def _graph_edge(source: str, target: str, edge_type: str, **attrs) -> dict[str, Any]:
    payload = {"source": source, "target": target, "type": edge_type}
    payload.update({key: value for key, value in attrs.items() if value is not None})
    return payload


def _step_node_id(job_id: str, step_id: str) -> str:
    return f"job:{job_id}:step:{step_id}"


def _job_node_id(job_id: str) -> str:
    return f"job:{job_id}"


def _step_graph_job_graph(project: ProjectContext, config_path: Path, job: StepGraphJob) -> dict[str, Any]:
    plan = build_step_graph_execution_plan(job)
    nodes = [
        _graph_node(
            _step_node_id(job.job_id, node.id),
            "step",
            node.id,
            step_type=node.type,
            conn=node.runtime.conn_id,
            pool=node.runtime.pool,
            join_rule=node.flow.join_rule,
            when=node.flow.when,
            retry=node.flow.retry,
            timeout=node.flow.timeout,
            terminal=node.id in plan.terminal_step_ids,
        )
        for node in plan.steps
    ]
    edges = [
        _graph_edge(
            _step_node_id(job.job_id, edge.upstream_id),
            _step_node_id(job.job_id, edge.downstream_id),
            edge.kind,
        )
        for edge in plan.edges
    ]
    return {
        "project": project.project_id,
        "project_id": project.project_id,
        "job_id": job.job_id,
        "dag_id": f"{project.project_id}__{job.job_id}",
        "config": project_relative_ref(project.root, config_path),
        "schema": "step_graph",
        "schedule": job.schedule,
        "nodes": nodes,
        "edges": edges,
        "job_edges": [],
    }


def _project_graph_summary(project_name: str, job_graphs: list[dict[str, Any]]) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    for graph in job_graphs:
        job_id = _job_node_id(str(graph["job_id"]))
        nodes[job_id] = _graph_node(
            job_id, "job", str(graph["job_id"]), dag_id=graph.get("dag_id"), schema=graph.get("schema")
        )
        for edge in graph.get("job_edges") or []:
            edges.append(_graph_edge(str(edge["source"]), str(edge["target"]), str(edge["type"])))
    return {
        "project_id": project_name,
        "nodes": [nodes[key] for key in sorted(nodes)],
        "edges": edges,
    }
