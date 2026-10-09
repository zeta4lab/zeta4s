"""Elasticsearch native command runtime."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlencode

from zeta4s.common.elasticsearch_index import resolve_elasticsearch_index
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.elasticsearch_client import (
    elasticsearch_connection,
    elasticsearch_json_request,
    elasticsearch_request,
)
from zeta4s.runtime.task_result import log_task_event, record_success, result_context

logger = logging.getLogger(__name__)
DEFAULT_BULK_CHUNK_BYTES = 25 * 1024 * 1024


def run_elasticsearch_command(
    *,
    conn_id: str,
    operation: str,
    project_root: str,
    source: dict[str, Any] | None = None,
    target: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    request: dict[str, Any] | None = None,
    refresh: bool | str | None = None,
    job_id: str | None = None,
    step_id: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    context = _current_context(kwargs)
    source = source or {}
    target = target or {}
    request = request or {}
    with result_context("elasticsearch_command", context) as (started_at, start_monotonic):
        conn = elasticsearch_connection(conn_id, connections=kwargs.get("connections"))
        log_task_event(
            logger,
            "elasticsearch_command.plan",
            context=context,
            job_id=job_id,
            step_id=step_id,
            operation=operation,
        )
        response = _run_operation(
            conn=conn,
            operation=operation,
            project_root=Path(project_root),
            source=source,
            target=target,
            body=body,
            request=request,
            refresh=refresh,
            context=context,
        )
    metrics = _response_metrics(operation, response)
    return record_success(
        stage="elasticsearch_command",
        metrics={
            "input_rows": metrics.get("input_rows"),
            "output_rows": metrics.get("output_rows"),
            "success_rows": metrics.get("success_rows", 1),
            "failed_rows": 0,
            "skipped_rows": 0,
            "error_rows": 0,
        },
        details={
            "job_id": job_id,
            "step_id": step_id,
            "operation": operation,
            "response": _response_summary(response),
        },
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _run_operation(
    *,
    conn,
    operation: str,
    project_root: Path,
    source: dict[str, Any],
    target: dict[str, Any],
    body: dict[str, Any] | None,
    request: dict[str, Any],
    refresh: bool | str | None,
    context: dict[str, Any],
) -> dict[str, Any]:
    if operation == "bulk":
        return _run_bulk(conn, project_root, source, target, refresh, context)
    if operation == "reindex":
        if not isinstance(body, dict):
            raise ValueError("elasticsearch.command operation=reindex requires body")
        return _json_post(conn, "/_reindex", body, refresh=refresh)
    if operation == "update_by_query":
        if not isinstance(body, dict):
            raise ValueError("elasticsearch.command operation=update_by_query requires body")
        index = _target_index(target, context)
        return _json_post(conn, f"/{index}/_update_by_query", body, refresh=refresh)
    if operation == "delete_by_query":
        if not isinstance(body, dict):
            raise ValueError("elasticsearch.command operation=delete_by_query requires body")
        index = _target_index(target, context)
        return _json_post(conn, f"/{index}/_delete_by_query", body, refresh=refresh)
    if operation == "request":
        return _run_request(conn, project_root, request)
    raise ValueError(f"unsupported elasticsearch.command operation: {operation}")


def _run_bulk(
    conn,
    project_root: Path,
    source: dict[str, Any],
    target: dict[str, Any],
    refresh: bool | str | None,
    context: dict[str, Any],
) -> dict[str, Any]:
    if source.get("format") != "ndjson":
        raise ValueError("elasticsearch.command bulk requires source.format=ndjson")
    source_file = source.get("file")
    if not source_file:
        raise ValueError("elasticsearch.command bulk requires source.file")
    payload_path = _project_file_path(project_root, str(source_file))
    if payload_path.stat().st_size > 0 and not _file_ends_with_newline(payload_path):
        raise ValueError("elasticsearch.command bulk NDJSON file must end with newline")
    index = _optional_target_index(target, context)
    if index and target.get("create_index_if_missing"):
        _ensure_index(conn, project_root, target, index)
    path = f"/{index}/_bulk" if index else "/_bulk"
    query = _refresh_query(refresh)
    max_bytes = _bulk_chunk_bytes(source)
    chunks = 0
    items_count = 0
    took = 0
    for payload in _bulk_payloads(payload_path, max_bytes):
        response = elasticsearch_request(
            f"{conn.base_url}{path}{query}",
            payload,
            method="POST",
            content_type="application/x-ndjson",
            headers=conn.headers,
        )
        result = json.loads(response.decode("utf-8")) if response else {}
        chunks += 1
        items = result.get("items") if isinstance(result.get("items"), list) else []
        items_count += len(items)
        took += int(result.get("took") or 0)
        if result.get("errors"):
            failed = [item for item in items if any(action.get("error") for action in item.values())]
            raise RuntimeError(f"Elasticsearch bulk request had item errors: {failed[:3]}")
    return {"errors": False, "items_count": items_count, "chunks": chunks, "took": took}


def _run_request(conn, project_root: Path, request: dict[str, Any]) -> dict[str, Any]:
    method = str(request.get("method") or "").strip().upper()
    path = str(request.get("path") or "")
    if not method:
        raise ValueError("elasticsearch.command request requires method")
    if not path.startswith("/"):
        raise ValueError("elasticsearch.command request.path must start with /")
    body_value = request.get("body")
    body, content_type = _request_body(project_root, body_value)
    response = elasticsearch_request(
        f"{conn.base_url}{path}",
        body,
        method=method,
        content_type=content_type,
        headers=conn.headers,
    )
    return json.loads(response.decode("utf-8")) if response else {}


def _json_post(conn, path: str, body: dict[str, Any], *, refresh: bool | str | None) -> dict[str, Any]:
    query = _refresh_query(refresh)
    response = elasticsearch_json_request(f"{conn.base_url}{path}{query}", body, method="POST", headers=conn.headers)
    _raise_failures(response, path)
    return response


def _target_index(target: dict[str, Any], context: dict[str, Any]) -> str:
    index = _optional_target_index(target, context)
    if not index:
        raise ValueError("elasticsearch.command target requires index or index_template")
    return index


def _optional_target_index(target: dict[str, Any], context: dict[str, Any]) -> str | None:
    if not (target.get("index") or target.get("index_template")):
        return None
    return resolve_elasticsearch_index(
        target.get("index"),
        target.get("index_template"),
        context,
        target.get("index_timezone"),
        index_label="elasticsearch.command.target.index",
        index_template_label="elasticsearch.command.target.index_template",
        index_timezone_label="elasticsearch.command.target.index_timezone",
    )


def _ensure_index(conn, project_root: Path, target: dict[str, Any], index: str) -> None:
    body: dict[str, Any] = {}
    settings = _read_json_ref(project_root, target.get("settings"))
    mappings = _read_json_ref(project_root, target.get("mappings"))
    if settings is not None:
        body["settings"] = settings
    if mappings is not None:
        body["mappings"] = mappings
    if _index_exists(conn.base_url, index, conn.headers):
        return
    elasticsearch_json_request(f"{conn.base_url}/{index}", body, method="PUT", headers=conn.headers)


def _index_exists(base_url: str, index: str, headers: dict[str, str]) -> bool:
    req = urllib.request.Request(f"{base_url}/{index}", method="HEAD", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Elasticsearch index check failed: {exc.code} {detail}") from exc


def _read_json_ref(project_root: Path, ref: Any) -> dict[str, Any] | None:
    if ref is None:
        return None
    if isinstance(ref, dict):
        return ref
    data = _read_project_file(project_root, str(ref))
    return json.loads(data.decode("utf-8"))


def _request_body(project_root: Path, body_value: Any) -> tuple[bytes | None, str]:
    if body_value is None:
        return None, "application/json"
    if isinstance(body_value, dict):
        return json.dumps(body_value, ensure_ascii=False).encode("utf-8"), "application/json"
    path = str(body_value)
    payload = _read_project_file(project_root, path)
    content_type = "application/x-ndjson" if path.endswith(".ndjson") else "application/json"
    return payload, content_type


def _read_project_file(project_root: Path, ref: str) -> bytes:
    return _project_file_path(project_root, ref).read_bytes()


def _project_file_path(project_root: Path, ref: str) -> Path:
    root = project_root.resolve()
    path = (root / ref).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"elasticsearch.command file must stay inside project root: {ref}") from exc
    return path


def _file_ends_with_newline(path: Path) -> bool:
    with path.open("rb") as file:
        file.seek(-1, 2)
        return file.read(1) == b"\n"


def _bulk_chunk_bytes(source: dict[str, Any]) -> int:
    value = int(source.get("batch_bytes") or DEFAULT_BULK_CHUNK_BYTES)
    if value <= 0:
        raise ValueError("elasticsearch.command bulk source.batch_bytes must be greater than zero")
    return value


def _bulk_payloads(path: Path, max_bytes: int) -> Iterator[bytes]:
    payload = bytearray()
    for operation in _bulk_operations(path):
        if len(operation) > max_bytes:
            raise ValueError("elasticsearch.command bulk operation exceeds source.batch_bytes")
        if payload and len(payload) + len(operation) > max_bytes:
            yield bytes(payload)
            payload.clear()
        payload.extend(operation)
    if payload:
        yield bytes(payload)


def _bulk_operations(path: Path) -> Iterator[bytes]:
    with path.open("rb") as file:
        while True:
            action_line = file.readline()
            if not action_line:
                break
            if action_line == b"\n":
                continue
            action = _bulk_action(action_line)
            operation = bytearray(action_line)
            if action != "delete":
                source_line = file.readline()
                if not source_line:
                    raise ValueError("elasticsearch.command bulk operation is missing source line")
                if source_line == b"\n":
                    raise ValueError("elasticsearch.command bulk source line must not be blank")
                operation.extend(source_line)
            yield bytes(operation)


def _bulk_action(line: bytes) -> str:
    try:
        metadata = json.loads(line.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("elasticsearch.command bulk action line must be JSON") from exc
    if not isinstance(metadata, dict) or len(metadata) != 1:
        raise ValueError("elasticsearch.command bulk action line must contain one action")
    action = next(iter(metadata))
    if action not in {"create", "delete", "index", "update"}:
        raise ValueError(f"unsupported elasticsearch.command bulk action: {action}")
    return action


def _refresh_query(refresh: bool | str | None) -> str:
    if refresh is None or refresh is False:
        return ""
    value = "true" if refresh is True else str(refresh)
    return "?" + urlencode({"refresh": value})


def _raise_failures(response: dict[str, Any], path: str) -> None:
    failures = response.get("failures") or []
    if failures:
        raise RuntimeError(f"Elasticsearch command failed for {path}: {failures[:3]}")


def _response_metrics(operation: str, response: dict[str, Any]) -> dict[str, Any]:
    if operation == "bulk":
        if response.get("items_count") is not None:
            rows = int(response.get("items_count") or 0)
            return {"input_rows": rows, "output_rows": rows, "success_rows": rows}
        items = response.get("items") if isinstance(response.get("items"), list) else []
        return {"input_rows": len(items), "output_rows": len(items), "success_rows": len(items)}
    total = int(response.get("total") or 0)
    changed = int(response.get("created") or 0) + int(response.get("updated") or 0) + int(response.get("deleted") or 0)
    output_rows = changed or total or None
    return {"input_rows": total or None, "output_rows": output_rows, "success_rows": 1}


def _response_summary(response: dict[str, Any]) -> dict[str, Any]:
    keep = (
        "took",
        "errors",
        "total",
        "created",
        "updated",
        "deleted",
        "batches",
        "chunks",
        "items_count",
        "version_conflicts",
        "failures",
    )
    return {key: response[key] for key in keep if key in response}


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs)
