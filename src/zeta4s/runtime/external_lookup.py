"""External lookup enrichment runtime."""

from __future__ import annotations

import json
import logging
import re
import base64
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.connections import resolve_runtime_connection
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.task_result import record_success, result_context

logger = logging.getLogger(__name__)

_TABLE_REF_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_][\w$]*)?)", re.IGNORECASE)


@dataclass(frozen=True)
class _HttpConnection:
    base_url: str
    headers: dict[str, str]


class _HttpLookupError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class _ResponseJsonPathError(RuntimeError):
    pass


def _mock_lookup_tables(sql_text: Any) -> str:
    if sql_text is None:
        return "[]"
    refs = []
    seen = set()
    for match in _TABLE_REF_RE.finditer(str(sql_text)):
        ref = match.group(1).lower()
        if ref not in seen:
            refs.append(ref)
            seen.add(ref)
    return json.dumps(refs, ensure_ascii=False)


def _http_connection(conn_id: str, *, connections: dict[str, Any] | None = None) -> _HttpConnection:
    conn = resolve_runtime_connection(conn_id, connections=connections)
    host = conn.host
    if not host:
        raise ValueError(f"http.lookup connection host is empty: conn_id={conn_id!r}")

    if host.startswith(("http://", "https://")):
        base_url = host
    else:
        scheme = getattr(conn, "schema", None) or conn.conn_type or "http"
        if scheme not in {"http", "https"}:
            scheme = "http"
        port = f":{conn.port}" if conn.port else ""
        base_url = f"{scheme}://{host}{port}"

    headers = {"Content-Type": "application/json"}
    extra = conn.extra_dejson or {}
    extra_headers = extra.get("headers")
    if isinstance(extra_headers, dict):
        headers.update({str(key): str(value) for key, value in extra_headers.items()})
    bearer_token = extra.get("bearer_token")
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    elif conn.login:
        password = conn.password or ""
        credentials = f"{conn.login}:{password}".encode("utf-8")
        token = base64.b64encode(credentials).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    return _HttpConnection(base_url=base_url.rstrip("/"), headers=headers)


def _request_json(
    url: str,
    method: str,
    payload: dict[str, Any],
    timeout_seconds: int,
    headers: dict[str, str],
) -> Any:
    body = None
    request_url = url
    if method == "GET":
        request_url = f"{url}?{urllib.parse.urlencode(payload)}"
    elif method == "POST":
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    else:
        raise ValueError(f"http.lookup method is not supported: {method}")
    req = urllib.request.Request(request_url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            response_body = response.read()
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise _HttpLookupError(f"http.lookup request failed: {e.code} {detail}", status=e.code) from e
    except TimeoutError as e:
        raise _HttpLookupError("http.lookup request timed out") from e
    except urllib.error.URLError as e:
        raise _HttpLookupError(f"http.lookup request failed: {e.reason}") from e
    return json.loads(response_body.decode("utf-8"))


def _extract_json_path(payload: Any, path: str) -> Any:
    if path == "$":
        return payload
    value = payload
    for part in path.removeprefix("$.").split("."):
        if not isinstance(value, dict) or part not in value:
            raise _ResponseJsonPathError(f"http.lookup response_json_paths candidate not found: {path}")
        value = value[part]
    return value


def _extract_json_paths(payload: Any, paths: list[str]) -> Any:
    errors = []
    for path in paths:
        try:
            return _extract_json_path(payload, path)
        except _ResponseJsonPathError as e:
            errors.append(str(e))
    raise _ResponseJsonPathError("http.lookup response_json_paths not found: " + "; ".join(errors))


def _resolve_lookup_response(response: Any, http: dict[str, Any], output_column_names: list[str]) -> Any:
    paths = http.get("response_json_paths")
    if paths:
        return _extract_json_paths(response, paths)
    if isinstance(response, dict) and "result" in response:
        return response["result"]
    if len(output_column_names) == 1 and isinstance(response, dict):
        output_column = output_column_names[0]
        if output_column in response:
            return response[output_column]
    return response


def _stringify_lookup_output(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _lookup_http(input_value: Any, http: dict[str, Any], conn: _HttpConnection) -> Any:
    url = conn.base_url + (http.get("path") or "")
    method = http.get("method", "POST")
    if method == "GET":
        payload = dict(http.get("query_params") or {})
        payload[http["request_query_param"]] = input_value
    else:
        payload = {http["request_json_field"]: input_value}
    attempts = int(http.get("retries", 1)) + 1
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            response = _request_json(
                url=url,
                method=method,
                payload=payload,
                timeout_seconds=int(http.get("timeout_seconds", 30)),
                headers=conn.headers,
            )
            return response
        except (_HttpLookupError, json.JSONDecodeError) as e:
            last_error = e
    assert last_error is not None
    raise last_error


def _truncate_log_value(value: Any, limit: int = 500) -> str:
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _log_lookup_failure(
    *,
    name: str,
    source_table: str,
    input_column: str,
    input_value: Any,
    error: Exception,
    attempts: int,
) -> None:
    logger.warning(
        "http.lookup row skipped step=%s source_table=%s input_column=%s input_value=%r "
        "error_type=%s error_message=%r http_status=%s attempts=%d",
        name,
        source_table,
        input_column,
        _truncate_log_value(input_value),
        type(error).__name__,
        _truncate_log_value(error),
        error.status if isinstance(error, _HttpLookupError) else None,
        attempts,
    )


def _normalize_output_columns(output_columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(output_columns, list) or not output_columns:
        raise ValueError("http.lookup.api.response.columns must be a non-empty list")
    normalized = []
    seen = set()
    for column in output_columns:
        if not isinstance(column, dict):
            raise ValueError("http.lookup.api.response.columns[] must be a mapping")
        name = validate_sql_identifier(column.get("name"), "http.lookup.api.response.columns[].name")
        if name in seen:
            raise ValueError(f"http.lookup output column is duplicated: {name}")
        seen.add(name)
        ch_type = str(column.get("type") or "").strip()
        if not ch_type:
            raise ValueError("http.lookup.api.response.columns[].type must be non-empty")
        nullable = bool(column.get("nullable", True))
        normalized.append({"name": name, "type": ch_type, "nullable": nullable})
    return normalized


def _lookup_outputs(value: Any, output_column_names: list[str]) -> tuple:
    if len(output_column_names) == 1:
        return (_stringify_lookup_output(value),)
    if not isinstance(value, dict):
        raise ValueError("http.lookup with multiple output columns requires object response after response_json_paths")
    return tuple(
        _stringify_lookup_output(value[column]) if column in value and value[column] is not None else None
        for column in output_column_names
    )


def run_external_lookup(
    name: str,
    mode: str,
    conn: str,
    source_table: str,
    target_table: str,
    input_column: str,
    output_columns: list[dict[str, Any]],
    concurrency: int = 1,
    batch_size: int = 1_000,
    http: dict[str, Any] | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Run a constrained external lookup enrichment step.

    `mode=mock` is deterministic for tests/demo. `mode=http` performs row-level
    external API lookup and skips API failed rows with log/metrics only.
    """
    if kwargs.get("failure_policy") is not None:
        raise ValueError("http.lookup does not support failure_policy")
    context = _current_context(kwargs)
    with result_context("http.lookup", context) as (started_at, start_monotonic):
        metrics = _run_external_lookup_impl(
            name=name,
            mode=mode,
            conn=conn,
            source_table=source_table,
            target_table=target_table,
            input_column=input_column,
            output_columns=output_columns,
            concurrency=concurrency,
            batch_size=batch_size,
            http=http,
            kwargs=kwargs,
        )
    return record_success(
        stage="http.lookup",
        metrics=metrics,
        details={"name": name, "mode": mode, "conn": conn, "source_table": source_table, "target_table": target_table},
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _run_external_lookup_impl(
    *,
    name: str,
    mode: str,
    conn: str,
    source_table: str,
    target_table: str,
    input_column: str,
    output_columns: list[dict[str, Any]],
    concurrency: int,
    batch_size: int,
    http: dict[str, Any] | None,
    kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = _current_context(kwargs or {})
    validate_sql_identifier(name, "http.lookup.id")
    if not conn:
        raise ValueError("http.lookup requires conn")
    if mode not in {"mock", "http"}:
        raise ValueError("http.lookup mode must be mock or http")
    if mode == "mock" and http:
        raise ValueError("http.lookup mode=mock does not accept api")
    if mode == "http" and not http:
        raise ValueError("http.lookup mode=http requires api")
    source_table = validate_table_identifier(source_table, "http.lookup.source.table", max_parts=2)
    target_table = validate_table_identifier(target_table, "http.lookup.target.table", max_parts=2)
    input_column = validate_sql_identifier(input_column, "http.lookup.lookup.column")
    normalized_outputs = _normalize_output_columns(output_columns)
    output_column_names = [column["name"] for column in normalized_outputs]
    if concurrency <= 0:
        raise ValueError("http.lookup.concurrency must be >= 1")
    if batch_size <= 0:
        raise ValueError("http.lookup.batch_size must be >= 1")
    if http:
        http = dict(http)
        method = http.get("method", "POST")
        if method not in {"GET", "POST"}:
            raise ValueError("http.lookup.api.method must be GET or POST")
        http["method"] = method
        if method == "POST":
            http["request_json_field"] = validate_sql_identifier(
                http.get("request_json_field"),
                "http.lookup.api.request_json_field",
            )
            if http.get("request_query_param"):
                raise ValueError("http.lookup.api.request_query_param is only supported with GET")
        if method == "GET":
            query_param = http.get("request_query_param")
            if not isinstance(query_param, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", query_param):
                raise ValueError("http.lookup.api.request_query_param must be a query parameter name")
            if http.get("request_json_field"):
                raise ValueError("http.lookup.api.request_json_field is only supported with POST")
            query_params = http.get("query_params") or {}
            if not isinstance(query_params, dict):
                raise ValueError("http.lookup.api.query_params must be a mapping")
            for key in query_params:
                if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", key):
                    raise ValueError("http.lookup.api.query_params keys must be query parameter names")
            http["query_params"] = {key: str(value) for key, value in query_params.items()}

    connections = (kwargs or {}).get("connections")
    http_conn = _http_connection(http["conn"], connections=connections) if mode == "http" and http else None
    http_request_success_rows = 0
    http_request_failed_rows = 0
    http_metrics_lock = threading.Lock()

    def enrich_row(row: tuple, input_idx: int) -> tuple[tuple | None, bool]:
        nonlocal http_request_success_rows, http_request_failed_rows
        try:
            if mode == "mock":
                output = _lookup_outputs(_mock_lookup_tables(row[input_idx]), output_column_names)
            else:
                assert http is not None and http_conn is not None
                response = _lookup_http(row[input_idx], http, http_conn)
                with http_metrics_lock:
                    http_request_success_rows += 1
                output = _lookup_outputs(
                    _resolve_lookup_response(response, http, output_column_names),
                    output_column_names,
                )
            return tuple(row) + output, False
        except Exception as e:
            if mode != "http":
                raise
            with http_metrics_lock:
                http_request_failed_rows += 1
            _log_lookup_failure(
                name=name,
                source_table=source_table,
                input_column=input_column,
                input_value=row[input_idx],
                error=e,
                attempts=int(http.get("retries", 1)) + 1 if http else 1,
            )
            return None, True

    def add_http_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        if mode != "http":
            return metrics
        merged = dict(metrics)
        merged["http_request_success_rows"] = http_request_success_rows
        merged["http_request_failed_rows"] = http_request_failed_rows
        return merged

    runtime_connection = resolve_runtime_connection(conn, connections=connections)
    if runtime_connection.conn_type == "clickhouse":
        from zeta4s.runtime.backends.clickhouse.http_lookup import run_clickhouse_http_lookup

        return add_http_metrics(
            run_clickhouse_http_lookup(
                conn_id=conn,
                name=name,
                mode=mode,
                source_table=source_table,
                target_table=target_table,
                input_column=input_column,
                output_columns=normalized_outputs,
                concurrency=concurrency,
                batch_size=batch_size,
                enrich_row=enrich_row,
                context=context,
                http=http,
                connections=connections,
            )
        )
    if runtime_connection.conn_type == "oracle":
        from zeta4s.runtime.backends.oracle.http_lookup import run_oracle_http_lookup

        return add_http_metrics(
            run_oracle_http_lookup(
                conn_id=conn,
                name=name,
                mode=mode,
                source_table=source_table,
                target_table=target_table,
                input_column=input_column,
                output_columns=normalized_outputs,
                concurrency=concurrency,
                batch_size=batch_size,
                enrich_row=enrich_row,
                context=context,
                http=http,
                connections=connections,
            )
        )
    raise NotImplementedError(
        f"http.lookup runtime adapter is not implemented for conn_type={runtime_connection.conn_type!r}"
    )


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs)
