"""Shared Elasticsearch HTTP helpers for source extract and write."""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from zeta4s.runtime.connections import resolve_runtime_connection


@dataclass(frozen=True)
class ElasticsearchConnection:
    base_url: str
    headers: dict[str, str]


def elasticsearch_connection(conn_id: str, *, connections: dict[str, Any] | None = None) -> ElasticsearchConnection:
    conn = resolve_runtime_connection(conn_id, connections=connections)
    host = conn.host
    if not host:
        raise ValueError(f"Elasticsearch connection host 가 비어 있다: conn_id={conn_id!r}")

    if host.startswith(("http://", "https://")):
        base_url = host
    else:
        scheme = getattr(conn, "schema", None) or conn.conn_type or "http"
        if scheme not in {"http", "https"}:
            scheme = "http"
        port = f":{conn.port}" if conn.port else ""
        base_url = f"{scheme}://{host}{port}"

    headers = {}
    if conn.login:
        password = conn.password or ""
        credentials = f"{conn.login}:{password}".encode("utf-8")
        token = base64.b64encode(credentials).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    return ElasticsearchConnection(base_url=base_url.rstrip("/"), headers=headers)


def elasticsearch_request(
    url: str,
    body: bytes | None = None,
    method: str = "POST",
    content_type: str = "application/json",
    headers: dict[str, str] | None = None,
    timeout: int = 60,
) -> bytes:
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": content_type, **(headers or {})},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Elasticsearch request failed: {e.code} {detail}") from e


def elasticsearch_json_request(
    url: str,
    body: dict[str, Any] | None = None,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    response = elasticsearch_request(url, payload, method=method, headers=headers, timeout=timeout)
    if not response:
        return {}
    return json.loads(response.decode("utf-8"))
