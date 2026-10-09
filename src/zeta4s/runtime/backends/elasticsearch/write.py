"""Elasticsearch rowset write backend."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
import urllib.error
import urllib.request
from typing import Any

from zeta4s.common.elasticsearch_index import resolve_elasticsearch_index
from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_sql_identifier_list
from zeta4s.runtime.elasticsearch_client import (
    elasticsearch_connection,
    elasticsearch_json_request,
    elasticsearch_request,
)
from zeta4s.runtime.rowsets import ResolvedRowsetRef
from zeta4s.runtime.task_result import log_task_event

logger = logging.getLogger(__name__)
DEFAULT_BULK_BATCH_SIZE = 1_000


def write_elasticsearch_rowset(
    *,
    rowset: ResolvedRowsetRef,
    target_conn: str,
    mode: str,
    columns: list[str],
    key: list[str] | None,
    options: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    options = dict(options or {})
    after = options.pop("_rowset_after", None)
    on_checkpoint = options.pop("_rowset_on_checkpoint", None)
    mode = str(mode or "").strip()
    if mode not in {"replace", "append", "upsert"}:
        raise ValueError("elasticsearch.write mode must be replace, append or upsert")
    columns = [validate_sql_identifier(column, "elasticsearch.write.columns[]") for column in columns]
    document_id = normalize_document_id(options.get("document_id"), key=key)
    if mode == "upsert" and document_id["mode"] != "columns":
        raise ValueError("elasticsearch.write mode=upsert requires document_id columns")

    schema = rowset.schema
    if len(schema) == 0:
        raise ValueError(f"rowset has no schema: {rowset.source_ref}")
    missing = [column for column in columns if column not in schema.names]
    if missing:
        raise ValueError(f"elasticsearch.write columns not found in rowset: {missing}")
    missing_key = [column for column in document_id["columns"] if column not in columns]
    if missing_key:
        raise ValueError(f"elasticsearch.write document_id columns must be included in columns: {missing_key}")

    batch_size = int(options.get("bulk_batch_size") or options.get("batch_size") or DEFAULT_BULK_BATCH_SIZE)
    if batch_size <= 0:
        raise ValueError("elasticsearch.write batch_size must be greater than zero")
    refresh = options.get("refresh")
    index = resolve_elasticsearch_index(
        options.get("index"),
        options.get("index_template"),
        context or {},
        options.get("index_timezone"),
        index_label="elasticsearch.write.target.index",
        index_template_label="elasticsearch.write.target.index_template",
        index_timezone_label="elasticsearch.write.target.index_timezone",
    )
    conn = elasticsearch_connection(target_conn, connections=connections)
    ensure_index(conn, index, create_if_missing=bool(options.get("create_index_if_missing")))
    if mode == "replace":
        delete_all_documents(conn, index)

    loaded = 0
    batches = 0
    took = 0
    for item in rowset.iter_positioned_batches(batch_size=batch_size, columns=columns, after=after):
        batch = item.batch
        if not batch.num_rows:
            continue
        payload = bulk_payload(batch, columns, mode=mode, document_id=document_id)
        response = post_bulk(conn, index, payload, refresh=refresh)
        loaded += int(batch.num_rows)
        batches += 1
        took += int(response.get("took") or 0)
        log_task_event(
            logger,
            "write.elasticsearch.progress",
            context=context,
            target=index,
            mode=mode,
            loaded_rows=loaded,
            batches=batches,
        )
        if on_checkpoint is not None:
            on_checkpoint(loaded, batches, item.continuation)
    return {
        "target": index,
        "input_rows": loaded,
        "output_rows": loaded,
        "success_rows": loaded,
        "failed_rows": 0,
        "skipped_rows": 0,
        "batches": batches,
        "took": took,
    }


def normalize_document_id(document_id: Any, *, key: list[str] | None) -> dict[str, Any]:
    if document_id is None and key:
        return {"mode": "columns", "columns": validate_sql_identifier_list(key, "elasticsearch.write.key")}
    if isinstance(document_id, str):
        return {"mode": "columns", "columns": [validate_sql_identifier(document_id, "elasticsearch.write.document_id")]}
    if document_id is None:
        return {"mode": "auto", "columns": []}
    if not isinstance(document_id, dict):
        raise ValueError("elasticsearch.write document_id must be a string or mapping")
    mode = str(document_id.get("mode") or "columns").strip()
    if mode not in {"columns", "auto"}:
        raise ValueError("elasticsearch.write document_id.mode must be columns or auto")
    if mode == "auto":
        if document_id.get("columns"):
            raise ValueError("elasticsearch.write document_id.mode=auto cannot include columns")
        return {"mode": "auto", "columns": []}
    columns = document_id.get("columns")
    if isinstance(columns, str):
        columns = [columns]
    return {
        "mode": "columns",
        "columns": validate_sql_identifier_list(columns or [], "elasticsearch.write.document_id.columns"),
    }


def ensure_index(conn, index: str, *, create_if_missing: bool) -> None:
    if index_exists(conn.base_url, index, conn.headers):
        return
    if not create_if_missing:
        raise RuntimeError(
            f"Elasticsearch index does not exist: {index}. Set create_index_if_missing=true for dev/demo auto-create."
        )
    elasticsearch_json_request(f"{conn.base_url}/{index}", {}, method="PUT", headers=conn.headers)


def delete_all_documents(conn, index: str) -> int:
    response = elasticsearch_json_request(
        f"{conn.base_url}/{index}/_delete_by_query?conflicts=proceed&refresh=true",
        {"query": {"match_all": {}}},
        method="POST",
        headers=conn.headers,
    )
    failures = response.get("failures") if isinstance(response.get("failures"), list) else []
    if failures:
        raise RuntimeError(f"Elasticsearch delete_by_query had failures: {failures[:3]}")
    return int(response.get("deleted") or 0)


def index_exists(base_url: str, index: str, headers: dict[str, str]) -> bool:
    req = urllib.request.Request(f"{base_url}/{index}", method="HEAD", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30):
            return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Elasticsearch index check failed: {exc.code} {detail}") from exc


def post_bulk(conn, index: str, payload: bytes, *, refresh: bool | str | None) -> dict[str, Any]:
    query = refresh_query(refresh)
    response = elasticsearch_request(
        f"{conn.base_url}/{index}/_bulk{query}",
        payload,
        method="POST",
        content_type="application/x-ndjson",
        headers=conn.headers,
    )
    result = json.loads(response.decode("utf-8")) if response else {}
    if result.get("errors"):
        items = result.get("items") if isinstance(result.get("items"), list) else []
        failed = [item for item in items if any(action.get("error") for action in item.values())]
        raise RuntimeError(f"Elasticsearch bulk request had item errors: {failed[:3]}")
    return result


def bulk_payload(batch, columns: list[str], *, mode: str, document_id: dict[str, Any]) -> bytes:
    table = batch.to_pydict()
    lines: list[str] = []
    for index in range(batch.num_rows):
        document = {column: elasticsearch_value(table[column][index]) for column in columns}
        action = "create" if mode == "append" else "index" if mode == "replace" else "update"
        metadata: dict[str, Any] = {}
        doc_id = document_id_value(document, document_id)
        if doc_id is not None:
            metadata["_id"] = doc_id
        lines.append(json.dumps({action: metadata}, ensure_ascii=False, separators=(",", ":")))
        if action == "update":
            lines.append(
                json.dumps(
                    {"doc": document, "doc_as_upsert": True},
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=json_default,
                )
            )
        else:
            lines.append(json.dumps(document, ensure_ascii=False, separators=(",", ":"), default=json_default))
    return ("\n".join(lines) + "\n").encode("utf-8")


def document_id_value(document: dict[str, Any], document_id: dict[str, Any]) -> str | None:
    if document_id["mode"] == "auto":
        return None
    values = []
    for column in document_id["columns"]:
        value = document.get(column)
        if value is None:
            raise ValueError(f"elasticsearch.write document_id column contains null values: {column}")
        values.append(str(value))
    if len(values) == 1:
        return values[0]
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def elasticsearch_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return elasticsearch_value(value)
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def refresh_query(refresh: bool | str | None) -> str:
    if refresh is None or refresh is False:
        return ""
    if refresh is True:
        return "?refresh=true"
    value = str(refresh).strip()
    return f"?refresh={value}" if value else ""
