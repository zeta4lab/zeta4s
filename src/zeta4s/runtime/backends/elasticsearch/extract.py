"""Elasticsearch rowset extract backend."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from zeta4s.common.elasticsearch_index import resolve_elasticsearch_index
from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.elasticsearch_client import elasticsearch_connection
from zeta4s.runtime.elasticsearch_client import elasticsearch_json_request
from zeta4s.runtime.project_metadata import EPOCH
from zeta4s.runtime.rowset_models import ResumeCapability
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch
from zeta4s.runtime.step_state import get_step_watermark

logger = logging.getLogger(__name__)

PIT_KEEP_ALIVE = "1m"


def _parse_interval(interval_str: str) -> timedelta:
    parts = interval_str.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid interval: {interval_str!r}")
    value = int(parts[0])
    unit = parts[1].upper().rstrip("S")
    if unit == "MINUTE":
        return timedelta(minutes=value)
    if unit == "SECOND":
        return timedelta(seconds=value)
    raise ValueError(f"Unknown interval unit: {unit}")


def _normalize_time_window(time_window: dict[str, Any] | None) -> dict[str, Any] | None:
    if not time_window:
        return None
    if not isinstance(time_window, dict):
        raise ValueError("extract.time_window must be a mapping")
    column = validate_sql_identifier(time_window.get("column"), "extract.time_window.column")
    lookback_window = time_window.get("lookback_window")
    if not isinstance(lookback_window, str) or not lookback_window.strip():
        raise ValueError("extract.time_window.lookback must be a non-empty string")
    upper_bound = time_window.get("upper_bound", "task_started_at")
    if upper_bound not in {"data_interval_end", "task_started_at"}:
        raise ValueError("extract.time_window.upper_bound must be data_interval_end or task_started_at")
    return {"column": column, "lookback_window": lookback_window, "upper_bound": upper_bound}


def _resolve_time_window_bounds(
    time_window: dict[str, Any], loaded_at: datetime, context: dict[str, Any]
) -> tuple[datetime, datetime]:
    if time_window["upper_bound"] == "data_interval_end" and context.get("data_interval_end") is not None:
        window_end = _as_naive_datetime(context["data_interval_end"], "data_interval_end")
    else:
        window_end = _as_naive_datetime(context.get("task_started_at", loaded_at), "task_started_at")
    return window_end - _parse_interval(time_window["lookback_window"]), window_end


def _as_naive_datetime(value, context: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{context} must be a datetime")
    if value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


_ROWSET_LOGICAL_TYPES = {
    "bool": "boolean",
    "int": "integer",
    "float": "float",
    "decimal": "decimal",
    "str": "string",
    "date": "date",
    "timestamp": "timestamp",
}


def _column_specs(columns: list[dict[str, Any]]) -> list[ColumnSpec]:
    specs: list[ColumnSpec] = []
    for column in columns:
        name = validate_sql_identifier(column["name"], "elasticsearch.extract.target.columns[].name")
        specs.append(_column_spec_from_field(name, column))
    if not specs:
        raise ValueError("elasticsearch.extract.target.columns must not be empty")
    return specs


def _column_spec_from_field(name: str, field: dict[str, Any]) -> ColumnSpec:
    raw_type = str(field.get("type") or "str").strip().lower()
    logical_type = _ROWSET_LOGICAL_TYPES.get(raw_type)
    if logical_type is None:
        raise ValueError(f"elasticsearch.extract.source.fields[].type must be a rowset type: {field.get('type')!r}")
    precision = _optional_int(field.get("precision"))
    scale = _optional_int(field.get("scale"))
    datetime_precision = _optional_int(field.get("datetime_precision"))
    if logical_type == "timestamp" and datetime_precision is None:
        datetime_precision = 6
    type_name = _rowset_type_name(logical_type, precision=precision, scale=scale, datetime_precision=datetime_precision)
    return ColumnSpec(
        name=name,
        type=type_name,
        nullable=bool(field.get("nullable", True)),
        logical_type=logical_type,
        precision=precision,
        scale=scale,
        datetime_precision=datetime_precision,
        source_backend="elasticsearch",
        source_type=raw_type,
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _rowset_type_name(
    logical_type: str,
    *,
    precision: int | None,
    scale: int | None,
    datetime_precision: int | None,
) -> str:
    if logical_type == "boolean":
        return "bool"
    if logical_type == "integer":
        return "int"
    if logical_type == "float":
        return "float"
    if logical_type == "decimal":
        return f"decimal({precision or 38},{scale or 0})"
    if logical_type == "string":
        return "str"
    if logical_type == "date":
        return "date"
    if logical_type == "timestamp":
        return f"timestamp({6 if datetime_precision is None else datetime_precision})"
    raise ValueError(f"unsupported rowset logical type: {logical_type}")


def _source_value(source: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = source
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _coerce_datetime(value: Any, column: str) -> Any:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError as exc:
            raise ValueError(
                f"Elasticsearch source datetime value is invalid: column={column}, value={value!r}"
            ) from exc
    raise ValueError(f"Elasticsearch source datetime value must be string/datetime: column={column}")


def _coerce_scalar(value: Any, column: str, spec: ColumnSpec) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        raise ValueError(
            "elasticsearch.extract.source.fields[].mode=scalar cannot handle object/list values: "
            f"column={column}. Use mode=json_string."
        )
    if spec.logical_type == "timestamp":
        return _coerce_datetime(value, column)
    if spec.logical_type == "date":
        return value
    if spec.logical_type == "integer":
        return int(value)
    if spec.logical_type == "float":
        return float(value)
    if spec.logical_type == "decimal":
        from decimal import Decimal

        return Decimal(str(value))
    if spec.logical_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
        raise ValueError(f"Elasticsearch source boolean value is invalid: column={column}, value={value!r}")
    return value


def _project_row(
    source: dict[str, Any],
    fields_by_column: dict[str, dict[str, Any]],
    column_order: list[str],
    column_specs_by_name: dict[str, ColumnSpec],
) -> tuple:
    values = []
    for column in column_order:
        field = fields_by_column[column]
        path = field["path"]
        mode = field.get("mode") or "scalar"
        found, value = _source_value(source, path)
        if (not found or value is None) and field.get("required"):
            raise ValueError(f"Elasticsearch source required path is missing/null: column={column}, path={path}")
        if mode == "json_string":
            value = None if value is None else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        elif mode == "scalar":
            value = _coerce_scalar(value, column, column_specs_by_name[column])
        else:
            raise ValueError(f"unsupported elasticsearch.extract.source.fields[].mode: {mode}")
        values.append(value)
    return tuple(values)


def _merge_query_filter(query: dict[str, Any], filters: list[dict[str, Any]]) -> dict[str, Any]:
    if not filters:
        return query
    if query == {"match_all": {}}:
        return {"bool": {"filter": filters}}
    return {"bool": {"filter": filters + [query]}}


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


def _watermark_filters(
    *,
    source_wm_path: str | None,
    time_window: dict[str, Any] | None,
    full_refresh: bool,
    loaded_at: datetime,
    overlap_window: str | None,
    raw_table_name: str,
    job_id: str,
    step_id: str | None,
    watermark_column: str | None,
    kwargs: dict[str, Any],
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if time_window:
        window_start, window_end = _resolve_time_window_bounds(time_window, loaded_at, kwargs)
        if not source_wm_path:
            raise ValueError("Elasticsearch time_window requires a fields[] projection for watermark.column")
        filters.append({"range": {source_wm_path: {"gte": _iso(window_start), "lt": _iso(window_end)}}})
        return filters
    if full_refresh or not source_wm_path:
        return filters
    last_wm = get_step_watermark(
        project_id=kwargs.get("project_id"),
        job_id=job_id,
        step_id=step_id,
        output_name=raw_table_name,
        watermark_column=watermark_column,
    )
    if last_wm == EPOCH:
        return filters
    overlap = _parse_interval(overlap_window or "0 SECOND")
    select_from = last_wm - overlap
    upper = loaded_at - overlap
    filters.append({"range": {source_wm_path: {"gt": _iso(select_from), "lte": _iso(upper)}}})
    return filters


def _sort_has_tie_breaker(sort: list[Any]) -> bool:
    for item in sort:
        if isinstance(item, str) and item in {"_shard_doc", "_doc"}:
            return True
        if isinstance(item, dict) and any(key in {"_shard_doc", "_doc"} for key in item):
            return True
    return False


def _search_sort(sort: list[Any] | None, source_wm_path: str | None) -> list[Any]:
    if sort:
        normalized = list(sort)
        if not _sort_has_tie_breaker(normalized):
            normalized.append({"_shard_doc": "asc"})
        return normalized
    if source_wm_path:
        return [{source_wm_path: "asc"}, {"_shard_doc": "asc"}]
    return [{"_shard_doc": "asc"}]


class ElasticsearchSearchReader:
    """Elasticsearch PIT/search_after reader that yields bounded row batches."""

    source_kind = "elasticsearch"
    resume_capability = ResumeCapability.EXACT

    def __init__(
        self,
        *,
        conn,
        index: str,
        fields: list[dict[str, Any]],
        raw_columns: list[dict[str, Any]],
        fields_by_column: dict[str, dict[str, Any]],
        query: dict[str, Any],
        sort: list[Any],
        batch_size: int,
        track_total_hits: bool,
        request_timeout_seconds: int = 60,
    ) -> None:
        self.conn = conn
        self.index = index
        self.source_object = index
        self.fields = fields
        self.raw_columns = raw_columns
        self.fields_by_column = fields_by_column
        self.query = query
        self.sort = sort
        self.batch_size = batch_size
        self.track_total_hits = track_total_hits
        self.request_timeout_seconds = request_timeout_seconds
        self.column_specs = _column_specs(raw_columns)
        self.column_specs_by_name = {spec.name: spec for spec in self.column_specs}
        self.columns = [spec.name for spec in self.column_specs]
        self._pit_id: str | None = None

    def __enter__(self) -> "ElasticsearchSearchReader":
        self._pit_id = _open_pit(
            self.conn.base_url,
            self.index,
            self.conn.headers,
            timeout=self.request_timeout_seconds,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._pit_id:
            _close_pit(
                self.conn.base_url,
                self._pit_id,
                self.conn.headers,
                timeout=self.request_timeout_seconds,
            )
        self._pit_id = None

    def read_batches(self, *, after: dict[str, Any] | None = None):
        if not self._pit_id:
            raise RuntimeError("ElasticsearchSearchReader must be opened before reading")
        search_after = None
        if after is not None:
            pit_id = after.get("pit_id")
            search_after = after.get("search_after")
            if not isinstance(pit_id, str) or not pit_id or not isinstance(search_after, list) or not search_after:
                raise ValueError("Elasticsearch continuation requires pit_id and non-empty search_after")
        while True:
            body: dict[str, Any] = {
                "pit": {"id": self._pit_id, "keep_alive": PIT_KEEP_ALIVE},
                "size": self.batch_size,
                "query": self.query,
                "sort": self.sort,
                "_source": [field["path"].split(".", 1)[0] for field in self.fields],
                "track_total_hits": self.track_total_hits,
            }
            if search_after is not None:
                body["search_after"] = search_after
            payload = elasticsearch_json_request(
                f"{self.conn.base_url}/_search",
                body=body,
                method="POST",
                headers=self.conn.headers,
                timeout=self.request_timeout_seconds,
            )
            self._pit_id = str(payload.get("pit_id") or self._pit_id)
            hits = (payload.get("hits") or {}).get("hits") or []
            if not hits:
                break
            rows = [
                _project_row(
                    hit.get("_source") or {},
                    self.fields_by_column,
                    self.columns,
                    self.column_specs_by_name,
                )
                for hit in hits
            ]
            search_after = hits[-1].get("sort")
            if not isinstance(search_after, list) or not search_after:
                raise RuntimeError("Elasticsearch exact reader batch did not include a search_after token")
            yield SourceBatch(
                rows=rows,
                continuation={"pit_id": self._pit_id, "search_after": search_after},
            )


def _open_pit(base_url: str, index: str, headers: dict[str, str], *, timeout: int = 60) -> str:
    payload = elasticsearch_json_request(
        f"{base_url}/{index}/_pit?keep_alive={PIT_KEEP_ALIVE}",
        method="POST",
        headers=headers,
        timeout=timeout,
    )
    pit_id = payload.get("id")
    if not pit_id:
        raise RuntimeError("Elasticsearch PIT open response did not include id")
    return str(pit_id)


def _close_pit(base_url: str, pit_id: str, headers: dict[str, str], *, timeout: int = 30) -> None:
    try:
        elasticsearch_json_request(
            f"{base_url}/_pit",
            body={"id": pit_id},
            method="DELETE",
            headers=headers,
            timeout=timeout,
        )
    except Exception:
        logger.warning("Failed to close Elasticsearch PIT", exc_info=True)


def open_elasticsearch_rowset_reader(
    *,
    source_conn: str,
    source: dict[str, Any],
    watermark: dict[str, Any] | None,
    time_window: dict[str, Any] | None,
    batch_size: int | None,
    loaded_at: datetime,
    metadata_name: str,
    job_id: str | None,
    step_id: str | None,
    output_name: str,
    context: dict[str, Any],
    kwargs: dict[str, Any],
) -> ElasticsearchSearchReader:
    conn = elasticsearch_connection(source_conn, connections=kwargs.get("connections"))
    index = resolve_elasticsearch_index(
        source.get("index"),
        source.get("index_template"),
        context,
        source.get("index_timezone"),
        index_label="elasticsearch.extract.source.index",
        index_template_label="elasticsearch.extract.source.index_template",
        index_timezone_label="elasticsearch.extract.source.index_timezone",
    )
    fields, raw_columns = _normalize_fields(source.get("fields") or [])
    fields_by_column = {field["column"]: field for field in fields}
    watermark_column = _watermark_column(watermark, time_window)
    source_wm_path = fields_by_column.get(watermark_column, {}).get("path") if watermark_column else None
    filters = _watermark_filters(
        source_wm_path=source_wm_path,
        time_window=_canonical_time_window(time_window),
        full_refresh=not watermark,
        loaded_at=loaded_at,
        overlap_window=_short_interval_to_words(str((watermark or {}).get("overlap_window") or "0s")),
        raw_table_name=output_name,
        job_id=job_id,
        step_id=step_id,
        watermark_column=watermark_column,
        kwargs=kwargs,
    )
    query = _merge_query_filter(source.get("query") or {"match_all": {}}, filters)
    sort = _search_sort(source.get("sort"), source_wm_path)
    request_timeout_seconds = int(source.get("request_timeout_seconds") or 60)
    if request_timeout_seconds <= 0:
        raise ValueError("elasticsearch.extract.source.request_timeout_seconds must be greater than zero")
    return ElasticsearchSearchReader(
        conn=conn,
        index=index,
        fields=fields,
        raw_columns=raw_columns,
        fields_by_column=fields_by_column,
        query=query,
        sort=sort,
        batch_size=batch_size or int(source.get("batch_size") or 1_000),
        track_total_hits=bool(source.get("track_total_hits")),
        request_timeout_seconds=request_timeout_seconds,
    )


def _normalize_fields(fields: list[Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_fields = []
    raw_columns = []
    for item in fields:
        if isinstance(item, str):
            name = validate_sql_identifier(item, "elasticsearch.extract.source.fields[]")
            normalized_fields.append({"column": name, "path": name, "mode": "scalar"})
            raw_columns.append({"name": name, "type": "str"})
            continue
        if not isinstance(item, dict):
            raise ValueError("elasticsearch.extract.source.fields[] must be string or mapping")
        column = validate_sql_identifier(
            str(item.get("column") or item.get("name")), "elasticsearch.extract.source.fields[].column"
        )
        path = str(item.get("path") or column)
        normalized_fields.append({**item, "column": column, "path": path})
        raw_columns.append(
            {
                "name": column,
                "type": item.get("type") or "str",
                "nullable": item.get("nullable", True),
                "precision": item.get("precision"),
                "scale": item.get("scale"),
                "datetime_precision": item.get("datetime_precision"),
            }
        )
    if not normalized_fields:
        raise ValueError("elasticsearch.extract.source.fields[] is required")
    return normalized_fields, raw_columns


def _watermark_column(watermark: dict[str, Any] | None, time_window: dict[str, Any] | None) -> str | None:
    if watermark:
        return validate_sql_identifier(str(watermark.get("column")), "extract.watermark.column")
    if time_window:
        return validate_sql_identifier(str(time_window.get("column")), "extract.time_window.column")
    return None


def _canonical_time_window(time_window: dict[str, Any] | None) -> dict[str, Any] | None:
    if not time_window:
        return None
    normalized = dict(time_window)
    if "lookback" in normalized and "lookback_window" not in normalized:
        normalized["lookback_window"] = _short_interval_to_words(str(normalized.pop("lookback")))
    elif "lookback_window" in normalized:
        normalized["lookback_window"] = _short_interval_to_words(str(normalized["lookback_window"]))
    normalized.setdefault("upper_bound", "task_started_at")
    return _normalize_time_window(normalized)


def _short_interval_to_words(value: str) -> str:
    text = value.strip()
    match = re.fullmatch(r"(\d+)([sm])", text)
    if not match:
        return text
    amount, unit = match.groups()
    return f"{amount} {'SECOND' if unit == 's' else 'MINUTE'}"
