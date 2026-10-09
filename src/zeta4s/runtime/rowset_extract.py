"""Rowset-producing extract runtime."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.project.extract_sql import load_extract_sql
from zeta4s.runtime.backends.clickhouse import ClickHouseSelectReader
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.project_metadata import EPOCH, ExtractHistoryEvent, record_extract_history
from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetIdentity
from zeta4s.runtime.rowset_models import ResumeCapability
from zeta4s.runtime.rowset_store import CheckpointPolicy, rowset_descriptor_payload
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch, SourceReader
from zeta4s.runtime.step_state import get_step_watermark, set_step_watermark
from zeta4s.runtime.task_result import log_task_event, record_success, result_context
from zeta4s.runtime.types import (
    arrow_type_from_clickhouse_type,
    arrow_type_from_column_spec,
)

logger = logging.getLogger(__name__)
DEFAULT_FETCH_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class RowsetWriteResult:
    descriptor: RowsetDescriptor
    new_watermark: Any | None = None

    @property
    def rows(self) -> int:
        return self.descriptor.rows

    @property
    def bytes(self) -> int:
        return self.descriptor.bytes

    @property
    def uri(self) -> str:
        return self.descriptor.uri

    @property
    def columns(self) -> tuple[str, ...]:
        return self.descriptor.columns

    @property
    def column_specs(self) -> tuple[ColumnSpec, ...]:
        return self.descriptor.column_specs


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs)


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


def _required_runtime_identity(
    *,
    project_id: Any,
    job_id: str | None,
    step_id: str | None,
) -> tuple[str, str, str]:
    missing = [
        name
        for name, value in (
            ("project_id", project_id),
            ("job_id", job_id),
            ("step_id", step_id),
        )
        if value is None or not str(value).strip()
    ]
    if missing:
        raise ValueError(f"rowset extract requires runtime identity: {', '.join(missing)}")
    return str(project_id).strip(), str(job_id).strip(), str(step_id).strip()


def run_extract_rowset(
    *,
    source_conn: str,
    source: dict[str, Any],
    output: dict[str, Any],
    source_type: str,
    project_root: str,
    job_id: str | None = None,
    step_id: str | None = None,
    params: dict[str, Any] | None = None,
    watermark: dict[str, Any] | None = None,
    time_window: dict[str, Any] | None = None,
    batch_size: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    context = _current_context(kwargs)
    params = dict(params or {})
    output_name = _single_rowset_output_name(output)
    with result_context("extract", context) as (started_at, start_monotonic):
        result = _run_extract_rowset_impl(
            source_conn=source_conn,
            source=source,
            output_name=output_name,
            source_type=source_type,
            project_root=project_root,
            job_id=job_id,
            step_id=step_id,
            params=params,
            watermark=watermark,
            time_window=time_window,
            batch_size=batch_size,
            context=context,
            kwargs=kwargs,
        )
    metrics = {
        "input_rows": result.rows,
        "output_rows": result.rows,
        "success_rows": result.rows,
        "failed_rows": 0,
        "skipped_rows": 0,
        "error_rows": 0,
        "rowset_bytes": result.bytes,
        "new_watermark": _json_value(result.new_watermark),
    }
    output_payload = rowset_descriptor_payload(result.descriptor)
    details = {
        "source": _source_object(source_type, source),
        "source_type": source_type,
        "outputs": {
            output_name: output_payload,
        },
    }
    return record_success(
        stage="extract",
        metrics=metrics,
        details=details,
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _run_extract_rowset_impl(
    *,
    source_conn: str,
    source: dict[str, Any],
    output_name: str,
    source_type: str,
    project_root: str,
    job_id: str | None,
    step_id: str | None,
    params: dict[str, Any],
    watermark: dict[str, Any] | None,
    time_window: dict[str, Any] | None,
    batch_size: int | None,
    context: dict[str, Any],
    kwargs: dict[str, Any],
) -> RowsetWriteResult:
    if watermark and time_window:
        raise ValueError("extract cannot use watermark and time_window together")
    source_kind = str(source.get("kind") or "")
    loaded_at = datetime.now()
    run_id = _run_id(context) or f"{job_id or step_id or 'extract'}__{loaded_at.isoformat()}"
    task_id = _task_id(context) or step_id or output_name
    metadata_name = job_id or step_id or output_name
    project_id = kwargs.get("project_id")
    project_id, job_id, step_id = _required_runtime_identity(project_id=project_id, job_id=job_id, step_id=step_id)
    watermark_column = _watermark_column(watermark, time_window)
    selected_from = None
    selected_to = None
    history_started_at = datetime.now()
    source_object = _source_object(source_type, source)
    record_extract_history(
        ExtractHistoryEvent(
            project_id=project_id,
            job_id=job_id,
            run_id=run_id,
            step_id=step_id,
            task_id=task_id,
            output_name=output_name,
            source_kind=source_type,
            source_conn=source_conn,
            source_object=source_object,
            mode="rowset",
            watermark_column=watermark_column,
            selected_from=None,
            selected_to=None,
            loaded_rows=0,
            status="running",
            error_message=None,
            started_at=history_started_at,
            ended_at=None,
        ),
    )
    try:
        with _open_reader(
            source_conn=source_conn,
            source=source,
            source_type=source_type,
            source_kind=source_kind,
            project_root=project_root,
            params=params,
            watermark=watermark,
            time_window=time_window,
            batch_size=batch_size,
            loaded_at=loaded_at,
            metadata_name=metadata_name,
            job_id=job_id,
            step_id=step_id,
            output_name=output_name,
            context=context,
            kwargs=kwargs,
        ) as reader:
            store = kwargs.get("rowset_store") or ParquetRowsetStore(_runtime_rowset_root(kwargs))
            result = write_reader_to_rowset(
                reader=reader,
                store=store,
                identity=RowsetIdentity(
                    project_id=project_id,
                    job_id=job_id,
                    run_id=run_id,
                    step_id=step_id,
                    attempt=int(context.get("attempt") or 1),
                    output_name=output_name,
                ),
                watermark_column=watermark_column,
                context=context,
                checkpoint_repository=kwargs.get("step_checkpoint_repository"),
            )
        if time_window:
            selected_from, selected_to = _resolve_time_window_bounds(
                _canonical_time_window(time_window), loaded_at, context
            )
        if watermark and result.new_watermark is not None:
            set_step_watermark(
                project_id=project_id,
                job_id=job_id,
                step_id=step_id,
                output_name=output_name,
                watermark_column=watermark_column,
                watermark_value=result.new_watermark,
                run_id=run_id,
            )
        record_extract_history(
            ExtractHistoryEvent(
                project_id=project_id,
                job_id=job_id,
                run_id=run_id,
                step_id=step_id,
                task_id=task_id,
                output_name=output_name,
                source_kind=source_type,
                source_conn=source_conn,
                source_object=source_object,
                mode="rowset",
                watermark_column=watermark_column,
                selected_from=selected_from,
                selected_to=selected_to,
                loaded_rows=result.rows,
                status="success",
                error_message=None,
                started_at=history_started_at,
                ended_at=datetime.now(),
            ),
        )
        return result
    except Exception as exc:
        record_extract_history(
            ExtractHistoryEvent(
                project_id=project_id,
                job_id=job_id,
                run_id=run_id,
                step_id=step_id,
                task_id=task_id,
                output_name=output_name,
                source_kind=source_type,
                source_conn=source_conn,
                source_object=source_object,
                mode="rowset",
                watermark_column=watermark_column,
                selected_from=selected_from,
                selected_to=selected_to,
                loaded_rows=0,
                status="failed",
                error_message=str(exc)[:1000],
                started_at=history_started_at,
                ended_at=datetime.now(),
            ),
        )
        raise


def write_reader_to_rowset(
    *,
    reader: SourceReader,
    store,
    identity: RowsetIdentity,
    watermark_column: str | None = None,
    context: dict[str, Any] | None = None,
    checkpoint_repository=None,
    checkpoint_policy: CheckpointPolicy | None = None,
    clock=time.monotonic,
) -> RowsetWriteResult:
    import pyarrow as pa

    columns = list(reader.columns)
    writer_column_specs: list[ColumnSpec] = []
    session = None
    rows = 0
    new_watermark = None
    continuation = None
    uncommitted_bytes = 0
    last_checkpoint_at = clock()
    policy = checkpoint_policy or CheckpointPolicy.from_environment()
    after = None
    resume_capability = getattr(reader, "resume_capability", ResumeCapability.RESTART_ONLY)
    if checkpoint_repository is not None and resume_capability is ResumeCapability.EXACT:
        from zeta4s.runtime.checkpoints import load_verified_checkpoint

        checkpoint = load_verified_checkpoint(identity, checkpoint_repository, store)
        if checkpoint is not None:
            descriptor = store.descriptor_for_checkpoint(checkpoint)
            session = store.resume(descriptor, identity=identity)
            schema = session.schema
            writer_column_specs = list(descriptor.column_specs)
            rows = descriptor.rows
            after = checkpoint.continuation
    try:
        batches = reader.read_batches(after=after) if after is not None else reader.read_batches()
        for batch in batches:
            if not columns:
                columns = list(reader.columns)
                if not columns and batch.column_values is not None:
                    columns = list(batch.column_values)
                if not columns and batch.arrow_table is not None:
                    columns = [str(name) for name in batch.arrow_table.schema.names]
            if not columns:
                raise ValueError(f"extract source produced a batch without columns: {reader.source_object}")
            if session is None:
                writer_column_specs = [ColumnSpec.from_value(spec) for spec in reader.column_specs]
                if writer_column_specs:
                    schema = _writer_schema(None, writer_column_specs)
                    table = _batch_to_arrow_table(batch, columns, schema=schema)
                else:
                    table = _batch_to_arrow_table(batch, columns)
                    schema = _writer_schema(table.schema, writer_column_specs)
                if resume_capability is ResumeCapability.RESTART_ONLY and hasattr(store, "restart"):
                    session = store.restart(identity, schema=schema)
                else:
                    session = store.begin(identity, schema=schema)
            else:
                table = _batch_to_arrow_table(batch, columns, schema=schema)
            table = table.cast(schema)
            session.append(table)
            rows += table.num_rows
            uncommitted_bytes += table.nbytes
            continuation = batch.continuation
            if resume_capability is ResumeCapability.EXACT and batch.row_count and not continuation:
                raise ValueError("exact source reader emitted a non-empty batch without continuation")
            new_watermark = _max_watermark(batch, columns, watermark_column, new_watermark)
            log_task_event(
                logger,
                "extract.rowset.progress",
                context=context,
                rowset=identity.output_name,
                loaded_rows=rows,
            )
            if (
                checkpoint_repository is not None
                and continuation is not None
                and (
                    uncommitted_bytes >= policy.target_bytes
                    or clock() - last_checkpoint_at >= policy.max_interval_seconds
                )
            ):
                from zeta4s.runtime.checkpoints import commit_step_checkpoint

                commit_step_checkpoint(session, continuation, checkpoint_repository)
                uncommitted_bytes = 0
                last_checkpoint_at = clock()
        if session is None:
            if not reader.column_specs:
                raise ValueError(f"extract source produced no rows and no schema: {reader.source_object}")
            writer_column_specs = [ColumnSpec.from_value(spec) for spec in reader.column_specs]
            if not columns:
                columns = [spec.name for spec in writer_column_specs]
            schema = _schema_with_column_specs(_arrow_schema(writer_column_specs), writer_column_specs)
            if resume_capability is ResumeCapability.RESTART_ONLY and hasattr(store, "restart"):
                session = store.restart(identity, schema=schema)
            else:
                session = store.begin(identity, schema=schema)
            session.append(pa.Table.from_pydict({field.name: [] for field in schema}, schema=schema))
        if checkpoint_repository is not None and continuation is not None and uncommitted_bytes:
            from zeta4s.runtime.checkpoints import commit_step_checkpoint

            commit_step_checkpoint(session, continuation, checkpoint_repository)
        descriptor = session.finish()
    except Exception:
        if session is not None:
            session.abort()
        raise
    return RowsetWriteResult(
        descriptor=descriptor,
        new_watermark=new_watermark,
    )


def _open_reader(
    *,
    source_conn: str,
    source: dict[str, Any],
    source_type: str,
    source_kind: str,
    project_root: str,
    params: dict[str, Any],
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
):
    if source_type in {"oracle", "clickhouse"}:
        query, source_object, query_params = _select_query(
            source=source,
            source_type=source_type,
            source_kind=source_kind,
            project_root=project_root,
            params=params,
            watermark=watermark,
            time_window=time_window,
            loaded_at=loaded_at,
            metadata_name=metadata_name,
            job_id=job_id,
            step_id=step_id,
            output_name=output_name,
            context=context,
            kwargs=kwargs,
        )
        if source_type == "oracle":
            from zeta4s.runtime.backends.oracle.extract import OracleSelectReader, normalize_lob_policy

            return OracleSelectReader(
                source_conn=source_conn,
                source_object=source_object,
                query=query,
                params=query_params,
                fetch_batch_size=batch_size or DEFAULT_FETCH_BATCH_SIZE,
                arraysize=None,
                prefetchrows=None,
                lob_policy=normalize_lob_policy(source.get("lob_policy")),
                connections=kwargs.get("connections"),
            )
        return ClickHouseSelectReader(
            source_conn=source_conn,
            source_object=source_object,
            query=query,
            params=query_params,
            batch_size=batch_size or DEFAULT_FETCH_BATCH_SIZE,
            connections=kwargs.get("connections"),
        )
    if source_type == "elasticsearch":
        from zeta4s.runtime.backends.elasticsearch import open_elasticsearch_rowset_reader

        return open_elasticsearch_rowset_reader(
            source_conn=source_conn,
            source=source,
            watermark=watermark,
            time_window=time_window,
            batch_size=batch_size,
            loaded_at=loaded_at,
            metadata_name=metadata_name,
            job_id=job_id,
            step_id=step_id,
            output_name=output_name,
            context=context,
            kwargs=kwargs,
        )
    raise ValueError(f"unsupported extract source_type: {source_type}")


def _select_query(
    *,
    source: dict[str, Any],
    source_type: str,
    source_kind: str,
    project_root: str,
    params: dict[str, Any],
    watermark: dict[str, Any] | None,
    time_window: dict[str, Any] | None,
    loaded_at: datetime,
    metadata_name: str,
    job_id: str | None,
    step_id: str | None,
    output_name: str,
    context: dict[str, Any],
    kwargs: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    query_params = dict(params)
    if source_kind == "query":
        if time_window:
            raise ValueError("extract source.kind=query uses params for fixed windows, not time_window")
        query = load_extract_sql(project_root, str(source["query"]))
        source_object = str(source["query"])
    elif source_kind == "table":
        table = validate_table_identifier(str(source["table"]), f"{source_type}.extract.source.table", max_parts=2)
        query = f"SELECT * FROM {table}"
        source_object = table
    else:
        raise ValueError(f"{source_type}.extract source.kind must be table or query")
    predicates = []
    if source.get("where_clause"):
        predicates.append(
            f"({_validate_where_clause(str(source['where_clause']), f'{source_type}.extract.source.where_clause')})"
        )
    watermark_column = _watermark_column(watermark, time_window)
    if time_window:
        normalized = _canonical_time_window(time_window)
        window_start, window_end = _resolve_time_window_bounds(normalized, loaded_at, context)
        column = validate_sql_identifier(normalized["column"], "extract.time_window.column")
        predicates.extend([f"{column} >= :window_start", f"{column} < :window_end"])
        query_params.update({"window_start": window_start, "window_end": window_end})
    elif watermark:
        column = validate_sql_identifier(watermark_column, "extract.watermark.column")
        last_wm = get_step_watermark(
            project_id=kwargs.get("project_id"),
            job_id=job_id,
            step_id=step_id,
            output_name=output_name,
            watermark_column=column,
        )
        overlap = _parse_interval(_short_interval_to_words(str(watermark.get("overlap_window") or "0s")))
        select_from = last_wm - overlap if last_wm > EPOCH else EPOCH
        cur_wm = loaded_at - overlap
        if source_kind == "table":
            predicates.extend([f"{column} > :select_from", f"{column} <= :cur_wm"])
        query_params.update({"select_from": select_from, "cur_wm": cur_wm})
    if predicates:
        if source_kind == "query":
            raise ValueError("extract source.kind=query must define selection predicates inside the SQL file")
        query += " WHERE " + " AND ".join(predicates)
    if watermark_column and source_kind == "table":
        query += f" ORDER BY {validate_sql_identifier(watermark_column, 'extract.watermark.column')}"
    return query, source_object, query_params


def _single_rowset_output_name(output: dict[str, Any]) -> str:
    if not isinstance(output, dict) or len(output) != 1:
        raise ValueError("extract output must define exactly one rowset output")
    return validate_sql_identifier(next(iter(output)), "extract.output name")


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


def _validate_where_clause(value: str, label: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{label} must be non-empty")
    if re.search(r";|--|/\*|\*/|\b(select|union)\b", text, re.IGNORECASE):
        raise ValueError(f"{label} must be a single predicate without comments, subqueries, or unions")
    return text


def _batch_to_arrow_table(batch: SourceBatch, columns: list[str], schema=None):
    import pyarrow as pa

    if batch.arrow_table is not None:
        table = batch.arrow_table.select(columns)
        return table.cast(schema) if schema is not None else table
    if batch.column_values is not None:
        values = {column: list(batch.column_values.get(column, [])) for column in columns}
        _coerce_column_values(values, schema)
        return pa.table(values, schema=schema)
    assert batch.rows is not None
    values = {column: [row[index] for row in batch.rows] for index, column in enumerate(columns)}
    _coerce_column_values(values, schema)
    return pa.table(values, schema=schema)


def _coerce_column_values(values: dict[str, list], schema) -> None:
    if schema is None:
        return
    import pyarrow as pa

    for field in schema:
        column_values = values.get(field.name)
        if column_values is None:
            continue
        if pa.types.is_decimal(field.type):
            values[field.name] = [
                None if value is None else value if isinstance(value, Decimal) else Decimal(str(value))
                for value in column_values
            ]
        elif pa.types.is_date(field.type):
            values[field.name] = [_coerce_date_value(value) for value in column_values]
        elif pa.types.is_timestamp(field.type):
            values[field.name] = [_coerce_timestamp_value(value) for value in column_values]


def _coerce_date_value(value):
    if value is None or isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text)
        except ValueError:
            return _coerce_timestamp_value(text).date()
    return value


def _coerce_timestamp_value(value):
    if value is None or isinstance(value, datetime):
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    return value


def _arrow_schema(column_specs: list[ColumnSpec]):
    import pyarrow as pa

    specs = [ColumnSpec.from_value(spec) for spec in column_specs]
    return pa.schema([pa.field(spec.name, _arrow_type_from_spec(spec), nullable=spec.nullable) for spec in specs])


def _writer_schema(inferred_schema, column_specs: list[ColumnSpec]):
    if column_specs:
        return _schema_with_column_specs(_arrow_schema(column_specs), column_specs)
    if inferred_schema is None:
        raise ValueError("rowset writer requires inferred schema or column_specs")
    return _schema_with_column_specs(inferred_schema, column_specs)


def _schema_with_column_specs(schema, column_specs: list[ColumnSpec]):
    return schema.with_metadata(
        {
            **(schema.metadata or {}),
            ROWSET_COLUMN_SPECS_METADATA_KEY: json.dumps(_json_column_specs(column_specs), sort_keys=True).encode(
                "utf-8"
            ),
        }
    )


def _json_column_specs(column_specs: tuple[ColumnSpec, ...] | list[ColumnSpec]) -> list[dict[str, Any]]:
    return [ColumnSpec.from_value(spec).to_json() for spec in column_specs]


def _arrow_type_from_spec(spec: ColumnSpec):
    return arrow_type_from_column_spec(spec)


def _arrow_type(ch_type: str):
    return arrow_type_from_clickhouse_type(ch_type)


def _max_watermark(batch: SourceBatch, columns: list[str], watermark_column: str | None, current):
    if not watermark_column or watermark_column not in columns:
        return current
    idx = columns.index(watermark_column)
    if batch.arrow_table is not None:
        values = batch.arrow_table.column(watermark_column).to_pylist()
    elif batch.column_values is not None:
        values = batch.column_values.get(watermark_column, [])
    else:
        assert batch.rows is not None
        values = [row[idx] for row in batch.rows]
    for value in values:
        if value is not None and (current is None or value > current):
            current = value
    return current


def _runtime_rowset_root(kwargs: dict[str, Any]) -> Path:
    return Path(kwargs.get("zeta4s_api_home") or os.environ.get("ZETA4S_API_HOME", "/var/lib/zeta4s"))


def _run_id(context: dict[str, Any]) -> str | None:
    if context and context.get("z4_run_id"):
        return str(context["z4_run_id"])
    return str(context.get("run_id")) if context and context.get("run_id") else None


def _task_id(context: dict[str, Any]) -> str | None:
    return str(context.get("task_id")) if context and context.get("task_id") else None


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("._")
    return safe or "item"


def _source_object(source_type: str, source: dict[str, Any]) -> str:
    if source_type == "elasticsearch":
        return str(source.get("index") or source.get("index_template") or "search")
    return str(source.get("table") or source.get("query") or source_type)


def _json_value(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value
