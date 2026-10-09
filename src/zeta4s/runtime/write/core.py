"""Rowset write runtime facade."""

from __future__ import annotations

import logging
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier_list, validate_table_identifier
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.input_checkpoints import (
    append_input_checkpoint,
    load_input_position,
    runtime_checkpoint_identity,
)
from zeta4s.runtime.rowsets import resolve_rowset_ref, runtime_home
from zeta4s.runtime.rowset_store import rowset_descriptor_payload
from zeta4s.runtime.task_result import log_task_event, record_success, result_context

logger = logging.getLogger(__name__)


def run_write_rowset(
    *,
    target_type: str,
    target_conn: str,
    source_ref: str,
    target_table: str | None = None,
    target_namespace: str | None = None,
    mode: str,
    columns: list[str],
    key: list[str] | None = None,
    writer_options: dict[str, Any] | None = None,
    job_id: str | None = None,
    write_name: str | None = None,
    **kwargs,
):
    context = _current_context(kwargs)
    rowset = resolve_rowset_ref(source_ref=source_ref, context=context, home=runtime_home(kwargs))
    columns = validate_sql_identifier_list(columns, "write.columns")
    key = validate_sql_identifier_list(key or [], "write.key") if key else []
    mode = str(mode or "").strip()
    repository = kwargs.get("step_checkpoint_repository")
    identity = runtime_checkpoint_identity(context, job_id)
    checkpoint_after = None
    checkpoint_callback = None
    if mode in {"replace", "upsert"} and identity is not None:
        checkpoint_after = load_input_position(
            rowset=rowset,
            repository=repository,
            project_id=identity[0],
            job_id=identity[1],
            run_id=identity[2],
            step_id=identity[3],
            task_id=identity[4],
            unit_id=source_ref,
            expected_receipt={
                "target_type": target_type,
                "target_table": target_table,
                "target_namespace": target_namespace,
                "mode": mode,
            },
        )
        if repository is not None:

            def checkpoint_callback(loaded, batches, position):
                append_input_checkpoint(
                    rowset=rowset,
                    repository=repository,
                    project_id=identity[0],
                    job_id=identity[1],
                    run_id=identity[2],
                    step_id=identity[3],
                    task_id=identity[4],
                    unit_id=source_ref,
                    attempt=identity[5],
                    position=position,
                    receipt={
                        "target_type": target_type,
                        "target_table": target_table,
                        "target_namespace": target_namespace,
                        "mode": mode,
                        "loaded_rows": loaded,
                        "batches": batches,
                    },
                )

    backend_options = dict(writer_options or {})
    backend_options["_rowset_after"] = checkpoint_after
    backend_options["_rowset_on_checkpoint"] = checkpoint_callback
    with result_context("write", context) as (started_at, start_monotonic):
        log_task_event(
            logger,
            "write.plan",
            context=context,
            target_type=target_type,
            source_ref=source_ref,
            rowset=rowset.uri,
            target=target_table,
            namespace=target_namespace,
            mode=mode,
            columns=len(columns),
            key_columns=len(key),
        )
        if target_type == "clickhouse":
            if not target_table:
                raise ValueError("clickhouse.write requires target_table")
            from zeta4s.runtime.backends.clickhouse.write import write_clickhouse_rowset

            result = write_clickhouse_rowset(
                rowset=rowset,
                target_conn=target_conn,
                target_table=validate_table_identifier(target_table, "clickhouse.write.target_table", max_parts=1),
                target_namespace=(
                    validate_table_identifier(target_namespace, "clickhouse.write.target_namespace", max_parts=1)
                    if target_namespace
                    else None
                ),
                mode=mode,
                columns=columns,
                key=key,
                options=backend_options,
                context=context,
                connections=kwargs.get("connections"),
            )
        elif target_type == "oracle":
            if not target_table:
                raise ValueError("oracle.write requires target_table")
            from zeta4s.runtime.backends.oracle.write import write_oracle_rowset

            result = write_oracle_rowset(
                rowset=rowset,
                target_conn=target_conn,
                target_table=validate_table_identifier(target_table, "oracle.write.target_table", max_parts=1),
                target_namespace=(
                    validate_table_identifier(target_namespace, "oracle.write.target_namespace", max_parts=1)
                    if target_namespace
                    else None
                ),
                mode=mode,
                columns=columns,
                key=key,
                options=backend_options,
                context=context,
                connections=kwargs.get("connections"),
            )
        elif target_type == "elasticsearch":
            from zeta4s.runtime.backends.elasticsearch.write import write_elasticsearch_rowset

            result = write_elasticsearch_rowset(
                rowset=rowset,
                target_conn=target_conn,
                mode=mode,
                columns=columns,
                key=key,
                options=backend_options,
                context=context,
                connections=kwargs.get("connections"),
            )
        else:
            raise ValueError(f"unsupported rowset write target: {target_type}")
        metrics = {
            "input_rows": result["input_rows"],
            "output_rows": result["output_rows"],
            "success_rows": result["success_rows"],
            "failed_rows": result["failed_rows"],
            "skipped_rows": result["skipped_rows"],
            "error_rows": 0,
            "batches": result["batches"],
        }
    return record_success(
        stage="write",
        metrics=metrics,
        details={
            "source": {
                "ref": source_ref,
                **rowset_descriptor_payload(rowset.descriptor),
            },
            "target_type": target_type,
            "target": result["target"],
            "mode": mode,
            "columns": columns,
            "key": key,
            "job_id": job_id,
            "write_name": write_name,
            "manifest": {
                "source": source_ref,
                "target_type": target_type,
                "target": result["target"],
                "mode": mode,
                "columns": columns,
                "key": key,
                "batch_count": result["batches"],
                "input_rows": result["input_rows"],
                "output_rows": result["output_rows"],
            },
        },
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs, set_run_date=True)
