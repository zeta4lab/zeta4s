"""Materialize parquet rowset outputs into runtime backend table snapshots."""

from __future__ import annotations

from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.backends.clickhouse import stage_clickhouse_rowset
from zeta4s.runtime.context import current_context_from_kwargs
from zeta4s.runtime.input_checkpoints import (
    append_input_checkpoint,
    load_input_position,
    runtime_checkpoint_identity,
)
from zeta4s.runtime.rowsets import (
    ResolvedRowsetRef,
    resolve_rowset_ref,
    rowset_column_specs_from_schema_metadata,
    runtime_home,
)
from zeta4s.runtime.rowset_store import rowset_descriptor_payload
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.task_result import log_task_event, record_success, result_context
from zeta4s.runtime.types import (
    clickhouse_type_from_arrow_field,
    clickhouse_type_from_column_spec,
)

import logging

logger = logging.getLogger(__name__)


def run_stage_rowset(
    *,
    stage_type: str,
    stage_conn: str,
    source_ref: str,
    target_table: str,
    target_namespace: str | None = None,
    job_id: str | None = None,
    **kwargs,
):
    """Materialize a named parquet rowset output into a backend table snapshot."""
    context = _current_context(kwargs)
    rowset = resolve_rowset_ref(source_ref=source_ref, context=context, home=runtime_home(kwargs))
    backend_kwargs = dict(kwargs)
    repository = kwargs.get("step_checkpoint_repository")
    identity = runtime_checkpoint_identity(context, job_id)
    after = None
    on_checkpoint = None
    if identity is not None:
        after = load_input_position(
            rowset=rowset,
            repository=repository,
            project_id=identity[0],
            job_id=identity[1],
            run_id=identity[2],
            step_id=identity[3],
            task_id=identity[4],
            unit_id=source_ref,
            expected_receipt={
                "target_type": stage_type,
                "target_table": target_table,
                "target_namespace": target_namespace,
            },
        )
        if repository is not None:

            def on_checkpoint(loaded, batches, position):
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
                        "target_type": stage_type,
                        "target_table": target_table,
                        "target_namespace": target_namespace,
                        "loaded_rows": loaded,
                        "batches": batches,
                    },
                )

    backend_kwargs["_rowset_after"] = after
    backend_kwargs["_rowset_on_checkpoint"] = on_checkpoint
    target_table = validate_table_identifier(target_table, "stage.target_table", max_parts=1)
    if target_namespace:
        target_namespace = validate_table_identifier(target_namespace, "stage.target_namespace", max_parts=1)

    with result_context(f"stage.{stage_type}", context) as (started_at, start_monotonic):
        log_task_event(
            logger,
            f"stage.{stage_type}.plan",
            context=context,
            source_ref=source_ref,
            rowset=rowset.uri,
            target=target_table,
            namespace=target_namespace,
        )
        if stage_type == "clickhouse":
            loaded, target_ref = stage_clickhouse_rowset(
                rowset=rowset,
                stage_conn=stage_conn,
                target_table=target_table,
                target_namespace=target_namespace,
                kwargs=backend_kwargs,
            )
        elif stage_type == "oracle":
            from zeta4s.runtime.backends.oracle.stage import stage_oracle_rowset

            loaded, target_ref = stage_oracle_rowset(
                rowset=rowset,
                stage_conn=stage_conn,
                target_table=target_table,
                target_namespace=target_namespace,
                kwargs=backend_kwargs,
            )
        else:
            raise ValueError(f"unsupported stage_type: {stage_type}")
        metrics = {
            "input_rows": loaded,
            "output_rows": loaded,
            "success_rows": loaded,
            "failed_rows": 0,
            "skipped_rows": 0,
            "error_rows": 0,
        }
    return record_success(
        stage=f"stage.{stage_type}",
        metrics=metrics,
        details={
            "source_ref": source_ref,
            "rowset": rowset_descriptor_payload(rowset.descriptor),
            "target": target_ref,
            "backend": stage_type,
            "job_id": job_id,
        },
        context=context,
        started_at=started_at,
        start_monotonic=start_monotonic,
    )


def _current_context(kwargs: dict[str, Any]) -> dict[str, Any]:
    return current_context_from_kwargs(kwargs)


def _require_schema(schema, source_ref: str) -> None:
    if len(schema) == 0:
        raise ValueError(f"rowset has no schema: {source_ref}")


def _clickhouse_column_specs_from_arrow(schema) -> list[ColumnSpec]:
    return [
        ColumnSpec.from_type(
            validate_sql_identifier(field.name, "clickhouse.stage.rowset.column"),
            clickhouse_type_from_arrow_field(field),
            field.nullable,
            source_backend="arrow",
        )
        for field in schema
    ]


def _clickhouse_type_from_arrow_field(field) -> str:
    return clickhouse_type_from_arrow_field(field)


def _column_specs(rowset: ResolvedRowsetRef, schema) -> list[ColumnSpec]:
    if rowset.column_specs:
        return list(rowset.column_specs)
    metadata_specs = rowset_column_specs_from_schema_metadata(schema)
    if metadata_specs:
        return metadata_specs
    return _clickhouse_column_specs_from_arrow(schema)


def _clickhouse_type_from_column_spec(spec: ColumnSpec) -> str:
    return clickhouse_type_from_column_spec(spec)
