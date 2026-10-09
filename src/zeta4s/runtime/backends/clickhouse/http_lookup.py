"""ClickHouse table materialization for http.lookup."""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.backends.clickhouse.client import get_clickhouse_runtime_client
from zeta4s.runtime.task_result import log_task_event
from zeta4s.runtime.types import nullable_clickhouse_type

logger = logging.getLogger(__name__)


def run_clickhouse_http_lookup(
    *,
    conn_id: str,
    name: str,
    mode: str,
    source_table: str,
    target_table: str,
    input_column: str,
    output_columns: list[dict[str, Any]],
    concurrency: int,
    batch_size: int,
    enrich_row: Callable[[tuple, int], tuple[tuple | None, bool]],
    context: dict[str, Any],
    http: dict[str, Any] | None,
    connections: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_table = validate_table_identifier(source_table, "http.lookup.source.table", max_parts=2)
    target_table = validate_table_identifier(target_table, "http.lookup.target.table", max_parts=2)
    input_column = validate_sql_identifier(input_column, "http.lookup.lookup.column")
    ch_client = get_clickhouse_runtime_client(conn_id, connections=connections)
    source_columns = _clickhouse_columns(ch_client, source_table)
    column_names = [column for column, _ in source_columns]
    if input_column not in column_names:
        raise ValueError(f"http.lookup input_column not found in source table: {input_column}")
    output_column_names = [column["name"] for column in output_columns]
    collisions = [column for column in output_column_names if column in column_names]
    if collisions:
        raise ValueError(f"http.lookup output columns already exist in source table: {collisions}")

    create_columns = ", ".join(
        [f"{column} {ch_type}" for column, ch_type in source_columns]
        + [f"{column['name']} {_clickhouse_output_type(column)}" for column in output_columns]
    )
    select_columns = ", ".join(column_names)
    input_idx = column_names.index(input_column)
    target_columns = column_names + output_column_names

    in_place_target = source_table == target_table
    write_table = target_table
    if in_place_target:
        write_table = validate_table_identifier(
            f"{target_table}_lookup_{uuid.uuid4().hex[:12]}",
            "http.lookup.target_work_table",
            max_parts=2,
        )

    ch_client.command(f"DROP TABLE IF EXISTS {write_table}")
    ch_client.command(f"CREATE TABLE {write_table} ({create_columns}) ENGINE = MergeTree ORDER BY tuple()")
    log_task_event(
        logger,
        "http.lookup.plan",
        context=context,
        name=name,
        mode=mode,
        conn_id=conn_id,
        conn_type="clickhouse",
        source_table=source_table,
        target_table=target_table,
        write_table=write_table,
        concurrency=concurrency,
        batch_size=batch_size,
        api_conn_id=http.get("conn") if http else None,
        http_method=http.get("method") if http else None,
        http_path=http.get("path") if http else None,
    )

    inserted = 0
    failed = 0
    total = 0
    batch = []
    promoted = False

    def flush_success() -> None:
        nonlocal inserted, batch
        if batch:
            ch_client.insert(write_table, batch, column_names=target_columns)
            inserted += len(batch)
            batch = []
            log_task_event(
                logger,
                "http.lookup.progress",
                context=context,
                name=name,
                conn_id=conn_id,
                source_table=source_table,
                write_table=write_table,
                input_rows=total,
                output_rows=inserted,
                failed_rows=failed,
            )

    try:
        with ch_client.query_rows_stream(f"SELECT {select_columns} FROM {source_table}") as stream:
            executor = ThreadPoolExecutor(max_workers=concurrency) if mode == "http" else None
            try:
                rows = []
                for row in stream:
                    rows.append(tuple(row))
                    if len(rows) >= batch_size:
                        total += len(rows)
                        iterable = (
                            executor.map(lambda item: enrich_row(item, input_idx), rows)
                            if executor
                            else (enrich_row(item, input_idx) for item in rows)
                        )
                        for enriched, error in iterable:
                            if enriched:
                                batch.append(enriched)
                            if error:
                                failed += 1
                        flush_success()
                        rows = []
                if rows:
                    total += len(rows)
                    iterable = (
                        executor.map(lambda item: enrich_row(item, input_idx), rows)
                        if executor
                        else (enrich_row(item, input_idx) for item in rows)
                    )
                    for enriched, error in iterable:
                        if enriched:
                            batch.append(enriched)
                        if error:
                            failed += 1
                    flush_success()
            finally:
                if executor:
                    executor.shutdown(wait=True)

        if in_place_target:
            ch_client.command(f"DROP TABLE IF EXISTS {target_table}")
            ch_client.command(f"RENAME TABLE {write_table} TO {target_table}")
            promoted = True
            log_task_event(
                logger,
                "http.lookup.cleanup",
                context=context,
                name=name,
                write_table=write_table,
                target_table=target_table,
                status="promoted",
            )

        logger.info(
            "http.lookup step=%s completed input_rows=%d output_rows=%d failed_rows=%d target_table=%s",
            name,
            total,
            inserted,
            failed,
            target_table,
        )
        return {
            "input_rows": total,
            "output_rows": inserted,
            "success_rows": inserted,
            "failed_rows": failed,
            "skipped_rows": failed,
            "error_rows": failed,
        }
    finally:
        if in_place_target and not promoted:
            ch_client.command(f"DROP TABLE IF EXISTS {write_table}")


def _clickhouse_columns(ch_client, source_table: str) -> list[tuple[str, str]]:
    result = ch_client.query(f"DESCRIBE TABLE {source_table}")
    return [(row[0], row[1]) for row in result.result_rows]


def _clickhouse_output_type(column: dict[str, Any]) -> str:
    ch_type = str(column["type"]).strip()
    return nullable_clickhouse_type(ch_type, bool(column.get("nullable", True)))
