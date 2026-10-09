"""Oracle table materialization for http.lookup."""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier
from zeta4s.runtime.backends.oracle.client import get_oracle_conn
from zeta4s.runtime.backends.oracle.stage import drop_oracle_table
from zeta4s.runtime.task_result import log_task_event
from zeta4s.runtime.types import unwrap_clickhouse_type

logger = logging.getLogger(__name__)


def run_oracle_http_lookup(
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
    source_table = validate_table_identifier(source_table, "http.lookup.source.table", max_parts=2).upper()
    target_table = validate_table_identifier(target_table, "http.lookup.target.table", max_parts=2).upper()
    input_column = validate_sql_identifier(input_column, "http.lookup.lookup.column").upper()
    output_column_names = [
        validate_sql_identifier(column["name"], "http.lookup.output.column").upper() for column in output_columns
    ]

    write_table = target_table
    conn = get_oracle_conn(conn_id, connections=connections)
    inserted = 0
    failed = 0
    total = 0
    promoted = False
    try:
        cursor = conn.cursor()
        try:
            source_columns = _oracle_columns(cursor, source_table)
            column_names = [column for column, _type in source_columns]
            if input_column not in column_names:
                raise ValueError(f"http.lookup input_column not found in source table: {input_column}")
            collisions = [column for column in output_column_names if column in column_names]
            if collisions:
                raise ValueError(f"http.lookup output columns already exist in source table: {collisions}")

            in_place_target = source_table == target_table
            if in_place_target:
                write_table = validate_table_identifier(
                    f"{target_table}_LOOKUP_{uuid.uuid4().hex[:12]}",
                    "http.lookup.target_work_table",
                    max_parts=2,
                ).upper()

            drop_oracle_table(cursor, write_table)
            _create_target_table(cursor, source_table, write_table, output_columns)
            log_task_event(
                logger,
                "http.lookup.plan",
                context=context,
                name=name,
                mode=mode,
                conn_id=conn_id,
                conn_type="oracle",
                source_table=source_table,
                target_table=target_table,
                write_table=write_table,
                concurrency=concurrency,
                batch_size=batch_size,
                api_conn_id=http.get("conn") if http else None,
                http_method=http.get("method") if http else None,
                http_path=http.get("path") if http else None,
            )

            input_idx = column_names.index(input_column)
            target_columns = column_names + output_column_names
            placeholders = ", ".join(f":{index + 1}" for index in range(len(target_columns)))
            insert_sql = f"INSERT INTO {write_table} ({', '.join(target_columns)}) VALUES ({placeholders})"

            batch: list[tuple] = []

            def flush_success() -> None:
                nonlocal inserted, batch
                if not batch:
                    return
                cursor.executemany(insert_sql, batch)
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

            select_sql = f"SELECT {', '.join(column_names)} FROM {source_table}"
            read_cursor = conn.cursor()
            executor = ThreadPoolExecutor(max_workers=concurrency) if mode == "http" else None
            try:
                read_cursor.execute(select_sql)
                rows: list[tuple] = []
                for row in read_cursor:
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
                read_cursor.close()

            if in_place_target:
                drop_oracle_table(cursor, target_table)
                cursor.execute(f"ALTER TABLE {write_table} RENAME TO {target_table.split('.')[-1]}")
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

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        try:
            if target_table == source_table and not promoted:
                cleanup_conn = get_oracle_conn(conn_id, connections=connections)
                try:
                    cleanup_cursor = cleanup_conn.cursor()
                    try:
                        drop_oracle_table(cleanup_cursor, write_table)
                        cleanup_conn.commit()
                    finally:
                        cleanup_cursor.close()
                finally:
                    cleanup_conn.close()
        finally:
            conn.close()

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


def _oracle_columns(cursor, source_table: str) -> list[tuple[str, Any]]:
    cursor.execute(f"SELECT * FROM {source_table} WHERE 1 = 0")
    return [(description[0].upper(), description[1]) for description in cursor.description]


def _create_target_table(
    cursor,
    source_table: str,
    target_table: str,
    output_columns: list[dict[str, Any]],
) -> None:
    cursor.execute(f"CREATE TABLE {target_table} AS SELECT source_table.* FROM {source_table} source_table WHERE 1 = 0")
    for column in output_columns:
        name = validate_sql_identifier(column["name"], "http.lookup.output.column").upper()
        cursor.execute(f"ALTER TABLE {target_table} ADD ({name} {_oracle_output_type(column)})")


def _oracle_output_type(column: dict[str, Any]) -> str:
    type_name = unwrap_clickhouse_type(str(column["type"]).strip()).upper()
    if type_name in {"STRING", "FIXEDSTRING"}:
        return "VARCHAR2(4000)"
    if type_name in {"BOOL", "BOOLEAN"}:
        return "NUMBER(1,0)"
    if type_name.startswith(("UINT", "INT")):
        return "NUMBER(19,0)"
    if type_name.startswith("DECIMAL"):
        return "NUMBER(38,10)"
    if type_name.startswith("FLOAT"):
        return "BINARY_DOUBLE"
    if type_name == "DATE":
        return "DATE"
    if type_name.startswith("DATETIME"):
        return "TIMESTAMP(6)"
    return "VARCHAR2(4000)"
