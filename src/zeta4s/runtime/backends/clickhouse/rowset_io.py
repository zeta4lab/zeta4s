"""ClickHouse rowset IO helpers shared by stage and write backends."""

from __future__ import annotations

from io import BytesIO


DEFAULT_BATCH_SIZE = 65_536


def require_rowset_schema(schema, source_ref: str) -> None:
    if len(schema) == 0:
        raise ValueError(f"rowset has no schema: {source_ref}")


def validate_no_nulls(rowset, columns: list[str], *, batch_size: int, label: str) -> None:
    if not columns:
        return
    for batch in rowset.iter_batches(batch_size=batch_size, columns=columns):
        for column in columns:
            if batch.column(column).null_count:
                raise ValueError(f"{label} column contains null values: {column}")


def insert_rowset_batches(
    *,
    client,
    target_ref: str,
    rowset,
    columns: list[str],
    batch_size: int,
    on_progress=None,
    after=None,
    on_checkpoint=None,
) -> tuple[int, int]:
    loaded = 0
    batches = 0
    for item in rowset.iter_positioned_batches(batch_size=batch_size, columns=columns, after=after):
        batch = item.batch
        if not batch.num_rows:
            continue
        insert_record_batch(client, target_ref, batch, columns)
        loaded += int(batch.num_rows)
        batches += 1
        if on_progress is not None:
            on_progress(loaded, batches)
        if on_checkpoint is not None:
            on_checkpoint(loaded, batches, item.continuation)
    return loaded, batches


def insert_record_batch(client, target_ref: str, batch, columns: list[str]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_batches([batch])
    raw_insert = getattr(client, "raw_insert", None)
    if raw_insert is None:
        client.insert_arrow(target_ref, table)
        return
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    raw_insert(
        target_ref,
        column_names=columns,
        insert_block=buffer.getvalue(),
        fmt="Parquet",
    )
