"""ClickHouse rowset extract reader."""

from __future__ import annotations

import logging

from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.backends.clickhouse.client import get_clickhouse_source_client
from zeta4s.runtime.backends.clickhouse.params import bind_clickhouse_named_params
from zeta4s.runtime.rowset_models import ResumeCapability
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch, reject_restart_only_continuation
from zeta4s.runtime.types import clickhouse_type_from_arrow_field, is_clickhouse_not_null_type

logger = logging.getLogger(__name__)


class ClickHouseSelectReader:
    source_kind = "clickhouse"
    resume_capability = ResumeCapability.RESTART_ONLY

    def __init__(
        self,
        *,
        source_conn: str,
        source_object: str,
        query: str,
        params: dict,
        batch_size: int,
        connections: dict | None = None,
    ) -> None:
        self.source_conn = source_conn
        self.source_object = source_object
        # The rowset extract contract writes `:name` placeholders; ClickHouse binds `{name:Type}`.
        self.query, self.params = bind_clickhouse_named_params(query, params)
        self.batch_size = int(batch_size)
        self.connections = connections
        self.column_specs: list[ColumnSpec] = []
        self.columns: list[str] = []
        self._client = None

    def __enter__(self) -> "ClickHouseSelectReader":
        self._client = get_clickhouse_source_client(self.source_conn, connections=self.connections)
        self._set_schema_from_clickhouse_query()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._client = None

    def read_batches(self, *, after: dict | None = None):
        reject_restart_only_continuation(after, self.source_kind)
        if self._client is None:
            raise RuntimeError("ClickHouseSelectReader must be opened before reading")
        yielded = False
        try:
            with self._client.query_arrow_stream(self.query, parameters=self.params) as stream:
                for item in stream:
                    for batch in _arrow_batches(item, self.batch_size):
                        yielded = True
                        yield self._source_batch_from_arrow(batch)
            if yielded:
                return
        except Exception:
            if yielded:
                raise
        arrow = self._client.query_arrow(self.query, parameters=self.params)
        self._set_schema_from_arrow(arrow.schema)
        for batch in arrow.to_batches(max_chunksize=self.batch_size):
            yield self._source_batch_from_arrow(batch)

    def _source_batch_from_arrow(self, batch) -> SourceBatch:
        import pyarrow as pa

        if not self.column_specs:
            self._set_schema_from_arrow(batch.schema)
        return SourceBatch(arrow_table=pa.Table.from_batches([batch]))

    def _set_schema_from_arrow(self, schema) -> None:
        self.columns = [validate_sql_identifier(str(name), "clickhouse.extract.column") for name in schema.names]
        self.column_specs = [
            ColumnSpec.from_type(
                column, clickhouse_type_from_arrow_field(field), field.nullable, source_backend="clickhouse"
            )
            for column, field in zip(self.columns, schema, strict=True)
        ]

    def _set_schema_from_clickhouse_query(self) -> None:
        if self._client is None:
            return
        try:
            result = self._client.query(f"DESCRIBE TABLE ({self.query})", parameters=self.params)
        except Exception:
            logger.debug("Failed to describe ClickHouse extract query; falling back to Arrow schema", exc_info=True)
            return
        specs = []
        for row in result.result_rows:
            column = validate_sql_identifier(str(row[0]), "clickhouse.extract.column")
            ch_type = str(row[1])
            specs.append(
                ColumnSpec.from_type(
                    column,
                    ch_type,
                    not is_clickhouse_not_null_type(ch_type),
                    source_backend="clickhouse",
                    source_type=ch_type,
                )
            )
        if specs:
            self.column_specs = specs
            self.columns = [column for column, _, _ in specs]


def _arrow_batches(item, batch_size: int):
    import pyarrow as pa

    if isinstance(item, pa.RecordBatch):
        yield item
        return
    if isinstance(item, pa.Table):
        yield from item.to_batches(max_chunksize=batch_size)
        return
    if hasattr(item, "to_batches"):
        yield from item.to_batches(max_chunksize=batch_size)
        return
    raise TypeError(f"unsupported Arrow stream item: {type(item)!r}")
