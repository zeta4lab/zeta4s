"""zeta4s rowset contract helper."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from zeta4s.common.sql_identifiers import validate_sql_identifier
from zeta4s.runtime.rowset_contract import ROWSET_COLUMN_SPECS_METADATA_KEY
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetStorage
from zeta4s.runtime.rowset_store import RowsetReader, rowset_descriptor_from_payload
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore
from zeta4s.runtime.source_reader import ColumnSpec
from zeta4s.runtime.task_result import load_task_results
from zeta4s.runtime.types import arrow_type_from_column_spec


@dataclass(frozen=True)
class ResolvedRowsetRef:
    source_ref: str
    step_id: str
    output_name: str
    descriptor: RowsetDescriptor
    reader: RowsetReader

    @property
    def uri(self) -> str:
        return self.descriptor.uri

    @property
    def rows(self) -> int:
        return self.descriptor.rows

    @property
    def bytes(self) -> int:
        return self.descriptor.bytes

    @property
    def columns(self) -> tuple[str, ...]:
        return self.descriptor.columns

    @property
    def column_specs(self) -> tuple[ColumnSpec, ...]:
        return self.descriptor.column_specs

    @property
    def schema(self):
        import pyarrow as pa

        if not self.column_specs:
            raise ValueError(f"rowset descriptor has no column specs: {self.source_ref}")
        return pa.schema(
            [
                pa.field(spec.name, arrow_type_from_column_spec(spec), nullable=spec.nullable)
                for spec in self.column_specs
            ]
        )

    def iter_batches(self, *, batch_size: int, columns: list[str] | None = None):
        return self.reader.iter_batches(batch_size=batch_size, columns=columns)

    def iter_positioned_batches(
        self,
        *,
        batch_size: int,
        columns: list[str] | None = None,
        after: dict[str, Any] | None = None,
    ):
        return self.reader.iter_positioned_batches(batch_size=batch_size, columns=columns, after=after)


def resolve_rowset_ref(*, source_ref: str, context: dict[str, Any], home: Path | None = None) -> ResolvedRowsetRef:
    step_id, output_name = parse_rowset_ref(source_ref)
    context_output = _context_rowset_output(source_ref, context)
    if context_output is not None:
        return _resolved_rowset(source_ref, step_id, output_name, context_output, context, home)
    run_id = rowset_run_id(context)
    if not run_id:
        raise ValueError("rowset resolution requires run_id")
    for result in load_task_results(run_id, home=home):
        if str(result.get("task_id") or "") != step_id:
            continue
        details = result.get("details") if isinstance(result.get("details"), dict) else {}
        outputs = details.get("outputs") if isinstance(details.get("outputs"), dict) else {}
        output = outputs.get(output_name)
        if isinstance(output, dict):
            return _resolved_rowset(source_ref, step_id, output_name, output, context, home)
    raise ValueError(f"rowset output not found in task results: {source_ref}")


def _context_rowset_output(source_ref: str, context: dict[str, Any]) -> dict[str, Any] | None:
    bindings = context.get("step_output_bindings") if isinstance(context, dict) else None
    if not isinstance(bindings, dict):
        return None
    raw_binding = bindings.get(source_ref)
    if not isinstance(raw_binding, dict):
        return None
    value = raw_binding.get("value")
    if isinstance(value, dict):
        output = dict(value)
    else:
        output = {"value": value}
    output.setdefault("kind", raw_binding.get("kind"))
    return output


def parse_rowset_ref(source_ref: str) -> tuple[str, str]:
    if not isinstance(source_ref, str) or "." not in source_ref:
        raise ValueError("rowset ref must use <step_id>.<output_name>")
    step_id, output_name = source_ref.split(".", 1)
    if not step_id.strip() or not output_name.strip():
        raise ValueError("rowset ref must use <step_id>.<output_name>")
    return step_id.strip(), output_name.strip()


def rowset_run_id(context: dict[str, Any]) -> str | None:
    if context and context.get("z4_run_id"):
        return str(context["z4_run_id"])
    return str(context.get("run_id")) if context and context.get("run_id") else None


def runtime_home(kwargs: dict[str, Any]) -> Path | None:
    raw_home = kwargs.get("zeta4s_api_home") or kwargs.get("runtime_home")
    return Path(raw_home) if raw_home else None


def _resolved_rowset(
    source_ref: str,
    step_id: str,
    output_name: str,
    output: dict[str, Any],
    context: dict[str, Any],
    home: Path | None,
) -> ResolvedRowsetRef:
    if output.get("kind") != "rowset":
        raise ValueError(f"source_ref does not reference a rowset output: {source_ref}")
    descriptor = rowset_descriptor_from_payload(output)
    store = context.get("rowset_store") if isinstance(context, dict) else None
    if store is None:
        if descriptor.storage is RowsetStorage.PARQUET:
            store = ParquetRowsetStore(home or Path("."))
        else:
            from zeta4s.runtime.rowset_stores.iceberg import IcebergRowsetStore

            store = IcebergRowsetStore.from_environment()
    return ResolvedRowsetRef(
        source_ref=source_ref,
        step_id=step_id,
        output_name=output_name,
        descriptor=descriptor,
        reader=store.open_reader(descriptor),
    )


def normalize_column_specs(value: Any) -> list[ColumnSpec]:
    if not isinstance(value, list):
        return []
    specs: list[ColumnSpec] = []
    for item in value:
        spec = ColumnSpec.from_value(item)
        specs.append(
            ColumnSpec(
                name=validate_sql_identifier(spec.name, "rowset.column_specs.name"),
                type=spec.type,
                nullable=spec.nullable,
                logical_type=spec.logical_type,
                precision=spec.precision,
                scale=spec.scale,
                datetime_precision=spec.datetime_precision,
                source_backend=spec.source_backend,
                source_type=spec.source_type,
            )
        )
    return specs


def rowset_column_specs_from_schema_metadata(schema) -> list[ColumnSpec]:
    metadata = schema.metadata or {}
    raw = metadata.get(ROWSET_COLUMN_SPECS_METADATA_KEY)
    if not raw:
        return []
    try:
        return normalize_column_specs(json.loads(raw.decode("utf-8")))
    except Exception as exc:
        raise ValueError("rowset column_specs metadata is invalid") from exc
