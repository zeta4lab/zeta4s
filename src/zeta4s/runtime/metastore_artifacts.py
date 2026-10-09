"""Metastore-backed artifact lookup for adapter-scoped step execution."""

from __future__ import annotations

from typing import Any

from zeta4s.core import InMemoryArtifactStore, StepOutputBinding
from zeta4s.project.execution_plan import TableRef


class MetastoreArtifactStore:
    def __init__(
        self,
        *,
        project_id: str,
        job_id: str,
        run_id: str,
        repository: Any,
    ) -> None:
        self.project_id = project_id
        self.job_id = job_id
        self.run_id = run_id
        self.repository = repository
        self._cache = InMemoryArtifactStore()

    def put(self, key: str, value: Any) -> None:
        self._cache.put(key, value)

    def get(self, key: str) -> Any:
        try:
            return self._cache.get(key)
        except KeyError:
            pass
        step_id, output_name = _parse_step_output_key(key)
        row = self.repository.get_binding(
            project_id=self.project_id,
            job_id=self.job_id,
            run_id=self.run_id,
            step_id=step_id,
            output_name=output_name,
        )
        if row is None:
            raise KeyError(key)
        binding = _step_output_binding_from_row(row)
        self._cache.put(key, binding)
        return binding


def _parse_step_output_key(key: str) -> tuple[str, str]:
    prefix = "step-output/"
    if not key.startswith(prefix):
        raise KeyError(key)
    remainder = key[len(prefix) :]
    step_id, separator, output_name = remainder.partition("/")
    if not separator or not step_id or not output_name:
        raise KeyError(key)
    return step_id, output_name


def _step_output_binding_from_row(row: dict[str, Any]) -> StepOutputBinding:
    payload = row.get("binding") if isinstance(row.get("binding"), dict) else {}
    table_ref = _table_ref(payload.get("table_ref"))
    output_name = str(row.get("output_name") or payload.get("output_name") or "")
    step_id = str(row.get("step_id") or payload.get("step_id") or "")
    if not step_id or not output_name:
        raise KeyError("invalid step output binding row")
    return StepOutputBinding(
        step_id=step_id,
        output_name=output_name,
        kind=str(row.get("output_kind") or payload.get("kind") or "value"),
        value=payload.get("value"),
        table_ref=table_ref,
        ref=dict(payload.get("ref") or {}),
    )


def _table_ref(value: Any) -> TableRef | None:
    if not isinstance(value, dict):
        return None
    conn = value.get("conn")
    table = value.get("table")
    if not conn or not table:
        return None
    return TableRef(conn=str(conn), table=str(table))


__all__ = [
    "MetastoreArtifactStore",
]
