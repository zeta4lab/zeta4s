"""Target schema inspector registry."""

from __future__ import annotations

from zeta4s.runtime.write.target_schema.base import TargetSchemaInspector
from zeta4s.runtime.write.target_schema.oracle import OracleTargetSchemaInspector


TARGET_SCHEMA_INSPECTORS: dict[str, TargetSchemaInspector] = {
    "oracle": OracleTargetSchemaInspector(),
}


def get_target_schema_inspector(target_type: str) -> TargetSchemaInspector | None:
    return TARGET_SCHEMA_INSPECTORS.get(target_type)
