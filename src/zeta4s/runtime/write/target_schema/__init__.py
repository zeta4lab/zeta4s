"""Target schema inspection and validation contracts."""

from zeta4s.runtime.write.target_schema.base import (
    TargetColumnContract,
    TargetSchemaInspector,
    TargetTableContract,
    TargetUniqueConstraint,
)
from zeta4s.runtime.write.target_schema.registry import get_target_schema_inspector
from zeta4s.runtime.write.target_schema.validation import validate_target_schema_contract

__all__ = [
    "TargetColumnContract",
    "TargetSchemaInspector",
    "TargetTableContract",
    "TargetUniqueConstraint",
    "get_target_schema_inspector",
    "validate_target_schema_contract",
]
