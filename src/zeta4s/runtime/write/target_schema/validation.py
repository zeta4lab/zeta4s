"""Target-neutral write schema validation."""

from __future__ import annotations

from zeta4s.runtime.write.target_schema.base import TargetColumnContract, TargetTableContract
from zeta4s.runtime.write.types import (
    WriteTypeIssue,
    clickhouse_type_family,
    decimal_precision,
    normalize_clickhouse_type,
    unwrap_clickhouse_type,
)


def validate_target_schema_contract(
    *,
    target_contract: TargetTableContract,
    source_schema: dict[str, str],
    columns: list[str],
    key: list[str],
    mode: str | None = None,
    job: str | None = None,
    write: str | None = None,
    model: str | None = None,
    source_kind: str | None = None,
    step: str = "target_schema_validate",
) -> list[WriteTypeIssue]:
    issues: list[WriteTypeIssue] = []
    target_columns = target_contract.column_by_name()
    required_columns = _required_columns(columns, key)

    for column in required_columns:
        target_column = target_columns.get(column.lower())
        if target_column is None:
            issues.append(
                _issue(
                    code="Z4E_WRITE_TARGET_COLUMN_MISSING",
                    message="Write column is missing from target table schema.",
                    target_contract=target_contract,
                    column=column,
                    reason=f"target table {target_contract.full_name} has no column named {column}.",
                    suggestion="Align write.columns/key with the target table columns, or prepare the target schema first.",
                    job=job,
                    write=write,
                    model=model,
                    step=step,
                )
            )
            continue
        ch_type = source_schema.get(column)
        if ch_type is None:
            issues.append(
                _issue(
                    code="Z4E_WRITE_COLUMN_MISSING",
                    message="Write column is missing from source model schema.",
                    target_contract=target_contract,
                    target_column=target_column,
                    column=column,
                    reason=f"source projection has no column named {column}.",
                    suggestion="Align write.columns/key with the dbt model columns, or add an alias in the source projection.",
                    job=job,
                    write=write,
                    model=model,
                    step=step,
                )
            )
            continue
        issues.extend(
            _validate_column_type(
                target_contract=target_contract,
                target_column=target_column,
                column=column,
                clickhouse_type=ch_type,
                job=job,
                write=write,
                model=model,
                source_kind=source_kind,
                step=step,
            )
        )

    if key and mode != "replace":
        key_issue = _validate_key_contract(
            target_contract=target_contract,
            key=key,
            job=job,
            write=write,
            model=model,
            step=step,
        )
        if key_issue:
            issues.append(key_issue)

    return issues


def _validate_column_type(
    *,
    target_contract: TargetTableContract,
    target_column: TargetColumnContract,
    column: str,
    clickhouse_type: str,
    job: str | None,
    write: str | None,
    model: str | None,
    source_kind: str | None,
    step: str,
) -> list[WriteTypeIssue]:
    base = unwrap_clickhouse_type(clickhouse_type)
    family = clickhouse_type_family(base)
    issues: list[WriteTypeIssue] = []
    common = {
        "target_contract": target_contract,
        "column": column,
        "clickhouse_type": clickhouse_type,
        "job": job,
        "write": write,
        "model": model,
        "step": step,
    }
    projection_label = _source_projection_label(source_kind)

    if not target_column.nullable and normalize_clickhouse_type(clickhouse_type).startswith("Nullable("):
        issues.append(
            _issue(
                code="Z4E_WRITE_MODEL_NORMALIZE_REQUIRED",
                message="Source projection can produce NULL for a NOT NULL target column.",
                reason=f"target column {column} is NOT NULL.",
                suggestion=f"Remove possible NULLs in {projection_label} with WHERE, coalesce(), or an explicit default.",
                target_column=target_column,
                **common,
            )
        )

    if family in {"Array", "Map", "Tuple", "Nested", "Object", "JSON"}:
        issues.append(
            _issue(
                code="Z4E_WRITE_MODEL_CAST_REQUIRED",
                message="Source projection uses a complex type for scalar target column.",
                reason=f"ClickHouse type {clickhouse_type} cannot be bound to target column {column}.",
                suggestion=f"Convert the value in {projection_label} to a scalar that fits the target column.",
                target_column=target_column,
                **common,
            )
        )
        return issues

    if target_column.logical_family == "number":
        issue = _validate_number_target(target_column, family, base, projection_label=projection_label, **common)
    elif target_column.logical_family == "string":
        issue = _validate_string_target(target_column, family, base, projection_label=projection_label, **common)
    elif target_column.logical_family == "datetime":
        issue = _validate_datetime_target(target_column, family, base, projection_label=projection_label, **common)
    else:
        issue = _issue(
            code="Z4E_WRITE_TARGET_TYPE_UNSUPPORTED",
            message="Target column type is not supported by write validation.",
            reason=f"target column {column} type {target_column.target_type} has no validator.",
            suggestion="Project to a supported target type, or add a target adapter validator.",
            target_column=target_column,
            **common,
        )
    if issue:
        issues.append(issue)
    return issues


def _validate_number_target(
    target_column: TargetColumnContract,
    family: str,
    base: str,
    projection_label: str,
    **common,
) -> WriteTypeIssue | None:
    if family.startswith("Int") and family not in {"Int8", "Int16", "Int32", "Int64"}:
        return _cast_issue("numeric", target_column, projection_label=projection_label, **common)
    if family.startswith("Decimal"):
        precision = decimal_precision(base)
        if target_column.precision is not None and precision is not None and precision > target_column.precision:
            return _issue(
                code="Z4E_WRITE_MODEL_PRECISION_UNSAFE",
                message="Source Decimal precision exceeds target precision.",
                reason=f"source precision {precision} exceeds target precision {target_column.precision}.",
                suggestion=f"Cast to a Decimal within the target precision in {projection_label}.",
                **common,
            )
    if family in {"Date", "Date32", "DateTime", "DateTime64"}:
        return _cast_issue("numeric", target_column, projection_label=projection_label, **common)
    return None


def _validate_string_target(
    target_column: TargetColumnContract,
    family: str,
    base: str,
    projection_label: str,
    **common,
) -> WriteTypeIssue | None:
    if family == "FixedString" and target_column.length is not None:
        fixed_length = _fixed_string_length(base)
        if fixed_length is not None and fixed_length > target_column.length:
            return _issue(
                code="Z4E_WRITE_MODEL_LENGTH_UNSAFE",
                message="Source FixedString length exceeds target length.",
                reason=f"source FixedString({fixed_length}) exceeds target length {target_column.length}.",
                suggestion=f"Explicitly convert to a value within the target length in {projection_label}.",
                **common,
            )
    return None


def _validate_datetime_target(
    target_column: TargetColumnContract,
    family: str,
    base: str,
    projection_label: str,
    **common,
) -> WriteTypeIssue | None:
    if family not in {"String", "FixedString", "Date", "Date32", "DateTime", "DateTime64"}:
        return _cast_issue("datetime", target_column, projection_label=projection_label, **common)
    if str(target_column.target_type or "").upper() == "DATE":
        return None
    if family == "DateTime64" and target_column.datetime_precision is not None:
        precision = _datetime64_precision(base)
        if precision is not None and precision > target_column.datetime_precision:
            return _issue(
                code="Z4E_WRITE_MODEL_PRECISION_UNSAFE",
                message="Source DateTime64 precision exceeds target timestamp precision.",
                reason=f"source precision {precision} exceeds target precision {target_column.datetime_precision}.",
                suggestion=f"Set an explicit datetime precision matching the target precision in {projection_label}.",
                **common,
            )
    return None


def _validate_key_contract(
    *,
    target_contract: TargetTableContract,
    key: list[str],
    job: str | None,
    write: str | None,
    model: str | None,
    step: str,
) -> WriteTypeIssue | None:
    key_set = {column.lower() for column in key}
    for constraint in target_contract.unique_constraints:
        if {column.lower() for column in constraint.columns} == key_set:
            return None
    return _issue(
        code="Z4E_WRITE_TARGET_KEY_MISSING",
        message="Write key does not match a target primary or unique key.",
        target_contract=target_contract,
        column=", ".join(key),
        reason=f"write.key={key} has no matching primary/unique constraint on {target_contract.full_name}.",
        suggestion="Align write.key with the target primary/unique key, or create the target constraint first.",
        job=job,
        write=write,
        model=model,
        step=step,
    )


def _required_columns(columns: list[str], key: list[str]) -> list[str]:
    required = list(columns)
    for column in key:
        if column not in required:
            required.append(column)
    return required


def _source_projection_label(source_kind: str | None) -> str:
    if source_kind == "map":
        return "map source query"
    return "dbt model"


def _cast_issue(
    target_family: str,
    target_column: TargetColumnContract,
    *,
    projection_label: str = "dbt model",
    **common,
) -> WriteTypeIssue:
    return _issue(
        code="Z4E_WRITE_MODEL_CAST_REQUIRED",
        message="Source projection type does not match target column family.",
        reason=f"target column {target_column.name} requires {target_family} compatible value.",
        suggestion=f"Add an explicit cast/normalization to target {target_column.target_type} in {projection_label}.",
        **common,
    )


def _fixed_string_length(base: str) -> int | None:
    if not base.startswith("FixedString("):
        return None
    try:
        return int(base.split("(", 1)[1].split(")", 1)[0])
    except ValueError:
        return None


def _datetime64_precision(base: str) -> int | None:
    if not base.startswith("DateTime64("):
        return None
    try:
        return int(base.split("(", 1)[1].split(",", 1)[0].split(")", 1)[0])
    except ValueError:
        return None


def _issue(
    *,
    code: str,
    message: str,
    target_contract: TargetTableContract,
    reason: str,
    suggestion: str,
    step: str,
    job: str | None = None,
    write: str | None = None,
    model: str | None = None,
    target_column: TargetColumnContract | None = None,
    column: str | None = None,
    clickhouse_type: str | None = None,
) -> WriteTypeIssue:
    return WriteTypeIssue(
        code=code,
        severity="error",
        message=message,
        step=step,
        job=job,
        write=write,
        model=model,
        target_type=target_contract.target_type,
        target_table=target_contract.full_name,
        column=column or (target_column.name if target_column else None),
        clickhouse_type=clickhouse_type,
        reason=reason,
        suggestion=suggestion,
    )
