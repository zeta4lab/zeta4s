"""Oracle target schema inspector."""

from __future__ import annotations

from zeta4s.common.sql_identifiers import validate_table_identifier
from zeta4s.runtime.write.target_schema.base import (
    TargetColumnContract,
    TargetTableContract,
    TargetUniqueConstraint,
)


class OracleTargetSchemaInspector:
    target_type = "oracle"

    def inspect_table(self, target_conn: str, target_table: str) -> TargetTableContract:
        from zeta4s.runtime.backends.oracle.client import get_oracle_conn

        owner, table = _split_oracle_table_name(target_table)
        conn = get_oracle_conn(target_conn)
        try:
            cursor = conn.cursor()
            try:
                columns = _read_columns(cursor, owner, table)
                if not columns:
                    raise ValueError(f"Oracle target table metadata not found: {target_table}")
                constraints = _read_unique_constraints(cursor, owner, table)
            finally:
                cursor.close()
        finally:
            conn.close()
        key_columns = _primary_key_columns(constraints)
        key_set = {column.lower() for column in key_columns}
        return TargetTableContract(
            target_type=self.target_type,
            schema=owner,
            table=table,
            columns=tuple(
                TargetColumnContract(
                    **{
                        **column.__dict__,
                        "is_key": column.name.lower() in key_set,
                    }
                )
                for column in columns
            ),
            key_columns=key_columns,
            unique_constraints=constraints,
        )


def _split_oracle_table_name(target_table: str) -> tuple[str | None, str]:
    value = validate_table_identifier(target_table, "write.target.table", max_parts=2)
    parts = value.split(".")
    if len(parts) == 1:
        return None, parts[0].upper()
    return parts[0].upper(), parts[1].upper()


def _read_columns(cursor, owner: str | None, table: str) -> list[TargetColumnContract]:
    if owner:
        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                NULLABLE,
                DATA_PRECISION,
                DATA_SCALE,
                CHAR_LENGTH,
                DATA_LENGTH,
                CHAR_USED,
                DATA_DEFAULT,
                COLUMN_ID
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
            """,
            {"owner": owner, "table_name": table},
        )
    else:
        cursor.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                NULLABLE,
                DATA_PRECISION,
                DATA_SCALE,
                CHAR_LENGTH,
                DATA_LENGTH,
                CHAR_USED,
                DATA_DEFAULT,
                COLUMN_ID
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
            """,
            {"table_name": table},
        )
    return [_column_contract(row) for row in cursor.fetchall()]


def _column_contract(row) -> TargetColumnContract:
    (
        name,
        data_type,
        nullable,
        precision,
        scale,
        char_length,
        data_length,
        char_used,
        default_expr,
        ordinal,
    ) = row
    target_type = str(data_type or "").upper()
    length_semantics = _length_semantics(char_used)
    return TargetColumnContract(
        name=str(name).lower(),
        ordinal=int(ordinal or 0),
        target_type=target_type,
        logical_family=_logical_family(target_type),
        nullable=str(nullable or "Y").upper() == "Y",
        precision=_int_or_none(precision),
        scale=_int_or_none(scale),
        length=_int_or_none(
            data_length if length_semantics == "byte" else char_length if char_length is not None else data_length
        ),
        length_semantics=length_semantics,
        datetime_precision=_datetime_precision(target_type),
        default_expr=str(default_expr).strip() if default_expr is not None else None,
    )


def _read_unique_constraints(cursor, owner: str | None, table: str) -> tuple[TargetUniqueConstraint, ...]:
    if owner:
        cursor.execute(
            """
            SELECT c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, cc.COLUMN_NAME, cc.POSITION
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc
              ON cc.OWNER = c.OWNER
             AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
             AND cc.TABLE_NAME = c.TABLE_NAME
            WHERE c.OWNER = :owner
              AND c.TABLE_NAME = :table_name
              AND c.CONSTRAINT_TYPE IN ('P', 'U')
            ORDER BY c.CONSTRAINT_NAME, cc.POSITION
            """,
            {"owner": owner, "table_name": table},
        )
    else:
        cursor.execute(
            """
            SELECT c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, cc.COLUMN_NAME, cc.POSITION
            FROM USER_CONSTRAINTS c
            JOIN USER_CONS_COLUMNS cc
              ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
             AND cc.TABLE_NAME = c.TABLE_NAME
            WHERE c.TABLE_NAME = :table_name
              AND c.CONSTRAINT_TYPE IN ('P', 'U')
            ORDER BY c.CONSTRAINT_NAME, cc.POSITION
            """,
            {"table_name": table},
        )
    grouped: dict[str, dict[str, object]] = {}
    for constraint_name, constraint_type, column_name, _ in cursor.fetchall():
        item = grouped.setdefault(
            str(constraint_name),
            {"kind": "primary" if constraint_type == "P" else "unique", "columns": []},
        )
        item["columns"].append(str(column_name).lower())
    return tuple(
        TargetUniqueConstraint(
            name=name,
            kind=str(item["kind"]),
            columns=tuple(item["columns"]),
        )
        for name, item in grouped.items()
    )


def _primary_key_columns(constraints: tuple[TargetUniqueConstraint, ...]) -> tuple[str, ...]:
    for constraint in constraints:
        if constraint.kind == "primary":
            return constraint.columns
    return ()


def _logical_family(target_type: str) -> str:
    if target_type in {"NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}:
        return "number"
    if target_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB"}:
        return "string"
    if target_type == "DATE" or target_type.startswith("TIMESTAMP"):
        return "datetime"
    if target_type in {"RAW", "BLOB"}:
        return "binary"
    return "unsupported"


def _length_semantics(char_used) -> str | None:
    value = str(char_used or "").upper()
    if value == "B":
        return "byte"
    if value == "C":
        return "char"
    return None


def _datetime_precision(target_type: str) -> int | None:
    if target_type == "DATE":
        return 0
    if not target_type.startswith("TIMESTAMP"):
        return None
    if "(" not in target_type:
        return 6
    try:
        return int(target_type.split("(", 1)[1].split(")", 1)[0])
    except ValueError:
        return 6


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    return int(value)
