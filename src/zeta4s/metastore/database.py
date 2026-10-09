"""Metastore database naming and client helpers."""

from __future__ import annotations

from zeta4s.common.sql_identifiers import validate_user_table_name as _validate_user_table_name


def validate_user_table_name(table_name: str, label: str) -> str:
    return _validate_user_table_name(table_name, label)
