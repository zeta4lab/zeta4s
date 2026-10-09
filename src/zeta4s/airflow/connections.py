"""Airflow Connection lookup helpers for task execution context."""

from __future__ import annotations


class AirflowConnectionResolver:
    def resolve(self, conn_id: str):
        return get_airflow_connection(conn_id)


def get_airflow_connection(conn_id: str):
    """Return an Airflow Connection inside task execution context.

    Airflow 3 SDK `BaseHook.get_connection()` resolves through the secrets
    backend chain, which reaches zeta4s-api. This module runs in the worker via
    `operators.py`; zeta4s-api reads connection policies from the profile
    instead, so there is no API-process path here and no metastore fallback.
    """
    from airflow.sdk.bases.hook import BaseHook

    return _hydrate_password_ref(BaseHook.get_connection(conn_id))


def _hydrate_password_ref(connection):
    if connection is None or getattr(connection, "password", None):
        return connection
    extra = getattr(connection, "extra_dejson", None) or {}
    if not isinstance(extra, dict):
        return connection
    ref = extra.get("password_ref")
    if not ref:
        return connection
    from zeta4s.runtime.secrets import EncryptedSecretStore

    connection.password = EncryptedSecretStore().resolve_secret(str(ref))
    return connection
