from __future__ import annotations

import sys
import unittest

from zeta4s.runtime.connections import ProfileConnectionResolver, RuntimeConnection, resolve_runtime_connection


class RuntimeConnectionsTest(unittest.TestCase):
    def test_resolve_runtime_connection_uses_explicit_mapping_without_airflow(self):
        airflow_modules = {
            name: module for name, module in sys.modules.items() if name == "airflow" or name.startswith("airflow.")
        }
        for name in airflow_modules:
            sys.modules.pop(name, None)
        try:
            conn = resolve_runtime_connection(
                "analytics",
                connections={
                    "analytics": {
                        "type": "clickhouse",
                        "host": "clickhouse",
                        "port": "8123",
                        "username": "default",
                        "password": "secret",
                        "database": "mart",
                        "extra": {"secure": False},
                    }
                },
            )
        finally:
            sys.modules.update(airflow_modules)

        self.assertEqual(conn.conn_id, "analytics")
        self.assertEqual(conn.conn_type, "clickhouse")
        self.assertEqual(conn.host, "clickhouse")
        self.assertEqual(conn.port, 8123)
        self.assertEqual(conn.login, "default")
        self.assertEqual(conn.password, "secret")
        self.assertEqual(conn.schema, "mart")
        self.assertEqual(conn.extra_dejson, {"secure": False})

    def test_resolve_runtime_connection_preserves_runtime_connection_object(self):
        source = RuntimeConnection(conn_id="oracle_source", conn_type="oracle", host="oracle")

        conn = resolve_runtime_connection("oracle_source", connections={"oracle_source": source})

        self.assertIs(conn, source)

    def test_resolve_runtime_connection_rejects_non_mapping_extra(self):
        with self.assertRaisesRegex(ValueError, "connection extra/options must be a mapping"):
            resolve_runtime_connection("analytics", connections={"analytics": {"extra": "not-json"}})

    def test_resolve_runtime_connection_requires_explicit_mapping(self):
        with self.assertRaisesRegex(KeyError, "runtime connection not provided: analytics"):
            resolve_runtime_connection("analytics")

    def test_profile_connection_resolver_normalizes_profile_connection_contract(self):
        class SecretStore:
            def resolve_secret(self, secret_key: str) -> str:
                self.secret_key = secret_key
                return "resolved-secret"

        secret_store = SecretStore()
        resolver = ProfileConnectionResolver(
            {
                "connections": {
                    "analytics": {
                        "type": "clickhouse",
                        "host": "clickhouse",
                        "port": "8123",
                        "username": "default",
                        "password_ref": "profiles/dev/analytics/password",
                        "database": "mart",
                        "options": {"secure": False},
                    }
                }
            },
            secret_store=secret_store,
        )

        conn = resolver.resolve("analytics")

        self.assertEqual(conn.conn_id, "analytics")
        self.assertEqual(conn.conn_type, "clickhouse")
        self.assertEqual(conn.login, "default")
        self.assertEqual(conn.password, "resolved-secret")
        self.assertEqual(conn.schema, "mart")
        self.assertEqual(conn.extra_dejson, {"secure": False})
        self.assertEqual(secret_store.secret_key, "profiles/dev/analytics/password")

    def test_profile_connection_resolver_rejects_non_mapping_connections(self):
        with self.assertRaisesRegex(ValueError, "profile.connections must be a mapping"):
            ProfileConnectionResolver({"connections": []})


if __name__ == "__main__":
    unittest.main()
