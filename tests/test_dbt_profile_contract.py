from __future__ import annotations

import json
import unittest

import yaml

from zeta4s.runtime.dbt_profiles import (
    dbt_profile_connection_from_asset,
    dbt_profile_connection_from_profile,
    render_dbt_profiles_yml,
)
from zeta4s.runtime.connection_policy import profile_connection_payloads


class DbtProfileContractTest(unittest.TestCase):
    def test_clickhouse_profile_uses_asset_database(self) -> None:
        connection = dbt_profile_connection_from_asset(
            "retail_clickhouse",
            {
                "conn_type": "clickhouse",
                "host": "metastore",
                "port": 8123,
                "login": "metastore",
                "password": "metastore_pwd",
                "extra": {"database": "default"},
            },
        )

        profile = yaml.safe_load(render_dbt_profiles_yml(connection))
        output = profile["retail_clickhouse"]["outputs"]["runtime"]

        self.assertEqual(output["type"], "clickhouse")
        self.assertEqual(output["host"], "metastore")
        self.assertEqual(output["user"], "metastore")
        self.assertEqual(output["schema"], "default")

    def test_oracle_profile_uses_asset_service_and_schema(self) -> None:
        connection = dbt_profile_connection_from_asset(
            "retail_oracle",
            {
                "conn_type": "oracle",
                "host": "oracle",
                "port": 1521,
                "login": "showcase_src",
                "password": "showcase_src",
                "schema": "FREEPDB1",
                "extra": {"service_name": "FREEPDB1", "schema": "SHOWCASE_SRC"},
            },
        )

        profile = yaml.safe_load(render_dbt_profiles_yml(connection))
        output = profile["retail_oracle"]["outputs"]["runtime"]

        self.assertEqual(output["type"], "oracle")
        self.assertEqual(output["protocol"], "tcp")
        self.assertEqual(output["host"], "oracle")
        self.assertEqual(output["port"], 1521)
        self.assertEqual(output["database"], "FREEPDB1")
        self.assertEqual(output["service"], "FREEPDB1")
        self.assertEqual(output["schema"], "SHOWCASE_SRC")

    def test_clickhouse_profile_uses_workspace_profile_contract(self) -> None:
        connection = dbt_profile_connection_from_profile(
            "analytics_clickhouse",
            {
                "type": "clickhouse",
                "host": "clickhouse",
                "port": 8123,
                "username": "metastore",
                "password_ref": "dev.analytics_clickhouse.password",
                "database": "default",
            },
        )

        profile = yaml.safe_load(render_dbt_profiles_yml(connection))
        output = profile["analytics_clickhouse"]["outputs"]["runtime"]

        self.assertEqual(output["type"], "clickhouse")
        self.assertEqual(output["host"], "clickhouse")
        self.assertEqual(output["user"], "metastore")
        self.assertEqual(output["schema"], "default")
        self.assertEqual(output["password"], "")

    def test_profile_connection_payload_preserves_database_extra(self) -> None:
        profile = {
            "connections": {
                "analytics_clickhouse": {
                    "type": "clickhouse",
                    "host": "clickhouse",
                    "port": 8123,
                    "username": "metastore",
                    "password_ref": "dev.analytics_clickhouse.password",
                    "database": "analytics",
                }
            }
        }

        payloads = dict(profile_connection_payloads(profile))

        payload = payloads["analytics_clickhouse"]
        extra = json.loads(payload["extra"])
        self.assertEqual(payload["schema"], "analytics")
        self.assertEqual(extra["database"], "analytics")
        self.assertEqual(extra["password_ref"], "dev.analytics_clickhouse.password")


if __name__ == "__main__":
    unittest.main()
