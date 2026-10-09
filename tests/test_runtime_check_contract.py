from __future__ import annotations

import unittest
from unittest.mock import patch

from zeta4s.airflow.runtime_check import _resolve_connection, check_profile_api


class RuntimeCheckContractTest(unittest.TestCase):
    def test_probe_reads_connection_from_profile_not_airflow(self) -> None:
        """probe 의 접속 정보 출처는 profile 이다.

        Airflow 에 되물으면 password 가 `"***"` 로 마스킹돼 붙을 수 없고, zeta4s-api 가
        airflow 에 다시 결합된다. profile 에 없는 conn_id 는 폴백 없이 실패한다.
        """
        policy = {
            "conn_id": "analytics_backend",
            "conn_type": "clickhouse",
            "host": "clickhouse",
            "port": 8123,
            "login": "metastore",
            "extra": {"database": "analytics"},
        }

        conn = _resolve_connection("analytics_backend", [policy])
        self.assertEqual(conn.conn_id, "analytics_backend")
        self.assertEqual(conn.host, "clickhouse")

        with self.assertRaises(ValueError) as ctx:
            _resolve_connection("absent_backend", [policy])
        self.assertIn("absent_backend", str(ctx.exception))

    def test_profile_check_auto_retries_clickhouse_secure_candidate(self) -> None:
        calls: list[bool] = []

        class Client:
            def command(self, query):
                return 1

        def fake_get_client(**kwargs):
            calls.append(kwargs["secure"])
            if not kwargs["secure"]:
                raise RuntimeError("plain failed")
            return Client()

        policy = {
            "conn_id": "analytics_clickhouse",
            "conn_type": "clickhouse",
            "host": "clickhouse.example.com",
            "port": 8123,
            "login": "metastore",
            "extra": {"database": "analytics"},
        }

        with patch("zeta4s.airflow.runtime_check.clickhouse_connect.get_client", side_effect=fake_get_client):
            checks = check_profile_api([policy])

        self.assertEqual(calls, [False, True])
        self.assertEqual(checks[0].ok, True)
        self.assertEqual(checks[0].detail, "SELECT 1 (https)")

    def test_profile_check_tries_https_first_for_tls_http_ports(self) -> None:
        urls: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return b"{}"

        def fake_urlopen(request, timeout):
            urls.append(request.full_url)
            return Response()

        policy = {
            "conn_id": "search",
            "conn_type": "elasticsearch",
            "host": "elastic.example.com",
            "port": 9243,
        }

        with patch("zeta4s.airflow.runtime_check.urllib.request.urlopen", side_effect=fake_urlopen):
            checks = check_profile_api([policy])

        self.assertEqual(urls, ["https://elastic.example.com:9243/"])
        self.assertEqual(checks[0].ok, True)
        self.assertEqual(checks[0].detail, "GET / (https)")

    def test_profile_check_honors_explicit_https_type(self) -> None:
        urls: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return b"{}"

        def fake_urlopen(request, timeout):
            urls.append(request.full_url)
            if request.full_url.startswith("http://"):
                return Response()
            return Response()

        policy = {
            "conn_id": "secure_api",
            "conn_type": "https",
            "host": "api.example.com",
        }

        with patch("zeta4s.airflow.runtime_check.urllib.request.urlopen", side_effect=fake_urlopen):
            checks = check_profile_api([policy])

        self.assertEqual(urls, ["https://api.example.com/"])
        self.assertEqual(checks[0].ok, True)
        self.assertEqual(checks[0].detail, "GET / (https)")

    def test_profile_check_honors_explicit_http_type(self) -> None:
        urls: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return b"{}"

        def fake_urlopen(request, timeout):
            urls.append(request.full_url)
            return Response()

        policy = {
            "conn_id": "plain_api",
            "conn_type": "http",
            "host": "api.example.com",
            "port": 443,
        }

        with patch("zeta4s.airflow.runtime_check.urllib.request.urlopen", side_effect=fake_urlopen):
            checks = check_profile_api([policy])

        self.assertEqual(urls, ["http://api.example.com:443/"])
        self.assertEqual(checks[0].ok, True)
        self.assertEqual(checks[0].detail, "GET / (http)")


if __name__ == "__main__":
    unittest.main()
