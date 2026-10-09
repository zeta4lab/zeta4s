"""Airflow REST client 계약 검사.

Airflow 3.x SimpleAuthManager 실측 결과를 계약으로 고정한다.

- basic auth 는 401 이다. JWT Bearer 만 통한다.
- JWT 는 `POST /auth/token` 으로 발급하고 수명이 24h 다. 따라서 static token 은
  언젠가 만료되므로 client 가 재발급할 수 있어야 한다.
- 무효/만료 토큰은 401 이 아니라 403 "Invalid JWT token" 이다. 인증 헤더가 없을 때만
  401 이다. 재발급 감지를 401 로만 하면 24h 뒤 조용히 실패하므로 둘 다 신호로 쓴다.
- 만료를 시각으로 예측하지 않으므로 서버 재시작이나 JWT secret 교체처럼 시계로 알 수
  없는 무효화도 같이 처리된다.
"""

from __future__ import annotations

import json
import pathlib
import tempfile
import unittest
import urllib.error
import urllib.parse

from zeta4s.airflow.rest_client import AirflowRestClient, AirflowRestError


class _FakeResponse:
    def __init__(self, payload: bytes, status: int = 200) -> None:
        self._payload = payload
        self.status = status

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _FakeTransport:
    """urlopen 대체. 요청을 기록하고 미리 정한 응답을 돌려준다.

    valid_token 을 정하면 그 토큰이 아닌 요청에 403 "Invalid JWT token" 을 준다.
    실측한 Airflow 동작이다 — 만료/무효 토큰은 401 이 아니라 403 이다.
    """

    def __init__(self, *, valid_token: str | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, str]]] = []
        self.token_issue_count = 0
        self.fail_next_with: int | None = None
        self.valid_token = valid_token

    def __call__(self, request, timeout=None):  # noqa: ANN001
        url = request.full_url
        method = request.get_method()
        headers = {k.lower(): v for k, v in request.headers.items()}
        self.calls.append((method, url, headers))

        if url.endswith("/auth/token"):
            self.token_issue_count += 1
            token = f"issued-token-{self.token_issue_count}"
            self.valid_token = token
            return _FakeResponse(json.dumps({"access_token": token}).encode())

        if self.fail_next_with is not None:
            status = self.fail_next_with
            self.fail_next_with = None
            raise urllib.error.HTTPError(url, status, "denied", {}, None)

        if self.valid_token is not None:
            if headers.get("authorization") != f"Bearer {self.valid_token}":
                raise urllib.error.HTTPError(url, 403, "Invalid JWT token", {}, None)

        return _FakeResponse(json.dumps({"ok": True}).encode())


class AirflowRestClientContractTest(unittest.TestCase):
    def test_from_env_returns_none_without_base_url(self) -> None:
        # base_url 이 없으면 REST 경로가 꺼진 것이다. 빈 client 를 만들지 않는다.
        self.assertIsNone(AirflowRestClient.from_env({}))
        self.assertIsNone(AirflowRestClient.from_env({"ZETA4S_AIRFLOW_REST_API_BASE_URL": "  "}))

    def test_static_token_is_sent_as_bearer(self) -> None:
        transport = _FakeTransport()
        client = AirflowRestClient(base_url="http://airflow:8080", token="static-token", transport=transport)
        client.get("/api/v2/dags")

        _method, url, headers = transport.calls[-1]
        self.assertEqual(url, "http://airflow:8080/api/v2/dags")
        self.assertEqual(headers["authorization"], "Bearer static-token")
        # static token 을 줬으면 발급을 부르지 않는다.
        self.assertEqual(transport.token_issue_count, 0)

    def test_username_password_mints_jwt_instead_of_basic_auth(self) -> None:
        # 실측: SimpleAuthManager 는 basic auth 를 401 로 거부한다.
        transport = _FakeTransport()
        client = AirflowRestClient(
            base_url="http://airflow:8080",
            username="admin",
            password="pw",
            transport=transport,
        )
        client.get("/api/v2/dags")

        token_calls = [c for c in transport.calls if c[1].endswith("/auth/token")]
        self.assertEqual(len(token_calls), 1)
        self.assertEqual(token_calls[0][0], "POST")

        _method, _url, headers = transport.calls[-1]
        self.assertEqual(headers["authorization"], "Bearer issued-token-1")
        self.assertNotIn("Basic", headers["authorization"])

    def test_token_is_reused_across_requests(self) -> None:
        transport = _FakeTransport()
        client = AirflowRestClient(base_url="http://airflow:8080", username="admin", password="pw", transport=transport)
        client.get("/api/v2/dags")
        client.get("/api/v2/pools")
        # 요청마다 발급하면 안 된다.
        self.assertEqual(transport.token_issue_count, 1)

    def test_expired_token_403_triggers_reissue_and_retry(self) -> None:
        # 무효/만료 JWT 는 401 이 아니라 403 "Invalid JWT token" 이다.
        # 401 로만 재발급을 감지하면 24h 뒤 조용히 실패한다.
        transport = _FakeTransport()
        client = AirflowRestClient(base_url="http://airflow:8080", username="admin", password="pw", transport=transport)
        client.get("/api/v2/dags")
        self.assertEqual(transport.token_issue_count, 1)

        # 서버가 토큰을 무효화한 상황 — client 는 아직 옛 토큰을 들고 있다.
        transport.valid_token = "rotated-server-side"
        result = client.get("/api/v2/dags")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(transport.token_issue_count, 2)
        _method, _url, headers = transport.calls[-1]
        self.assertEqual(headers["authorization"], "Bearer issued-token-2")

    def test_401_triggers_reissue_and_single_retry(self) -> None:
        transport = _FakeTransport()
        client = AirflowRestClient(base_url="http://airflow:8080", username="admin", password="pw", transport=transport)
        client.get("/api/v2/dags")

        transport.fail_next_with = 401
        result = client.get("/api/v2/dags")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(transport.token_issue_count, 2)

    def test_denied_without_credentials_does_not_reissue(self) -> None:
        # static token 만 있으면 재발급할 수단이 없다. 재시도하지 않고 실패한다.
        transport = _FakeTransport()
        transport.fail_next_with = 403
        client = AirflowRestClient(base_url="http://airflow:8080", token="static-token", transport=transport)
        with self.assertRaises(AirflowRestError):
            client.get("/api/v2/dags")
        self.assertEqual(transport.token_issue_count, 0)

    def test_real_forbidden_retries_once_then_fails(self) -> None:
        # 진짜 권한 부족이면 재발급해도 다시 403 이다. 한 번만 재시도하고 실패한다.
        calls: list[str] = []

        def always_403(request, timeout=None):  # noqa: ANN001
            url = request.full_url
            calls.append(url)
            if url.endswith("/auth/token"):
                return _FakeResponse(json.dumps({"access_token": "t"}).encode())
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)

        client = AirflowRestClient(
            base_url="http://airflow:8080", username="admin", password="pw", transport=always_403
        )
        with self.assertRaises(AirflowRestError) as ctx:
            client.get("/api/v2/dags")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(len([c for c in calls if c.endswith("/api/v2/dags")]), 2)

    def test_query_uses_repeated_params_and_measured_match_mode(self) -> None:
        # 실측: tags 는 array 라 반복 파라미터이고 tags_match_mode 는 any|all 이다.
        transport = _FakeTransport()
        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        client.get("/api/v2/dags", query={"tags": ["zeta4s", "x"], "tags_match_mode": "any"})

        _method, url, _headers = transport.calls[-1]
        self.assertIn("tags=zeta4s&tags=x", url)
        self.assertIn("tags_match_mode=any", url)

    def test_http_error_becomes_rest_error_with_status(self) -> None:
        def boom(request, timeout=None):  # noqa: ANN001
            raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=boom)
        with self.assertRaises(AirflowRestError) as ctx:
            client.get("/api/v2/dags/missing")
        self.assertEqual(ctx.exception.status, 404)

    def test_transport_failure_becomes_rest_error(self) -> None:
        def boom(request, timeout=None):  # noqa: ANN001
            raise urllib.error.URLError("connection refused")

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=boom)
        with self.assertRaises(AirflowRestError):
            client.get("/api/v2/dags")


class AirflowRestPasswordFileTest(unittest.TestCase):
    """비밀번호를 파일에서 읽는다.

    개발 stack 은 SimpleAuthManager 의 passwords 파일을 그대로 정본으로 쓴다. 값을
    compose 에 박으면 저장소에 자격증명이 늘어나므로 파일을 가리킨다.
    """

    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.root = pathlib.Path(self._dir.name)

    def _env(self, **extra: str) -> dict[str, str]:
        env = {
            "ZETA4S_AIRFLOW_REST_API_BASE_URL": "http://airflow:8080",
            "ZETA4S_AIRFLOW_REST_API_USERNAME": "admin",
        }
        env.update(extra)
        return env

    def test_json_password_file_is_read_by_username(self) -> None:
        # SimpleAuthManager 파일 형식이다: {"admin": "pw"}
        path = self.root / "passwords.json"
        path.write_text(json.dumps({"admin": "pw-from-json", "other": "nope"}))

        transport = _FakeTransport()
        client = AirflowRestClient.from_env(
            self._env(ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE=str(path)), transport=transport
        )
        client.get("/api/v2/dags")

        token_call = [c for c in transport.calls if c[1].endswith("/auth/token")]
        self.assertEqual(len(token_call), 1)

    def test_plain_password_file_is_supported(self) -> None:
        path = self.root / "password.txt"
        path.write_text("  pw-plain\n")

        client = AirflowRestClient.from_env(
            self._env(ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE=str(path)), transport=_FakeTransport()
        )
        self.assertEqual(client._password, "pw-plain")

    def test_explicit_password_wins_over_file(self) -> None:
        path = self.root / "passwords.json"
        path.write_text(json.dumps({"admin": "from-file"}))

        client = AirflowRestClient.from_env(
            self._env(
                ZETA4S_AIRFLOW_REST_API_PASSWORD="from-env",
                ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE=str(path),
            ),
            transport=_FakeTransport(),
        )
        self.assertEqual(client._password, "from-env")

    def test_missing_file_does_not_crash_client_construction(self) -> None:
        # 파일이 없으면 REST 를 못 쓰지만 client 생성 자체가 터지면 안 된다.
        # 실패는 인증을 실제로 시도할 때 드러난다.
        client = AirflowRestClient.from_env(
            self._env(ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE=str(self.root / "absent.json")),
            transport=_FakeTransport(),
        )
        self.assertIsNotNone(client)
        self.assertIsNone(client._password)

    def test_json_file_without_matching_username_yields_no_password(self) -> None:
        path = self.root / "passwords.json"
        path.write_text(json.dumps({"someone-else": "pw"}))

        client = AirflowRestClient.from_env(
            self._env(ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE=str(path)), transport=_FakeTransport()
        )
        self.assertIsNone(client._password)


class AirflowRestPaginationTest(unittest.TestCase):
    """collect 는 total_entries 에 도달할 때까지 페이지를 모은다.

    metastore 쿼리는 전부 돌려주지만 REST 는 limit 기본 50 이다. 페이징 없이 옮기면
    50건을 넘는 순간 조용히 일부만 반환한다.
    """

    def _paged_transport(self, total: int, *, page_size: int = 50):
        def transport(request, timeout=None):  # noqa: ANN001
            url = request.full_url
            query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", [str(page_size)])[0])
            items = [{"dag_id": f"dag_{i}"} for i in range(offset, min(offset + limit, total))]
            return _FakeResponse(json.dumps({"dags": items, "total_entries": total}).encode())

        return transport

    def test_collect_gathers_every_page(self) -> None:
        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=self._paged_transport(127))
        items = client.collect("/api/v2/dags", items_key="dags")
        self.assertEqual(len(items), 127)
        self.assertEqual(items[0]["dag_id"], "dag_0")
        self.assertEqual(items[-1]["dag_id"], "dag_126")

    def test_collect_single_page(self) -> None:
        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=self._paged_transport(3))
        self.assertEqual(len(client.collect("/api/v2/dags", items_key="dags")), 3)

    def test_collect_empty(self) -> None:
        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=self._paged_transport(0))
        self.assertEqual(client.collect("/api/v2/dags", items_key="dags"), [])

    def test_collect_stops_when_page_returns_nothing(self) -> None:
        # total_entries 가 실제보다 크게 와도 무한루프에 빠지지 않는다.
        def liar(request, timeout=None):  # noqa: ANN001
            return _FakeResponse(json.dumps({"dags": [], "total_entries": 999}).encode())

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=liar)
        self.assertEqual(client.collect("/api/v2/dags", items_key="dags"), [])

    def test_collect_preserves_caller_query(self) -> None:
        seen: list[str] = []

        def transport(request, timeout=None):  # noqa: ANN001
            seen.append(request.full_url)
            return _FakeResponse(json.dumps({"dags": [], "total_entries": 0}).encode())

        client = AirflowRestClient(base_url="http://airflow:8080", token="t", transport=transport)
        client.collect("/api/v2/dags", items_key="dags", query={"tags": ["zeta4s"], "exclude_stale": False})
        self.assertIn("tags=zeta4s", seen[0])
        self.assertIn("exclude_stale=false", seen[0])


if __name__ == "__main__":
    unittest.main()
