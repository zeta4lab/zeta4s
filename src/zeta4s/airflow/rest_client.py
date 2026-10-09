"""Airflow REST API client.

zeta4s 가 Airflow 를 외부 engine 으로 다루는 유일한 접점이다. metastore 직접 접근과
CLI subprocess 를 대체하므로 표준 라이브러리만 쓰고 airflow 를 import 하지 않는다.

인증은 Airflow 3.x SimpleAuthManager 실측에 맞춘다. basic auth 는 401 로 거부되므로 쓰지
않고 `POST /auth/token` 으로 JWT 를 발급받아 Bearer 로 보낸다. JWT 수명은 24h 라 static
token 은 언젠가 만료된다.

만료를 시각으로 예측하지 않고 응답 status 로 감지해 재발급 후 한 번 재시도한다. 서버
재시작이나 secret 교체처럼 시계로 알 수 없는 무효화도 같은 경로로 처리된다. 감지 조건이
401 과 403 둘 다인 이유는 실측 때문이다 — 인증 헤더가 아예 없으면 401 이지만 토큰이
있는데 무효/만료면 403 "Invalid JWT token" 이 온다.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Callable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_TIMEOUT_SECONDS = 10.0
# REST 기본 limit 은 50 이다. 왕복을 줄이되 상한(실측상 1000 은 허용)에 붙지 않게 잡는다.
DEFAULT_PAGE_SIZE = 100

Transport = Callable[..., Any]


class AirflowRestError(RuntimeError):
    """Airflow REST 호출 실패. status 가 없으면 전송 자체가 실패한 것이다."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class AirflowRestTimeout(AirflowRestError):
    """전송이 timeout 됐다.

    호출부가 timeout 을 다른 전송 실패와 다르게 보고한다 — Airflow 가 느린 것과 붙을 수
    없는 것은 다른 상황이다. 둘을 뭉치면 그 구분이 사라진다.
    """


class AirflowRestClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        transport: Transport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._timeout = timeout
        self._transport = transport or urllib.request.urlopen
        self._token = token

    @classmethod
    def from_env(cls, env: Mapping[str, str], **kwargs: Any) -> "AirflowRestClient | None":
        """base_url 이 없으면 REST 경로가 꺼진 것이므로 client 를 만들지 않는다."""
        raw = env.get("ZETA4S_AIRFLOW_REST_API_BASE_URL")
        if not raw or not raw.strip():
            return None
        timeout_raw = env.get("ZETA4S_AIRFLOW_REST_API_TIMEOUT_SECONDS")
        timeout = DEFAULT_TIMEOUT_SECONDS
        if timeout_raw and timeout_raw.strip():
            try:
                timeout = float(timeout_raw)
            except ValueError:
                timeout = DEFAULT_TIMEOUT_SECONDS
        username = _clean(env.get("ZETA4S_AIRFLOW_REST_API_USERNAME"))
        password = _clean(env.get("ZETA4S_AIRFLOW_REST_API_PASSWORD"))
        if password is None:
            password = _password_from_file(env.get("ZETA4S_AIRFLOW_REST_API_PASSWORD_FILE"), username)
        return cls(
            base_url=raw.strip(),
            token=_clean(env.get("ZETA4S_AIRFLOW_REST_API_TOKEN")),
            username=username,
            password=password,
            timeout=timeout,
            **kwargs,
        )

    def get(self, path: str, *, query: Mapping[str, Any] | None = None) -> Any:
        return self.request("GET", path, query=query)

    def post(self, path: str, *, body: Any = None, query: Mapping[str, Any] | None = None) -> Any:
        return self.request("POST", path, body=body, query=query)

    def patch(self, path: str, *, body: Any = None, query: Mapping[str, Any] | None = None) -> Any:
        return self.request("PATCH", path, body=body, query=query)

    def delete(self, path: str, *, query: Mapping[str, Any] | None = None) -> Any:
        return self.request("DELETE", path, query=query)

    def collect(
        self,
        path: str,
        *,
        items_key: str,
        query: Mapping[str, Any] | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> list[Any]:
        """페이지를 모두 모아 돌려준다.

        metastore 쿼리는 전부 돌려주지만 REST 는 limit 기본 50 이다. 조회 경로를 그대로
        옮기면 50건을 넘는 순간 조용히 일부만 반환하므로 total_entries 까지 모은다.
        """

        def fetch(limit: int, offset: int) -> Any:
            page_query = dict(query or {})
            page_query["limit"] = limit
            page_query["offset"] = offset
            return self.get(path, query=page_query)

        return self._paginate(fetch, items_key, page_size)

    def collect_batch(
        self,
        path: str,
        *,
        items_key: str,
        body: Mapping[str, Any] | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> list[Any]:
        """batch endpoint 를 모두 모아 돌려준다.

        `.../list` 는 POST body 로 페이징한다. 이름이 `page_limit`/`page_offset` 이고
        query 의 `limit`/`offset` 이 아니다. 섞어 쓰면 조용히 첫 페이지만 돌아온다.
        """

        def fetch(limit: int, offset: int) -> Any:
            page_body = dict(body or {})
            page_body["page_limit"] = limit
            page_body["page_offset"] = offset
            return self.post(path, body=page_body)

        return self._paginate(fetch, items_key, page_size)

    def _paginate(self, fetch: Callable[[int, int], Any], items_key: str, page_size: int) -> list[Any]:
        collected: list[Any] = []
        offset = 0
        while True:
            payload = fetch(page_size, offset)
            items = (payload or {}).get(items_key) or []
            collected.extend(items)
            total = (payload or {}).get("total_entries")
            # 빈 페이지는 종료 조건이다. total_entries 가 실제와 어긋나도 멈춘다.
            if not items:
                return collected
            if not isinstance(total, int) or len(collected) >= total:
                return collected
            offset += len(items)

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        query: Mapping[str, Any] | None = None,
    ) -> Any:
        url = self._url(path, query)
        try:
            return self._send(method, url, body)
        except AirflowRestError as error:
            # 실측: 인증 헤더가 없으면 401 이지만 토큰이 무효/만료면 403 "Invalid JWT token"
            # 이다. 만료 재발급을 401 로만 감지하면 24h 뒤 조용히 실패한다. 403 이 진짜
            # 권한 부족이면 재발급 후에도 다시 403 이므로 한 번의 재시도로 끝난다.
            if error.status not in (401, 403) or not self._can_issue_token():
                raise
            self._token = None
            return self._send(method, url, body)

    def _send(self, method: str, url: str, body: Any) -> Any:
        request = urllib.request.Request(
            url,
            data=None if body is None else json.dumps(body).encode("utf-8"),
            headers=self._headers(),
            method=method,
        )
        return _read_json(self._call(request))

    def _call(self, request: urllib.request.Request) -> bytes:
        try:
            with self._transport(request, timeout=self._timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            detail = _error_detail(error)
            raise AirflowRestError(
                f"Airflow REST {request.get_method()} {request.full_url} failed with {error.code}{detail}",
                status=error.code,
            ) from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            message = f"Airflow REST {request.get_method()} {request.full_url} failed: {error}"
            if _is_timeout(error):
                raise AirflowRestTimeout(message) from error
            raise AirflowRestError(message) from error

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        token = self._access_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _access_token(self) -> str | None:
        if self._token:
            return self._token
        if not self._can_issue_token():
            return None
        self._token = self._issue_token()
        return self._token

    def _can_issue_token(self) -> bool:
        return bool(self._username)

    def _issue_token(self) -> str:
        """SimpleAuthManager 는 basic auth 를 받지 않으므로 JWT 를 발급받는다."""
        request = urllib.request.Request(
            f"{self._base_url}/auth/token",
            data=json.dumps({"username": self._username, "password": self._password or ""}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        payload = _read_json(self._call(request))
        token = (payload or {}).get("access_token") if isinstance(payload, dict) else None
        if not token:
            raise AirflowRestError("Airflow REST token issue response has no access_token")
        return str(token)

    def _url(self, path: str, query: Mapping[str, Any] | None) -> str:
        url = f"{self._base_url}/{path.lstrip('/')}"
        params = _query_pairs(query)
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        return url


def _query_pairs(query: Mapping[str, Any] | None) -> list[tuple[str, str]]:
    """array 파라미터는 반복 파라미터로 보낸다 — Airflow 의 tags 가 그렇다."""
    if not query:
        return []
    pairs: list[tuple[str, str]] = []
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)) or (
            isinstance(value, Sequence) and not isinstance(value, (str, bytes))
        ):
            pairs.extend((key, _scalar(item)) for item in value)
        else:
            pairs.append((key, _scalar(value)))
    return pairs


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _read_json(payload: bytes) -> Any:
    if not payload:
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def _is_timeout(error: Exception) -> bool:
    """urllib 은 timeout 을 그대로 내기도 하고 `URLError` 로 감싸기도 한다.

    감싼 쪽을 놓치면 timeout 이 조용히 일반 전송 실패로 강등된다. Python 3.10 부터
    `socket.timeout` 은 `TimeoutError` 의 별칭이므로 한 번만 검사하면 된다.
    """
    if isinstance(error, TimeoutError):
        return True
    return isinstance(getattr(error, "reason", None), TimeoutError)


def require_rest_client(env: Mapping[str, str] | None = None) -> AirflowRestClient:
    """REST 가 정본인 경로가 쓴다. 배선이 없으면 조용히 새지 않고 실패한다."""
    client = AirflowRestClient.from_env(os.environ if env is None else env)
    if client is None:
        raise RuntimeError(
            "Airflow REST API is not configured: set ZETA4S_AIRFLOW_REST_API_BASE_URL",
        )
    return client


def _error_detail(error: urllib.error.HTTPError) -> str:
    try:
        body = error.read().decode("utf-8", errors="replace").strip()
    except Exception:  # noqa: BLE001 - 본문 없는 오류도 그대로 전달한다
        return ""
    return f": {body[:500]}" if body else ""


def _password_from_file(path: str | None, username: str | None) -> str | None:
    """비밀번호를 파일에서 읽는다. 값을 배포 파일에 박지 않기 위한 경로다.

    SimpleAuthManager 의 passwords 파일은 {"admin": "pw"} 형식이라 username 으로 고른다.
    평문 한 줄짜리 파일도 받는다. 파일이 없거나 읽을 수 없으면 None 이다 — client 생성은
    성공하고 실패는 인증을 실제로 시도할 때 드러난다.
    """
    if not path or not path.strip():
        return None
    try:
        raw = pathlib.Path(path.strip()).read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return _clean(raw)
    if isinstance(payload, dict):
        if not username:
            return None
        return _clean(payload.get(username))
    return None


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
