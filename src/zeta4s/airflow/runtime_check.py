"""Runtime connectivity probes for backend connections used by project steps."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import clickhouse_connect
import oracledb

from zeta4s.airflow.local_runtime import host_service_endpoint, is_container_runtime
from zeta4s.runtime.connection_policy import airflow_connection_projection, password_ref
from zeta4s.runtime.secrets import EncryptedSecretStore


@dataclass(frozen=True)
class RuntimeCheck:
    conn_id: str
    kind: str
    ok: bool
    detail: str


class RuntimeConnection:
    def __init__(self, payload: dict[str, Any]):
        self.conn_id = str(payload.get("conn_id") or "")
        self.conn_type = str(payload.get("conn_type") or "")
        self.host = payload.get("host")
        self.login = payload.get("login")
        self.password = payload.get("password")
        self.schema = payload.get("schema")
        self.description = payload.get("description")
        self.port = int(payload["port"]) if payload.get("port") is not None else None
        extra = payload.get("extra")
        self.extra = json.dumps(extra, ensure_ascii=False, sort_keys=True) if isinstance(extra, dict) else (extra or "")

    @property
    def extra_dejson(self) -> dict[str, Any]:
        if not self.extra:
            return {}
        try:
            data = json.loads(self.extra)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}


def _resolve_connection(conn_id: str, connection_policies: list[dict[str, Any]]) -> RuntimeConnection:
    """probe 대상 접속 정보를 profile 에서 직접 읽는다.

    Airflow 에 되물을 이유가 없다. probe 가 확인하려는 것은 profile 이 가리키는 backend 에
    붙는지이고, 그 정보의 출처는 profile 자신이다. `GET /api/v2/connections/{conn_id}` 는
    password 를 `"***"` 로 마스킹하므로 REST 도 대체재가 되지 못한다.
    """
    policy = _candidate_connection_policy(conn_id, connection_policies)
    if policy is None:
        raise ValueError(f"runtime connection is not defined in profile: {conn_id}")
    return _connection_from_policy(policy)


def _candidate_connection_policy(
    conn_id: str, connection_policies: list[dict[str, Any]] | None
) -> dict[str, Any] | None:
    matches = [
        policy
        for policy in connection_policies or []
        if isinstance(policy, dict) and str(policy.get("conn_id") or "") == conn_id
    ]
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(f"runtime connection is ambiguous in candidate profile: {conn_id}")
    return dict(matches[0])


def _connection_from_policy(policy: dict[str, Any]):
    ref = password_ref(policy)
    password = EncryptedSecretStore().resolve_secret(ref) if ref else None
    return RuntimeConnection(airflow_connection_projection(policy, password=password))


def _adapt_local_connection(conn, kind: str):
    if is_container_runtime():
        return conn
    host = conn.host
    service_aliases = {
        "clickhouse": {"metastore"},
        "elasticsearch": {"elasticsearch"},
    }
    if host not in service_aliases.get(kind, set()):
        return conn

    default_ports = {
        "clickhouse": 8123,
        "elasticsearch": 9200,
    }
    local_host, local_port = host_service_endpoint(host, default_ports[kind])
    conn.host = local_host
    conn.port = local_port
    return conn


def _clickhouse_secure_candidates(port: int | None) -> list[bool]:
    if port in {443, 8443, 9440}:
        return [True, False]
    return [False, True]


def _probe_clickhouse_profile(conn: RuntimeConnection) -> str:
    errors: list[str] = []
    for secure in _clickhouse_secure_candidates(conn.port):
        try:
            clickhouse_connect.get_client(
                host=conn.host,
                port=conn.port,
                username=conn.login,
                password=conn.password,
                database=conn.extra_dejson.get("database", "default"),
                secure=secure,
            ).command("SELECT 1")
        except Exception as e:  # noqa: BLE001 - try the alternate transport before failing
            errors.append(f"secure={secure}: {e}")
            continue
        scheme = "https" if secure else "http"
        return f"SELECT 1 ({scheme})"
    raise RuntimeError("; ".join(errors))


def _oracle_dsn_from_connection(conn, conn_id: str) -> str:
    extra = conn.extra_dejson or {}
    if extra.get("dsn"):
        return extra["dsn"]

    host = conn.host
    port = conn.port
    service_name = extra.get("service_name") or getattr(conn, "schema", None)
    sid = extra.get("sid")
    if not host or not port:
        raise ValueError(f"Oracle Airflow connection {conn_id!r} requires host and port unless extra.dsn is set.")
    if service_name and sid:
        raise ValueError(f"Oracle Airflow connection {conn_id!r} must set only one of extra.service_name or extra.sid.")
    if service_name:
        return oracledb.makedsn(host, port, service_name=service_name)
    if sid:
        return oracledb.makedsn(host, port, sid=sid)
    raise ValueError(
        f"Oracle Airflow connection {conn_id!r} requires one of extra.dsn, extra.service_name, extra.sid, or schema."
    )


def _probe_oracle(conn_id: str, conn_policy: RuntimeConnection) -> str:
    dsn = _oracle_dsn_from_connection(conn_policy, conn_id)
    conn = oracledb.connect(user=conn_policy.login, password=conn_policy.password, dsn=dsn)
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()
    return "SELECT 1 FROM dual"


def _base_url_candidates(conn: RuntimeConnection, *, label: str, explicit_scheme: str | None = None) -> list[str]:
    host = conn.host
    if not host:
        raise ValueError(f"{label} connection host 가 비어 있다: conn_id={conn.conn_id!r}")
    if host.startswith(("http://", "https://")):
        return [host.rstrip("/")]
    if explicit_scheme:
        port = f":{conn.port}" if conn.port else ""
        return [f"{explicit_scheme}://{host}{port}".rstrip("/")]
    if conn.schema in {"http", "https"}:
        port = f":{conn.port}" if conn.port else ""
        return [f"{conn.schema}://{host}{port}".rstrip("/")]
    schemes = ["https", "http"] if conn.port in {443, 8443, 9243} else ["http", "https"]
    port = f":{conn.port}" if conn.port else ""
    return [f"{scheme}://{host}{port}".rstrip("/") for scheme in schemes]


def _basic_auth_headers(conn: RuntimeConnection) -> dict[str, str]:
    if not conn.login:
        return {}
    password = conn.password or ""
    credentials = f"{conn.login}:{password}".encode("utf-8")
    token = base64.b64encode(credentials).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def _probe_elasticsearch_profile(conn: RuntimeConnection) -> str:
    headers = _basic_auth_headers(conn)
    errors: list[str] = []
    for base_url in _base_url_candidates(conn, label="Elasticsearch"):
        req = urllib.request.Request(base_url + "/", method="GET", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                response.read()
        except Exception as e:  # noqa: BLE001 - try the alternate scheme before failing
            errors.append(f"{base_url}: {e}")
            continue
        return f"GET / ({urllib.parse.urlparse(base_url).scheme})"
    raise RuntimeError("; ".join(errors))


def _probe_http_profile(conn: RuntimeConnection, *, explicit_scheme: str | None = None) -> str:
    headers = {"Content-Type": "application/json"}
    extra = conn.extra_dejson or {}
    extra_headers = extra.get("headers")
    if isinstance(extra_headers, dict):
        headers.update({str(key): str(value) for key, value in extra_headers.items()})
    bearer_token = extra.get("bearer_token")
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    else:
        headers.update(_basic_auth_headers(conn))
    errors: list[str] = []
    for base_url in _base_url_candidates(conn, label="http.lookup", explicit_scheme=explicit_scheme):
        req = urllib.request.Request(base_url + "/", method="GET", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                response.read()
        except urllib.error.HTTPError as e:
            if 100 <= e.code < 500:
                return f"GET / returned HTTP {e.code} ({urllib.parse.urlparse(base_url).scheme})"
            errors.append(f"{base_url}: {e}")
            continue
        except Exception as e:  # noqa: BLE001 - try the alternate scheme before failing
            errors.append(f"{base_url}: {e}")
            continue
        return f"GET / ({urllib.parse.urlparse(base_url).scheme})"
    raise RuntimeError("; ".join(errors))


def _probe_profile(conn_id: str, kind: str, connection_policies: list[dict[str, Any]]) -> str:
    conn = _adapt_local_connection(_resolve_connection(conn_id, connection_policies), kind)
    if kind == "clickhouse":
        return _probe_clickhouse_profile(conn)
    if kind == "oracle":
        return _probe_oracle(conn_id, conn)
    if kind == "elasticsearch":
        return _probe_elasticsearch_profile(conn)
    if kind in {"http", "https"}:
        return _probe_http_profile(conn, explicit_scheme=kind)
    raise ValueError(f"unsupported profile connection type: {kind}")


def check_profile_api(connection_policies: list[dict[str, Any]]) -> list[RuntimeCheck]:
    checks: list[RuntimeCheck] = []
    for policy in sorted(connection_policies, key=lambda item: str(item.get("conn_id") or "")):
        conn_id = str(policy.get("conn_id") or "")
        kind = str(policy.get("conn_type") or "")
        try:
            if kind not in {"clickhouse", "oracle", "elasticsearch", "http", "https"}:
                raise ValueError(f"unsupported profile connection type: {kind}")
            detail = _probe_profile(conn_id, kind, connection_policies)
        except Exception as e:  # noqa: BLE001 - collect every profile connection failure in one report
            checks.append(RuntimeCheck(conn_id=conn_id, kind=kind, ok=False, detail=str(e)))
        else:
            checks.append(RuntimeCheck(conn_id=conn_id, kind=kind, ok=True, detail=detail))
    return checks
