"""Local runtime environment helpers for host-side CLI commands."""

from __future__ import annotations

from pathlib import Path


def _dotenv_values(start: Path | None = None) -> dict[str, str]:
    path = (start or Path.cwd()).resolve()
    candidates = [path, *path.parents] if path.is_dir() else [path.parent, *path.parent.parents]
    for directory in candidates:
        env_path = directory / ".env"
        if not env_path.exists():
            continue
        values: dict[str, str] = {}
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            values[key.strip()] = value.strip().strip("'\"")
        return values
    return {}


def is_container_runtime() -> bool:
    return Path("/.dockerenv").exists() or Path("/opt/airflow").exists()


def host_service_endpoint(service: str, default_port: int, start: Path | None = None) -> tuple[str, int]:
    """Return host-reachable endpoint for a zeta4s compose service."""
    if is_container_runtime():
        return service, default_port

    env = _dotenv_values(start)
    host = "127.0.0.1"
    port_by_service = {
        "metastore": int(env.get("METASTORE_HTTP_PORT", env.get("ZETA4S_METASTORE_HTTP_PORT", default_port))),
        "elasticsearch": int(env.get("ELASTICSEARCH_PORT", default_port)),
    }
    return host, port_by_service.get(service, default_port)
