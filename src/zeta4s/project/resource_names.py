"""zeta4s 가 관리하는 resource 이름 helper."""

from __future__ import annotations

from zeta4s.project.loader import validate_project_id


def encode_project_resource_name(project_name: str) -> str:
    """Encode a project name for ClickHouse database and Airflow pool names."""
    project = validate_project_id(project_name)
    parts: list[str] = []
    for char in project:
        if char == "_":
            parts.append("__")
        elif char == "-":
            parts.append("_h")
        elif char == ".":
            parts.append("_d")
        elif "A" <= char <= "Z":
            parts.append("_u" + char.lower())
        else:
            parts.append(char)
    return "".join(parts)
