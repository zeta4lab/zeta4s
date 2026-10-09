from __future__ import annotations

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


_TABLE_REF_RE = re.compile(
    r"\b(?:from|join|update|into)\s+([a-zA-Z_][\w$#]*(?:\.[a-zA-Z_][\w$#]*)?)",
    re.IGNORECASE,
)


def _priority_label(text: str) -> str:
    lowered = text.lower()
    urgent_terms = ("urgent", "critical", "outage", "down", "failure", "긴급", "장애")
    high_terms = ("vip", "priority", "escalation", "escalate")
    low_terms = ("low", "minor", "question", "inquiry")
    if any(term in lowered for term in urgent_terms):
        return "high"
    if any(term in lowered for term in high_terms):
        return "high"
    if any(term in lowered for term in low_terms):
        return "low"
    return "medium"


def _parse_sql_tables(sql_text: str) -> dict[str, object]:
    tables: list[str] = []
    seen = set()
    for match in _TABLE_REF_RE.finditer(sql_text):
        table = match.group(1).lower()
        if table not in seen:
            tables.append(table)
            seen.add(table)
    return {"tables": tables}


class ExampleHttpApiHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/healthz"}:
            self._write_json({"status": "ok"})
            return
        if parsed.path == "/classify":
            query = parse_qs(parsed.query)
            description = (query.get("query") or query.get("description") or [""])[0]
            self._write_json(
                {
                    "priority_label": _priority_label(description),
                    "description": description,
                }
            )
            return
        self._write_json({"error": "not_found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/query-parser/api/query":
            try:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
                payload = json.loads(body.decode("utf-8")) if body else {}
            except json.JSONDecodeError:
                self._write_json({"error": "invalid_json"}, status=400)
                return
            query = payload.get("query")
            if not isinstance(query, str):
                self._write_json({"error": "query_required"}, status=400)
                return
            self._write_json({"result": _parse_sql_tables(query)})
            return
        self._write_json({"error": "not_found"}, status=404)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _write_json(self, payload: dict[str, object], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    port = int(os.environ.get("EXAMPLE_HTTP_API_PORT", "8099"))
    server = ThreadingHTTPServer(("0.0.0.0", port), ExampleHttpApiHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
