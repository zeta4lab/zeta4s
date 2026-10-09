from __future__ import annotations

import json
from pathlib import Path
import types
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from zeta4s.runtime.backends.elasticsearch.command import run_elasticsearch_command  # noqa: E402


class ElasticsearchCommandRuntimeTest(unittest.TestCase):
    def test_bulk_posts_ndjson_to_target_index(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            seed = root / "products.bulk.ndjson"
            seed.write_text(
                "\n".join(
                    [
                        '{"index":{"_id":"p-001"}}',
                        '{"product_id":"p-001"}',
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            fake_conn = types.SimpleNamespace(
                base_url="http://elasticsearch:9200", headers={"Authorization": "Basic x"}
            )
            calls = []

            def fake_request(url, body=None, method="POST", content_type="application/json", headers=None, timeout=60):
                calls.append(
                    {
                        "url": url,
                        "body": body,
                        "method": method,
                        "content_type": content_type,
                        "headers": headers,
                    }
                )
                return json.dumps({"errors": False, "items": [{"index": {"status": 201}}]}).encode("utf-8")

            with (
                patch("zeta4s.runtime.backends.elasticsearch.command.elasticsearch_connection", return_value=fake_conn),
                patch("zeta4s.runtime.backends.elasticsearch.command.elasticsearch_request", side_effect=fake_request),
            ):
                result = run_elasticsearch_command(
                    conn_id="elasticsearch_admin",
                    operation="bulk",
                    project_root=str(root),
                    source={"file": "products.bulk.ndjson", "format": "ndjson"},
                    target={"index": "products"},
                    refresh=True,
                    job_id="seed_products",
                    step_id="seed_products",
                )

            self.assertEqual(result["stage"], "elasticsearch_command")
            self.assertEqual(result["metrics"]["output_rows"], 1)
            self.assertEqual(calls[0]["url"], "http://elasticsearch:9200/products/_bulk?refresh=true")
            self.assertEqual(calls[0]["method"], "POST")
            self.assertEqual(calls[0]["content_type"], "application/x-ndjson")
            self.assertEqual(calls[0]["headers"], {"Authorization": "Basic x"})

    def test_reindex_posts_body_to_reindex_api(self) -> None:
        fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})
        calls = []

        def fake_json_request(url, body=None, method="GET", headers=None, timeout=60):
            calls.append({"url": url, "body": body, "method": method, "headers": headers})
            return {"total": 2, "created": 2}

        with (
            patch("zeta4s.runtime.backends.elasticsearch.command.elasticsearch_connection", return_value=fake_conn),
            patch(
                "zeta4s.runtime.backends.elasticsearch.command.elasticsearch_json_request",
                side_effect=fake_json_request,
            ),
        ):
            result = run_elasticsearch_command(
                conn_id="elasticsearch_admin",
                operation="reindex",
                project_root="/tmp",
                body={"source": {"index": "products-v1"}, "dest": {"index": "products-v2"}},
                refresh=True,
                job_id="reindex_products",
                step_id="reindex_products",
            )

        self.assertEqual(result["metrics"]["input_rows"], 2)
        self.assertEqual(result["metrics"]["output_rows"], 2)
        self.assertEqual(calls[0]["url"], "http://elasticsearch:9200/_reindex?refresh=true")
        self.assertEqual(calls[0]["method"], "POST")
        self.assertEqual(calls[0]["body"], {"source": {"index": "products-v1"}, "dest": {"index": "products-v2"}})

    def test_bulk_splits_large_ndjson_by_batch_bytes(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            seed = root / "products.bulk.ndjson"
            seed.write_text(
                "\n".join(
                    [
                        '{"index":{"_id":"p-001"}}',
                        '{"product_id":"p-001"}',
                        '{"index":{"_id":"p-002"}}',
                        '{"product_id":"p-002"}',
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})
            calls = []

            def fake_request(url, body=None, method="POST", content_type="application/json", headers=None, timeout=60):
                calls.append(body)
                return json.dumps({"errors": False, "items": [{"index": {"status": 201}}]}).encode("utf-8")

            with (
                patch("zeta4s.runtime.backends.elasticsearch.command.elasticsearch_connection", return_value=fake_conn),
                patch("zeta4s.runtime.backends.elasticsearch.command.elasticsearch_request", side_effect=fake_request),
            ):
                result = run_elasticsearch_command(
                    conn_id="elasticsearch_admin",
                    operation="bulk",
                    project_root=str(root),
                    source={"file": "products.bulk.ndjson", "format": "ndjson", "batch_bytes": 60},
                    target={"index": "products"},
                )

            self.assertGreater(len(calls), 1)
            self.assertTrue(all(payload.endswith(b"\n") for payload in calls))
            self.assertTrue(all(payload.startswith(b'{"index"') for payload in calls))
            self.assertEqual(
                calls,
                [
                    b'{"index":{"_id":"p-001"}}\n{"product_id":"p-001"}\n',
                    b'{"index":{"_id":"p-002"}}\n{"product_id":"p-002"}\n',
                ],
            )
            self.assertEqual(result["metrics"]["output_rows"], len(calls))
            self.assertEqual(result["details"]["response"]["chunks"], len(calls))
