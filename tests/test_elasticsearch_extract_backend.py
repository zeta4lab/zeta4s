from __future__ import annotations

from datetime import datetime
import types
import unittest
from unittest.mock import patch

from zeta4s.runtime.types import arrow_type_from_column_spec
from zeta4s.runtime.backends.elasticsearch.extract import ElasticsearchSearchReader, open_elasticsearch_rowset_reader


class ElasticsearchExtractBackendTest(unittest.TestCase):
    def test_open_rowset_reader_builds_elasticsearch_reader(self) -> None:
        fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})

        with patch("zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_connection", return_value=fake_conn):
            reader = open_elasticsearch_rowset_reader(
                source_conn="elasticsearch_source",
                source={
                    "index": "products",
                    "query": {"term": {"status": "active"}},
                    "fields": [
                        {"column": "product_id", "path": "product.id", "type": "str"},
                        {"column": "updated_at", "path": "updated_at", "type": "timestamp", "datetime_precision": 6},
                    ],
                    "sort": [{"updated_at": "asc"}],
                    "track_total_hits": True,
                },
                watermark=None,
                time_window=None,
                batch_size=500,
                loaded_at=datetime(2026, 1, 1),
                metadata_name="products",
                job_id="products",
                step_id="extract_products",
                output_name="products_rows",
                context={},
                kwargs={},
            )

        self.assertIsInstance(reader, ElasticsearchSearchReader)
        self.assertEqual(reader.index, "products")
        self.assertEqual(reader.query, {"term": {"status": "active"}})
        self.assertEqual(reader.sort, [{"updated_at": "asc"}, {"_shard_doc": "asc"}])
        self.assertEqual(reader.batch_size, 500)
        self.assertTrue(reader.track_total_hits)
        self.assertEqual(reader.columns, ["product_id", "updated_at"])
        self.assertEqual([spec.source_backend for spec in reader.column_specs], ["elasticsearch", "elasticsearch"])
        self.assertEqual([spec.logical_type for spec in reader.column_specs], ["string", "timestamp"])
        self.assertEqual(reader.column_specs[1].datetime_precision, 6)

    def test_open_rowset_reader_applies_request_timeout_to_pit_search_and_close(self) -> None:
        fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})

        with (
            patch("zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_connection", return_value=fake_conn),
            patch(
                "zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_json_request",
                side_effect=[
                    {"id": "pit-1"},
                    {
                        "pit_id": "pit-2",
                        "hits": {
                            "hits": [
                                {
                                    "_source": {"sale_id": 1},
                                    "sort": [1, "shard"],
                                }
                            ]
                        },
                    },
                    {"pit_id": "pit-2", "hits": {"hits": []}},
                    {},
                ],
            ) as elasticsearch_json_request,
        ):
            reader = open_elasticsearch_rowset_reader(
                source_conn="elasticsearch_source",
                source={
                    "index": "checkpoint-recovery",
                    "query": {"match_all": {}},
                    "sort": [{"sale_id": "asc"}],
                    "request_timeout_seconds": 5,
                    "fields": [
                        {"column": "sale_id", "path": "sale_id", "type": "int", "nullable": False},
                    ],
                },
                watermark=None,
                time_window=None,
                batch_size=1,
                loaded_at=datetime(2026, 1, 1),
                metadata_name="checkpoint-recovery",
                job_id="checkpoint_recovery",
                step_id="extract_recovery_documents",
                output_name="recovery_rows",
                context={},
                kwargs={},
            )

            with reader:
                batches = list(reader.read_batches())

        self.assertEqual(reader.request_timeout_seconds, 5)
        self.assertEqual([batch.rows for batch in batches], [[(1,)]])
        self.assertEqual([call.kwargs["timeout"] for call in elasticsearch_json_request.mock_calls], [5, 5, 5, 5])

    def test_open_rowset_reader_rejects_engine_specific_field_type(self) -> None:
        fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})

        with patch("zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_connection", return_value=fake_conn):
            with self.assertRaisesRegex(ValueError, "must be a rowset type"):
                open_elasticsearch_rowset_reader(
                    source_conn="elasticsearch_source",
                    source={
                        "index": "products",
                        "fields": [
                            {"column": "product_id", "path": "product.id", "type": "String"},
                        ],
                    },
                    watermark=None,
                    time_window=None,
                    batch_size=500,
                    loaded_at=datetime(2026, 1, 1),
                    metadata_name="products",
                    job_id="products",
                    step_id="extract_products",
                    output_name="products_rows",
                    context={},
                    kwargs={},
                )

    def test_timestamp_field_defaults_to_microsecond_precision(self) -> None:
        fake_conn = types.SimpleNamespace(base_url="http://elasticsearch:9200", headers={})

        with patch("zeta4s.runtime.backends.elasticsearch.extract.elasticsearch_connection", return_value=fake_conn):
            reader = open_elasticsearch_rowset_reader(
                source_conn="elasticsearch_source",
                source={
                    "index": "products",
                    "fields": [
                        {"column": "updated_at", "path": "updated_at", "type": "timestamp"},
                    ],
                },
                watermark=None,
                time_window=None,
                batch_size=500,
                loaded_at=datetime(2026, 1, 1),
                metadata_name="products",
                job_id="products",
                step_id="extract_products",
                output_name="products_rows",
                context={},
                kwargs={},
            )

        spec = reader.column_specs[0]
        self.assertEqual(spec.type, "timestamp(6)")
        self.assertEqual(spec.datetime_precision, 6)
        self.assertEqual(str(arrow_type_from_column_spec(spec)), "timestamp[us]")
