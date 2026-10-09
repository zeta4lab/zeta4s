from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import yaml

from zeta4s.project.bundle import validate_project_configs
from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.extract_sql import load_extract_sql
from zeta4s.project.step_graph import STEP_TYPE_VALUES, ScheduleConfig, StepGraphStep, validate_step_graph_config


class StepSchemaContractTest(unittest.TestCase):
    def test_schedule_timezone_is_optional_and_falls_back_to_project_timezone(self) -> None:
        inherited = ScheduleConfig(cron="0 2 * * *")
        explicit = ScheduleConfig(cron="0 2 * * *", timezone=" Europe/Berlin ")

        self.assertIsNone(inherited.timezone)
        self.assertEqual(inherited.effective_timezone("Asia/Seoul"), "Asia/Seoul")
        self.assertEqual(explicit.effective_timezone("Asia/Seoul"), "Europe/Berlin")
        with self.assertRaisesRegex(ValueError, "unsupported schedule.timezone"):
            ScheduleConfig(cron="0 2 * * *", timezone="Mars/Olympus")

    def test_canonical_extract_stage_contract_passes(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_orders",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {
                        "kind": "query",
                        "query": "sql/oracle/fetch_orders.sql",
                    },
                    "output": {
                        "orders_rows": {
                            "kind": "rowset",
                        },
                    },
                },
                {
                    "step_id": "stage_orders",
                    "type": "clickhouse.stage",
                    "conn": "analytics_clickhouse",
                    "depends_on": ["fetch_orders"],
                    "map": {
                        "fetch_orders.orders_rows": "mart.stg_orders",
                    },
                },
            ],
        }

        job = validate_step_graph_config(Path("orders.yml"), config)

        self.assertEqual(job.job_id, "orders")

    def test_extract_rowset_output_rejects_obsolete_storage_format(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_orders",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {
                        "kind": "query",
                        "query": "sql/oracle/fetch_orders.sql",
                    },
                    "output": {
                        "orders_rows": {
                            "kind": "rowset",
                            "format": "parquet",
                        },
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "does not allow format"):
            validate_step_graph_config(Path("orders.yml"), config)

    def test_job_name_field_is_rejected(self) -> None:
        config = {
            "job_name": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "start",
                    "type": "noop",
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "job_id"):
            validate_step_graph_config(Path("orders.yml"), config)

    def test_id_field_is_rejected(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "id": "start",
                    "type": "noop",
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "step_id"):
            validate_step_graph_config(Path("orders.yml"), config)

    def test_sql_scalar_accepts_canonical_output_types(self) -> None:
        config = {
            "job_id": "scalar_checks",
            "schedule": None,
            "steps": [
                {
                    "step_id": "read_metrics",
                    "type": "sql.scalar",
                    "conn": "analytics_clickhouse",
                    "sql": "select 1, 1.5, true, 'ready'",
                    "outputs": {
                        "row_count": {"kind": "scalar", "type": "int", "column": 1},
                        "ratio": {"kind": "scalar", "type": "float", "column": 2},
                        "ready": {"kind": "scalar", "type": "bool", "column": 3},
                        "status": {"kind": "scalar", "type": "str", "column": 4},
                    },
                }
            ],
        }

        job = validate_step_graph_config(Path("scalar_checks.yml"), config)

        self.assertEqual(job.steps[0].outputs["row_count"]["type"], "int")

    def test_sql_scalar_rejects_type_aliases(self) -> None:
        config = {
            "job_id": "scalar_checks",
            "schedule": None,
            "steps": [
                {
                    "step_id": "read_metrics",
                    "type": "sql.scalar",
                    "conn": "analytics_clickhouse",
                    "sql": "select 1",
                    "outputs": {
                        "row_count": {"kind": "scalar", "type": "integer", "column": 1},
                    },
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "must be one of"):
            validate_step_graph_config(Path("scalar_checks.yml"), config)

    def test_execution_plan_separates_control_and_data_edges(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_orders",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {
                        "kind": "query",
                        "query": "sql/oracle/fetch_orders.sql",
                    },
                    "output": {
                        "orders_rows": {
                            "kind": "rowset",
                        },
                    },
                },
                {
                    "step_id": "stage_orders",
                    "type": "clickhouse.stage",
                    "conn": "analytics_clickhouse",
                    "depends_on": ["fetch_orders"],
                    "map": {
                        "fetch_orders.orders_rows": "mart.stg_orders",
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("orders.yml"), config)

        plan = build_step_graph_execution_plan(job)

        self.assertEqual(len(plan.control_edges), 1)
        self.assertEqual(len(plan.data_edges), 1)
        self.assertEqual(len(plan.data_bindings), 1)
        self.assertEqual(plan.data_bindings[0].source.step_id, "fetch_orders")
        self.assertEqual(plan.data_bindings[0].source.output_name, "orders_rows")

    def test_data_reference_requires_explicit_depends_on(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_orders",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {
                        "kind": "query",
                        "query": "sql/oracle/fetch_orders.sql",
                    },
                    "output": {
                        "orders_rows": {
                            "kind": "rowset",
                        },
                    },
                },
                {
                    "step_id": "stage_orders",
                    "type": "clickhouse.stage",
                    "conn": "analytics_clickhouse",
                    "when": {
                        "success": "fetch_orders",
                    },
                    "map": {
                        "fetch_orders.orders_rows": "mart.stg_orders",
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("orders.yml"), config)

        with self.assertRaisesRegex(ValueError, "not listed in depends_on"):
            build_step_graph_execution_plan(job)

    def test_stage_rejects_target_field(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "stage_orders",
                    "type": "clickhouse.stage",
                    "conn": "analytics_clickhouse",
                    "target": {
                        "table": "mart.stg_orders",
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "requires map"):
            validate_step_graph_config(Path("orders.yml"), config)

    def test_extract_rejects_watermark_and_time_window_together(self) -> None:
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_orders",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {
                        "kind": "table",
                        "table": "sales.orders",
                    },
                    "watermark": {
                        "column": "updated_at",
                        "overlap_window": "5m",
                    },
                    "time_window": {
                        "column": "updated_at",
                        "lookback": "30m",
                    },
                    "output": {
                        "orders_rows": {
                            "kind": "rowset",
                        },
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "cannot use watermark and time_window together"):
            validate_step_graph_config(Path("orders.yml"), config)

    def test_elasticsearch_extract_requires_fields(self) -> None:
        config = {
            "job_id": "products",
            "schedule": None,
            "steps": [
                {
                    "step_id": "extract_products",
                    "type": "elasticsearch.extract",
                    "conn": "elasticsearch_source",
                    "source": {
                        "kind": "search",
                        "index": "products",
                    },
                    "output": {
                        "products_rows": {
                            "kind": "rowset",
                        },
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "requires source.fields"):
            validate_step_graph_config(Path("products.yml"), config)

    def test_elasticsearch_extract_rejects_engine_specific_field_type(self) -> None:
        config = {
            "job_id": "products",
            "schedule": None,
            "steps": [
                {
                    "step_id": "extract_products",
                    "type": "elasticsearch.extract",
                    "conn": "elasticsearch_source",
                    "source": {
                        "kind": "search",
                        "index": "products",
                        "fields": [
                            {"column": "product_id", "path": "product.id", "type": "UInt64"},
                        ],
                    },
                    "output": {
                        "products_rows": {
                            "kind": "rowset",
                        },
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "source.fields\\[1\\].type must be one of"):
            validate_step_graph_config(Path("products.yml"), config)

    def test_elasticsearch_command_bulk_contract_passes(self) -> None:
        config = {
            "job_id": "seed_products",
            "schedule": None,
            "steps": [
                {
                    "step_id": "seed_products",
                    "type": "elasticsearch.command",
                    "conn": "elasticsearch_admin",
                    "operation": "bulk",
                    "target": {"index": "products"},
                    "source": {"file": "elasticsearch/products.bulk.ndjson", "format": "ndjson"},
                    "refresh": True,
                },
            ],
        }

        job = validate_step_graph_config(Path("seed_products.yml"), config)

        self.assertEqual(job.steps[0].type, "elasticsearch.command")
        self.assertEqual(job.steps[0].operation, "bulk")

    def test_elasticsearch_command_rejects_mode_policy(self) -> None:
        config = {
            "job_id": "seed_products",
            "schedule": None,
            "steps": [
                {
                    "step_id": "seed_products",
                    "type": "elasticsearch.command",
                    "conn": "elasticsearch_admin",
                    "operation": "bulk",
                    "mode": "replace",
                    "source": {"file": "elasticsearch/products.bulk.ndjson", "format": "ndjson"},
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "does not use mode"):
            validate_step_graph_config(Path("seed_products.yml"), config)

    def test_elasticsearch_command_reindex_requires_body_source_dest(self) -> None:
        config = {
            "job_id": "reindex_products",
            "schedule": None,
            "steps": [
                {
                    "step_id": "reindex_products",
                    "type": "elasticsearch.command",
                    "conn": "elasticsearch_admin",
                    "operation": "reindex",
                    "body": {"source": {"index": "products-v1"}},
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "requires body.source and body.dest"):
            validate_step_graph_config(Path("reindex_products.yml"), config)

    def test_clickhouse_write_requires_rowset_binding(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_sales",
                    "type": "clickhouse.extract",
                    "conn": "clickhouse_source",
                    "source": {"kind": "query", "query": "select sale_id, amount from source.sales"},
                    "output": {"sales_rows": {"kind": "rowset"}},
                },
                {
                    "step_id": "write_sales",
                    "type": "clickhouse.write",
                    "conn": "clickhouse_target",
                    "depends_on": ["fetch_sales"],
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "mart.sales",
                            "mode": "replace",
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("write_sales.yml"), config)

        plan = build_step_graph_execution_plan(job)

        self.assertEqual(plan.data_bindings[0].required_kind, "rowset")

    def test_oracle_write_requires_rowset_binding(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_sales",
                    "type": "clickhouse.extract",
                    "conn": "clickhouse_source",
                    "source": {"kind": "query", "query": "select sale_id, amount from source.sales"},
                    "output": {"sales_rows": {"kind": "rowset"}},
                },
                {
                    "step_id": "write_sales",
                    "type": "oracle.write",
                    "conn": "oracle_target",
                    "depends_on": ["fetch_sales"],
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "sales",
                            "mode": "replace",
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("write_sales.yml"), config)

        plan = build_step_graph_execution_plan(job)

        self.assertEqual(plan.data_bindings[0].required_kind, "rowset")

    def test_http_lookup_accepts_dbt_table_binding_on_same_conn(self) -> None:
        config = {
            "job_id": "lookup_customer",
            "schedule": None,
            "steps": [
                {
                    "step_id": "build_customer_mart",
                    "type": "dbt.run",
                    "conn": "analytics_clickhouse",
                    "models": ["customer_360"],
                },
                {
                    "step_id": "enrich_customer",
                    "type": "http.lookup",
                    "conn": "analytics_clickhouse",
                    "depends_on": ["build_customer_mart"],
                    "lookup": {
                        "table": "build_customer_mart.customer_360",
                        "column": "customer_name",
                    },
                    "target": {"table": "mart.customer_360_enriched"},
                    "api": {
                        "conn": "priority_api",
                        "method": "GET",
                        "response": {
                            "columns": [
                                {"name": "priority_label", "type": "String"},
                            ],
                        },
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("lookup_customer.yml"), config)

        plan = build_step_graph_execution_plan(job)

        self.assertEqual(plan.data_bindings[0].required_kind, "table")

    def test_http_lookup_requires_same_table_conn(self) -> None:
        config = {
            "job_id": "lookup_customer",
            "schedule": None,
            "steps": [
                {
                    "step_id": "build_customer_mart",
                    "type": "dbt.run",
                    "conn": "analytics_clickhouse",
                    "models": ["customer_360"],
                },
                {
                    "step_id": "enrich_customer",
                    "type": "http.lookup",
                    "conn": "other_clickhouse",
                    "depends_on": ["build_customer_mart"],
                    "lookup": {
                        "table": "build_customer_mart.customer_360",
                        "column": "customer_name",
                    },
                    "target": {"table": "mart.customer_360_enriched"},
                    "api": {
                        "conn": "priority_api",
                        "method": "GET",
                        "response": {
                            "columns": [
                                {"name": "priority_label", "type": "String"},
                            ],
                        },
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("lookup_customer.yml"), config)

        with self.assertRaisesRegex(ValueError, "lookup.table conn mismatch"):
            build_step_graph_execution_plan(job)

    def test_clickhouse_write_rejects_top_level_mode(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "clickhouse.write",
                    "conn": "clickhouse_target",
                    "mode": "replace",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "mart.sales",
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "does not support top-level mode"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_clickhouse_write_rejects_multiple_map_entries(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "clickhouse.write",
                    "conn": "clickhouse_target",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "mart.sales",
                            "mode": "replace",
                            "columns": ["sale_id", "amount"],
                        },
                        "fetch_customers.customer_rows": {
                            "table": "mart.customers",
                            "mode": "append",
                            "columns": ["customer_id", "name"],
                        },
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "requires exactly one map entry"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_clickhouse_write_rejects_keys_alias(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "clickhouse.write",
                    "conn": "clickhouse_target",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "mart.sales",
                            "mode": "upsert",
                            "keys": ["sale_id"],
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "unsupported keys: keys"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_clickhouse_write_rejects_non_contract_target_fields(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "clickhouse.write",
                    "conn": "clickhouse_target",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "table": "sales",
                            "schema": "mart",
                            "database": "analytics",
                            "alias": "sales_alias",
                            "mode": "replace",
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "unsupported keys: alias, database, schema"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_elasticsearch_write_requires_rowset_binding(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "fetch_sales",
                    "type": "elasticsearch.extract",
                    "conn": "elasticsearch_source",
                    "source": {
                        "kind": "search",
                        "index": "sales",
                        "fields": [
                            {"column": "sale_id", "path": "sale_id", "type": "int", "precision": 19},
                            {"column": "amount", "path": "amount", "type": "decimal", "precision": 18, "scale": 2},
                        ],
                    },
                    "output": {"sales_rows": {"kind": "rowset"}},
                },
                {
                    "step_id": "write_sales",
                    "type": "elasticsearch.write",
                    "conn": "elasticsearch_target",
                    "depends_on": ["fetch_sales"],
                    "map": {
                        "fetch_sales.sales_rows": {
                            "index": "sales-target",
                            "mode": "replace",
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }
        job = validate_step_graph_config(Path("write_sales.yml"), config)

        plan = build_step_graph_execution_plan(job)

        self.assertEqual(plan.data_bindings[0].required_kind, "rowset")

    def test_elasticsearch_write_upsert_rejects_auto_document_id(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "elasticsearch.write",
                    "conn": "elasticsearch_target",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "index": "sales-target",
                            "mode": "upsert",
                            "document_id": {"mode": "auto"},
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "mode=upsert requires key\\[\\] or document_id columns"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_elasticsearch_write_rejects_keys_alias(self) -> None:
        config = {
            "job_id": "write_sales",
            "schedule": None,
            "steps": [
                {
                    "step_id": "write_sales",
                    "type": "elasticsearch.write",
                    "conn": "elasticsearch_target",
                    "map": {
                        "fetch_sales.sales_rows": {
                            "index": "sales-target",
                            "mode": "upsert",
                            "keys": ["sale_id"],
                            "columns": ["sale_id", "amount"],
                        }
                    },
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "unsupported keys: keys"):
            validate_step_graph_config(Path("write_sales.yml"), config)

    def test_extract_sql_allows_bind_parameters(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            sql_path = root / "extract_orders.sql"
            sql_path.write_text(
                "\n".join(
                    [
                        "select order_id, updated_at",
                        "from orders",
                        "where updated_at > :select_from",
                        "  and updated_at <= :cur_wm",
                    ]
                ),
                encoding="utf-8",
            )

            sql = load_extract_sql(root, "extract_orders.sql")

            self.assertIn(":select_from", sql)
            self.assertIn(":cur_wm", sql)

    def test_dbt_rejects_selector(self) -> None:
        config = {
            "job_id": "dbt_job",
            "schedule": None,
            "steps": [
                {
                    "step_id": "build_mart",
                    "type": "dbt.run",
                    "conn": "analytics_clickhouse",
                    "selector": "tag:mart",
                },
            ],
        }

        with self.assertRaisesRegex(ValueError, "Extra inputs are not permitted"):
            validate_step_graph_config(Path("dbt_job.yml"), config)

    def test_step_id_is_scoped_to_job(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "duplicate_steps"
            jobs = root / "jobs"
            jobs.mkdir(parents=True)
            (root / "dbt").mkdir()
            (root / "project.yml").write_text(
                yaml.safe_dump(
                    {
                        "project_id": "duplicate_steps",
                        "timezone": "Asia/Seoul",
                        "paths": {
                            "jobs": "jobs",
                            "dbt": "dbt",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            for job_name in ("first", "second"):
                (jobs / f"{job_name}.yml").write_text(
                    yaml.safe_dump(
                        {
                            "job_id": job_name,
                            "schedule": None,
                            "steps": [
                                {
                                    "step_id": "shared_step",
                                    "type": "noop",
                                }
                            ],
                        },
                        sort_keys=False,
                    ),
                    encoding="utf-8",
                )

            self.assertEqual(len(validate_project_configs(root)), 2)

    def test_step_type_json_schema_exposes_registry_enum(self) -> None:
        # STEP_TYPE Literal 을 str + membership 검증으로 바꾼 뒤에도 JSON Schema 의
        # type 필드는 registry 정본의 허용값 enum 을 노출해야 한다. 그렇지 않으면
        # 계약 생성기/API 문서(임의 문자열 허용)와 런타임 계약(18개만 허용)이 어긋난다.
        type_schema = StepGraphStep.model_json_schema()["properties"]["type"]

        self.assertEqual(type_schema.get("enum"), list(STEP_TYPE_VALUES))
        self.assertEqual(len(type_schema["enum"]), len(set(STEP_TYPE_VALUES)))

    def test_unknown_step_type_is_rejected_at_load(self) -> None:
        # 런타임 membership 검증이 JSON Schema enum 과 같은 registry 정본을 쓰는지
        # 확인한다. enum 밖 값은 로드에서 거부되어야 한다.
        config = {
            "job_id": "orders",
            "schedule": None,
            "steps": [
                {
                    "step_id": "start",
                    "type": "oracle.magic",
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "steps\\[\\].type must be one of"):
            validate_step_graph_config(Path("orders.yml"), config)


if __name__ == "__main__":
    unittest.main()
