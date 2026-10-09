from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

from zeta4s.project.execution_plan import build_step_graph_execution_plan
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import validate_step_graph_config
from zeta4s.runtime.source_reader import ColumnSpec, SourceBatch


class FakePythonOperator:
    def __init__(
        self,
        *,
        task_id: str,
        python_callable,
        op_kwargs: dict | None = None,
        pool: str | None = None,
        **kwargs,
    ):
        self.task_id = task_id
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}
        self.pool = pool
        self.kwargs = kwargs


class FakeEmptyOperator:
    def __init__(self, *, task_id: str, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs


class FakeDAG:
    def __init__(self, *, dag_id: str, **kwargs):
        self.dag_id = dag_id
        self.max_active_runs = kwargs.get("max_active_runs")
        self.catchup = kwargs.get("catchup", False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


def _install_airflow_stub() -> None:
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")
    empty = types.ModuleType("airflow.providers.standard.operators.empty")
    sdk = types.ModuleType("airflow.sdk")
    python.PythonOperator = FakePythonOperator
    empty.EmptyOperator = FakeEmptyOperator
    sdk.DAG = FakeDAG
    sdk.get_current_context = lambda: {}
    sys.modules.setdefault("airflow", airflow)
    sys.modules.setdefault("airflow.sdk", sdk)
    sys.modules.setdefault("airflow.providers", providers)
    sys.modules.setdefault("airflow.providers.standard", standard)
    sys.modules.setdefault("airflow.providers.standard.operators", operators)
    sys.modules.setdefault("airflow.providers.standard.operators.python", python)
    sys.modules.setdefault("airflow.providers.standard.operators.empty", empty)


_install_airflow_stub()

from zeta4s.airflow.step_binding import StepBindingContext  # noqa: E402
from zeta4s.airflow import dag_generator  # noqa: E402
from zeta4s.project.pools import project_pool_name  # noqa: E402
from zeta4s.core import RuntimeCallableStepExecutor  # noqa: E402
from zeta4s.core.step_executors import built_in_step_executor, supported_builtin_step_types  # noqa: E402
from zeta4s.runtime.rowset_extract import ROWSET_COLUMN_SPECS_METADATA_KEY  # noqa: E402
from zeta4s.runtime.rowset_extract import ClickHouseSelectReader  # noqa: E402
from zeta4s.runtime.rowset_extract import _run_extract_rowset_impl  # noqa: E402
from zeta4s.runtime.rowset_extract import _select_query  # noqa: E402
from zeta4s.runtime.rowset_extract import write_reader_to_rowset  # noqa: E402
from zeta4s.runtime.rowset_models import RowsetDescriptor, RowsetIdentity, RowsetStorage  # noqa: E402
from zeta4s.runtime.rowset_stores.parquet import ParquetRowsetStore  # noqa: E402
from zeta4s.runtime.step_state import parse_watermark_value  # noqa: E402
from zeta4s.runtime.rowset_stage import _column_specs  # noqa: E402
from zeta4s.runtime.rowset_stage import _clickhouse_type_from_column_spec  # noqa: E402
from zeta4s.runtime.rowsets import ResolvedRowsetRef, resolve_rowset_ref  # noqa: E402
from zeta4s.runtime.rowsets import runtime_home  # noqa: E402


def _column_spec_tuples(specs):
    return [(spec.name, spec.type, spec.nullable) for spec in specs]


def write_reader_to_parquet_rowset(*, reader, path, watermark_column=None, context=None):
    """Exercise the storage-neutral writer with a local Parquet store."""
    import shutil
    from urllib.parse import unquote, urlparse

    context = dict(context or {})
    result = write_reader_to_rowset(
        reader=reader,
        store=ParquetRowsetStore(path.parent),
        identity=RowsetIdentity(
            project_id="test",
            job_id="test",
            run_id=str(context.get("run_id") or "run"),
            step_id=str(context.get("task_id") or "extract"),
            attempt=1,
            output_name=path.stem,
        ),
        watermark_column=watermark_column,
        context=context,
    )
    generated = Path(unquote(urlparse(result.uri).path))
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(generated, path)
    return result


def _resolved_test_parquet(path: Path, specs, source_ref: str) -> ResolvedRowsetRef:
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema_arrow
    step_id, output_name = source_ref.split(".", 1)
    descriptor = RowsetDescriptor(
        RowsetStorage.PARQUET,
        path.as_uri(),
        pq.read_metadata(path).num_rows,
        path.stat().st_size,
        tuple(schema.names),
        tuple(specs),
        hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
    )
    return ResolvedRowsetRef(
        source_ref, step_id, output_name, descriptor, ParquetRowsetStore(path.parent).open_reader(descriptor)
    )


def _project() -> ProjectContext:
    root = Path("/tmp/zeta4s-adapter-test")
    return ProjectContext(
        project_id="adapter_test",
        root=root,
        jobs_dir=root / "jobs",
        assets_dir=root / "assets",
        dbt_dir=root / "dbt",
        timezone="Asia/Seoul",
    )


def _ctx(config: dict, step_id: str) -> StepBindingContext:
    job = validate_step_graph_config(Path("job.yml"), config)
    plan = build_step_graph_execution_plan(job)
    plan_step = plan.step_by_id[step_id]
    return StepBindingContext(
        project=_project(),
        plan=plan,
        plan_step=plan_step,
        step=plan_step.step,
    )


def _binding(ctx: StepBindingContext):
    return dag_generator._make_step_graph_binding(ctx.project, ctx.plan, ctx.plan_step)


def _assert_core_step_task(testcase: unittest.TestCase, task, *, step_id: str, job_id: str) -> None:
    testcase.assertEqual(task.python_callable.__name__, "run_core_step")
    testcase.assertEqual(task.op_kwargs["step_config"]["step_id"], step_id)
    testcase.assertEqual(task.op_kwargs["job_config"]["job_id"], job_id)
    testcase.assertEqual(task.op_kwargs["project_config"]["project_id"], "adapter_test")
    testcase.assertIsNone(task.op_kwargs["profile"])
    testcase.assertNotIn("runtime_callable", task.op_kwargs)
    testcase.assertNotIn("runtime_kwargs", task.op_kwargs)


def _core_projection_contracts() -> dict[str, dict]:
    return {
        "noop": {
            "job_id": "noop_job",
            "steps": [{"step_id": "step_under_test", "type": "noop"}],
        },
        "oracle.extract": {
            "job_id": "oracle_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "oracle.extract",
                    "conn": "oracle_source",
                    "source": {"kind": "query", "query": "select id from source.orders"},
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "clickhouse.extract": {
            "job_id": "clickhouse_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "clickhouse.extract",
                    "conn": "clickhouse_source",
                    "source": {"kind": "query", "query": "select id from source.orders"},
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "elasticsearch.extract": {
            "job_id": "elasticsearch_extract_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "elasticsearch.extract",
                    "conn": "search_source",
                    "source": {
                        "kind": "search",
                        "index": "orders",
                        "fields": [{"column": "id", "path": "id", "type": "int", "precision": 19}],
                    },
                    "output": {"orders_rows": {"kind": "rowset"}},
                }
            ],
        },
        "clickhouse.stage": _mapped_step_contract("clickhouse.stage", "clickhouse_stage_job", "mart.stg_orders"),
        "oracle.stage": _mapped_step_contract("oracle.stage", "oracle_stage_job", "mart.stg_orders"),
        "http.lookup": {
            "job_id": "http_lookup_job",
            "steps": [
                {
                    "step_id": "build_customer",
                    "type": "dbt.run",
                    "conn": "analytics",
                    "models": ["customer"],
                },
                {
                    "step_id": "step_under_test",
                    "type": "http.lookup",
                    "conn": "analytics",
                    "depends_on": ["build_customer"],
                    "lookup": {"table": "build_customer.customer", "column": "name"},
                    "target": {"table": "mart.customer_enriched"},
                    "api": {
                        "conn": "customer_api",
                        "method": "GET",
                        "response": {"columns": [{"name": "priority", "type": "String"}]},
                    },
                },
            ],
        },
        "dbt.run": _dbt_contract("dbt.run", "dbt_run_job"),
        "dbt.test": _dbt_contract("dbt.test", "dbt_test_job"),
        "sql.scalar": {
            "job_id": "sql_scalar_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "sql.scalar",
                    "conn": "analytics",
                    "sql": "select 1 as row_count",
                    "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                }
            ],
        },
        "clickhouse.write": _write_contract(
            "clickhouse.write",
            "clickhouse_write_job",
            {"table": "mart.orders", "mode": "replace", "columns": ["id"]},
        ),
        "oracle.write": _write_contract(
            "oracle.write",
            "oracle_write_job",
            {"table": "mart.orders", "mode": "upsert", "key": ["id"], "columns": ["id"]},
        ),
        "elasticsearch.write": _write_contract(
            "elasticsearch.write",
            "elasticsearch_write_job",
            {"index": "orders", "mode": "upsert", "document_id": {"columns": ["id"]}, "columns": ["id"]},
        ),
        "elasticsearch.command": {
            "job_id": "elasticsearch_command_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "elasticsearch.command",
                    "conn": "search_admin",
                    "operation": "bulk",
                    "target": {"index": "orders"},
                    "source": {"file": "elasticsearch/orders.ndjson", "format": "ndjson"},
                }
            ],
        },
        "sql.check": _sql_contract("sql.check", "sql_check_job"),
        "oracle.sql": _sql_contract("oracle.sql", "oracle_sql_job"),
        "clickhouse.sql": _sql_contract("clickhouse.sql", "clickhouse_sql_job"),
        "oracle.call": {
            "job_id": "oracle_call_job",
            "steps": [
                {
                    "step_id": "step_under_test",
                    "type": "oracle.call",
                    "conn": "analytics",
                    "call": "pkg.refresh_orders",
                }
            ],
        },
    }


def _mapped_step_contract(step_type: str, job_id: str, target: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "fetch_orders",
                "type": "oracle.extract",
                "conn": "oracle_source",
                "source": {"kind": "query", "query": "select id from orders"},
                "output": {"orders_rows": {"kind": "rowset"}},
            },
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "depends_on": ["fetch_orders"],
                "map": {"fetch_orders.orders_rows": target},
            },
        ],
    }


def _write_contract(step_type: str, job_id: str, target: dict) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "fetch_orders",
                "type": "oracle.extract",
                "conn": "oracle_source",
                "source": {"kind": "query", "query": "select id from orders"},
                "output": {"orders_rows": {"kind": "rowset"}},
            },
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "target",
                "depends_on": ["fetch_orders"],
                "map": {"fetch_orders.orders_rows": target},
            },
        ],
    }


def _dbt_contract(step_type: str, job_id: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "models": ["orders"],
            }
        ],
    }


def _sql_contract(step_type: str, job_id: str) -> dict:
    return {
        "job_id": job_id,
        "steps": [
            {
                "step_id": "step_under_test",
                "type": step_type,
                "conn": "analytics",
                "sql": "select 1",
            }
        ],
    }


def _uses_unsupported_runtime_callable(executor: object) -> bool:
    if isinstance(executor, RuntimeCallableStepExecutor):
        return executor.runtime_callable == "zeta4s.core:run_unsupported_step"
    nested = getattr(executor, "executors", ())
    return any(_uses_unsupported_runtime_callable(item) for item in nested)


class AirflowAdapterPayloadContractTest(unittest.TestCase):
    def test_core_executor_factory_supports_every_builtin_step_type(self) -> None:
        contracts = _core_projection_contracts()

        self.assertEqual(set(contracts), set(supported_builtin_step_types()))
        for step_type, config in contracts.items():
            with self.subTest(step_type=step_type):
                ctx = _ctx(config, "step_under_test")

                executor = built_in_step_executor(
                    project=ctx.project,
                    plan=ctx.plan,
                    plan_step=ctx.plan_step,
                    runtime_home="/tmp/zeta4s",
                )

                self.assertFalse(_uses_unsupported_runtime_callable(executor))

    def test_every_builtin_step_type_binds_to_generic_core_step_operator(self) -> None:
        contracts = _core_projection_contracts()

        self.assertEqual(set(contracts), set(supported_builtin_step_types()))
        for step_type, config in contracts.items():
            with self.subTest(step_type=step_type):
                ctx = _ctx(config, "step_under_test")

                binding = _binding(ctx)

                self.assertEqual(len(binding.tasks), 1)
                task = binding.tasks[0]
                self.assertIsInstance(task, FakePythonOperator)
                self.assertEqual(binding.roots, (task,))
                self.assertEqual(binding.terminals, (task,))
                _assert_core_step_task(self, task, step_id="step_under_test", job_id=config["job_id"])

    def test_noop_step_binds_to_generic_core_step_operator(self) -> None:
        ctx = _ctx(
            {
                "job_id": "noop_job",
                "schedule": None,
                "steps": [{"step_id": "start", "type": "noop"}],
            },
            "start",
        )

        binding = _binding(ctx)
        task = binding.tasks[0]

        self.assertIsInstance(task, FakePythonOperator)
        self.assertEqual(task.task_id, "start")
        _assert_core_step_task(self, task, step_id="start", job_id="noop_job")

    def test_generated_airflow_task_uses_core_reporter_without_step_execution_callbacks(self) -> None:
        ctx = _ctx(
            {
                "job_id": "noop_job",
                "schedule": None,
                "steps": [{"step_id": "start", "type": "noop"}],
            },
            "start",
        )

        binding = dag_generator._make_step_graph_binding(_project(), ctx.plan, ctx.plan_step)
        task = binding.tasks[0]

        self.assertEqual(task.python_callable.__name__, "run_core_step")
        self.assertFalse(hasattr(task, "on_execute_callback"))
        self.assertFalse(hasattr(task, "on_success_callback"))
        self.assertNotIn("__zeta4s_step_execution", getattr(task, "params", {}))

    def test_generated_airflow_task_carries_deployment_profile_to_core_step(self) -> None:
        ctx = _ctx(
            {
                "job_id": "noop_job",
                "schedule": None,
                "steps": [{"step_id": "start", "type": "noop"}],
            },
            "start",
        )

        binding = dag_generator._make_step_graph_binding(_project(), ctx.plan, ctx.plan_step, profile_id="prod")
        task = binding.tasks[0]

        self.assertEqual(task.op_kwargs["profile"], "prod")

    def test_generated_airflow_task_uses_execution_plan_default_pool(self) -> None:
        ctx = _ctx(
            {
                "job_id": "pooled_job",
                "schedule": None,
                "defaults": {"pools": {"transform": "analytics_transform_pool"}},
                "steps": [
                    {
                        "step_id": "select_one",
                        "type": "clickhouse.sql",
                        "conn": "analytics",
                        "sql": "select 1",
                    }
                ],
            },
            "select_one",
        )

        binding = dag_generator._make_step_graph_binding(_project(), ctx.plan, ctx.plan_step)
        task = binding.tasks[0]

        self.assertEqual(task.pool, "analytics_transform_pool")
        self.assertEqual(task.op_kwargs["step_config"]["pool"], "analytics_transform_pool")

    def test_generated_airflow_task_uses_project_scoped_pool_without_default_pool(self) -> None:
        ctx = _ctx(
            {
                "job_id": "auto_pooled_job",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "select_one",
                        "type": "clickhouse.sql",
                        "conn": "analytics",
                        "sql": "select 1",
                    }
                ],
            },
            "select_one",
        )

        binding = dag_generator._make_step_graph_binding(_project(), ctx.plan, ctx.plan_step)
        task = binding.tasks[0]
        expected_pool = project_pool_name("adapter_test", "transform")

        self.assertEqual(task.pool, expected_pool)
        self.assertEqual(task.op_kwargs["step_config"]["pool"], expected_pool)

    def test_extract_adapter_uses_rowset_payload(self) -> None:
        ctx = _ctx(
            {
                "job_id": "orders",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "fetch_orders",
                        "type": "oracle.extract",
                        "conn": "oracle_source",
                        "source": {"kind": "query", "query": "sql/oracle/fetch_orders.sql"},
                        "params": {"window_start": "2026-01-01 00:00"},
                        "output": {"orders_rows": {"kind": "rowset"}},
                    },
                ],
            },
            "fetch_orders",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        self.assertEqual(task.task_id, "fetch_orders")
        _assert_core_step_task(self, task, step_id="fetch_orders", job_id="orders")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "oracle_source")
        self.assertEqual(
            task.op_kwargs["step_config"]["source"], {"kind": "query", "query": "sql/oracle/fetch_orders.sql"}
        )
        self.assertEqual(task.op_kwargs["step_config"]["output"], {"orders_rows": {"kind": "rowset"}})
        self.assertEqual(task.op_kwargs["step_config"]["params"], {"window_start": "2026-01-01 00:00"})

    def test_elasticsearch_command_adapter_uses_command_payload(self) -> None:
        ctx = _ctx(
            {
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
            },
            "seed_products",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        self.assertEqual(task.task_id, "seed_products")
        _assert_core_step_task(self, task, step_id="seed_products", job_id="seed_products")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "elasticsearch_admin")
        self.assertEqual(task.op_kwargs["step_config"]["operation"], "bulk")
        self.assertEqual(task.op_kwargs["step_config"]["target"], {"index": "products"})
        self.assertEqual(
            task.op_kwargs["step_config"]["source"], {"file": "elasticsearch/products.bulk.ndjson", "format": "ndjson"}
        )
        self.assertTrue(task.op_kwargs["step_config"]["refresh"])

    def test_clickhouse_stage_uses_map_target_payload(self) -> None:
        ctx = _ctx(
            {
                "job_id": "orders",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "fetch_orders",
                        "type": "oracle.extract",
                        "conn": "oracle_source",
                        "source": {"kind": "query", "query": "select * from orders"},
                        "output": {"orders_rows": {"kind": "rowset"}},
                    },
                    {
                        "step_id": "stage_orders",
                        "type": "clickhouse.stage",
                        "conn": "analytics_clickhouse",
                        "depends_on": ["fetch_orders"],
                        "map": {"fetch_orders.orders_rows": "mart.stg_orders"},
                    },
                ],
            },
            "stage_orders",
        )

        binding = _binding(ctx)

        self.assertEqual(len(binding.tasks), 1)
        task = binding.tasks[0]
        self.assertEqual(task.task_id, "stage_orders")
        _assert_core_step_task(self, task, step_id="stage_orders", job_id="orders")
        self.assertEqual(task.op_kwargs["step_config"]["map"], {"fetch_orders.orders_rows": "mart.stg_orders"})

    def test_clickhouse_write_uses_rowset_source_and_target_payload(self) -> None:
        ctx = _ctx(
            {
                "job_id": "write_sales",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "fetch_sales",
                        "type": "clickhouse.extract",
                        "conn": "source_clickhouse",
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
            },
            "write_sales",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        _assert_core_step_task(self, task, step_id="write_sales", job_id="write_sales")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "clickhouse_target")
        self.assertEqual(task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["table"], "mart.sales")
        self.assertEqual(
            task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["columns"], ["sale_id", "amount"]
        )

    def test_oracle_write_uses_rowset_source_and_target_payload(self) -> None:
        ctx = _ctx(
            {
                "job_id": "write_sales",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "fetch_sales",
                        "type": "clickhouse.extract",
                        "conn": "source_clickhouse",
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
                                "table": "mart.sales",
                                "mode": "upsert",
                                "key": ["sale_id"],
                                "columns": ["sale_id", "amount"],
                            }
                        },
                    },
                ],
            },
            "write_sales",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        _assert_core_step_task(self, task, step_id="write_sales", job_id="write_sales")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "oracle_target")
        self.assertEqual(task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["table"], "mart.sales")
        self.assertEqual(task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["key"], ["sale_id"])

    def test_elasticsearch_write_uses_rowset_source_and_target_payload(self) -> None:
        ctx = _ctx(
            {
                "job_id": "write_sales",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "fetch_sales",
                        "type": "elasticsearch.extract",
                        "conn": "source_elasticsearch",
                        "source": {
                            "kind": "search",
                            "index": "sales",
                            "fields": [{"column": "sale_id", "path": "sale_id", "type": "int", "precision": 19}],
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
                                "mode": "upsert",
                                "document_id": {"columns": ["sale_id"]},
                                "columns": ["sale_id", "amount"],
                                "batch_size": 500,
                            }
                        },
                    },
                ],
            },
            "write_sales",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        _assert_core_step_task(self, task, step_id="write_sales", job_id="write_sales")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "elasticsearch_target")
        self.assertEqual(task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["index"], "sales-target")
        self.assertEqual(
            task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["document_id"], {"columns": ["sale_id"]}
        )
        self.assertEqual(task.op_kwargs["step_config"]["map"]["fetch_sales.sales_rows"]["batch_size"], 500)

    def test_http_lookup_uses_lookup_and_api_response_payload(self) -> None:
        ctx = _ctx(
            {
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
                            "request": {"query_param": "text"},
                            "response": {
                                "json_paths": ["$.result.priority_label", "$.priority_label"],
                                "columns": [
                                    {"name": "priority_label", "type": "String"},
                                ],
                            },
                        },
                    },
                ],
            },
            "enrich_customer",
        )

        binding = _binding(ctx)

        task = binding.tasks[0]
        _assert_core_step_task(self, task, step_id="enrich_customer", job_id="lookup_customer")
        self.assertEqual(task.op_kwargs["step_config"]["conn"], "analytics_clickhouse")
        self.assertEqual(task.op_kwargs["step_config"]["lookup"]["table"], "build_customer_mart.customer_360")
        self.assertEqual(task.op_kwargs["step_config"]["api"]["request"]["query_param"], "text")
        self.assertEqual(
            task.op_kwargs["step_config"]["api"]["response"]["json_paths"],
            ["$.result.priority_label", "$.priority_label"],
        )

    def test_airflow_when_expr_remains_core_runner_flow_control(self) -> None:
        ctx = _ctx(
            {
                "job_id": "expr_job",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "count_rows",
                        "type": "sql.scalar",
                        "conn": "analytics",
                        "sql": "select count(*) as row_count from orders",
                        "outputs": {"row_count": {"kind": "scalar", "type": "int"}},
                    },
                    {
                        "step_id": "publish",
                        "type": "noop",
                        "depends_on": ["count_rows"],
                        "when": {"expr": "$steps.count_rows.outputs.row_count >= 4"},
                    },
                ],
            },
            "publish",
        )

        binding = dag_generator._make_step_graph_binding(ctx.project, ctx.plan, ctx.plan_step)

        self.assertEqual(len(binding.tasks), 1)
        task = binding.tasks[0]
        self.assertEqual(binding.roots, (task,))
        self.assertEqual(binding.terminals, (task,))
        _assert_core_step_task(self, task, step_id="publish", job_id="expr_job")
        self.assertEqual(task.op_kwargs["step_config"]["when"], {"expr": "$steps.count_rows.outputs.row_count >= 4"})

    def test_airflow_retry_and_timeout_map_to_engine_policy(self) -> None:
        ctx = _ctx(
            {
                "job_id": "policy_job",
                "schedule": None,
                "steps": [
                    {
                        "step_id": "work",
                        "type": "noop",
                        "retry": {"max_attempts": 3, "delay_seconds": 0},
                        "timeout": {"seconds": 5},
                    },
                ],
            },
            "work",
        )

        binding = dag_generator._make_step_graph_binding(ctx.project, ctx.plan, ctx.plan_step)

        task = binding.tasks[0]
        _assert_core_step_task(self, task, step_id="work", job_id="policy_job")
        self.assertEqual(task.kwargs["retries"], 2)
        self.assertEqual(task.kwargs["retry_delay"].total_seconds(), 0)
        self.assertEqual(task.kwargs["execution_timeout"].total_seconds(), 5)
        self.assertEqual(task.op_kwargs["step_config"]["retry"], {"max_attempts": 3, "delay_seconds": 0})
        self.assertEqual(task.op_kwargs["step_config"]["timeout"], {"seconds": 5})


class FakeReader:
    source_kind = "fake"
    source_object = "fake_source"
    column_specs = [("id", "Int64", False), ("updated_at", "DateTime", False)]
    columns = ["id", "updated_at"]

    def read_batches(self):
        from datetime import datetime

        yield SourceBatch(
            column_values={
                "id": [1, 2],
                "updated_at": [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 1, 0, 5)],
            }
        )


class _ContextReader:
    def __init__(self, reader):
        self.reader = reader

    def __enter__(self):
        return self.reader

    def __exit__(self, exc_type, exc, tb):
        return None


class _FakeStepStateAdapter:
    def __init__(self, states):
        self.step_state_repository = _FakeStepStateRepository(states)


class _FakeStepStateRepository:
    def __init__(self, states):
        self.states = states

    def get_state(self, *, project_id, job_id, step_id, state_key):
        return self.states.get((project_id, job_id, step_id, state_key))


class EmptySchemaReader:
    source_kind = "fake"
    source_object = "empty_source"
    column_specs = []
    columns = []

    def read_batches(self):
        return
        yield


class EmptyRowsReader:
    source_kind = "fake"
    source_object = "empty_rows_source"
    column_specs = [("id", "Int64", False), ("updated_at", "DateTime64(3)", True)]
    columns = ["id", "updated_at"]

    def read_batches(self):
        return
        yield


class DecimalDriftReader:
    source_kind = "fake"
    source_object = "decimal_drift_source"
    column_specs = [("id", "Int64", False), ("amount", "Decimal(18, 2)", False)]
    columns = ["id", "amount"]

    def read_batches(self):
        from decimal import Decimal

        yield SourceBatch(column_values={"id": [1, 2], "amount": [Decimal("1.23"), Decimal("12.34")]})
        yield SourceBatch(column_values={"id": [3, 4], "amount": [Decimal("12345.67"), Decimal("987654.32")]})


class NullFirstBatchReader:
    source_kind = "fake"
    source_object = "null_first_batch_source"
    column_specs = [
        ("id", "Int64", True),
        ("name", "String", True),
        ("updated_at", "DateTime64(6)", True),
    ]
    columns = ["id", "name", "updated_at"]

    def read_batches(self):
        from datetime import datetime

        yield SourceBatch(column_values={"id": [None], "name": [None], "updated_at": [None]})
        yield SourceBatch(
            column_values={
                "id": [1],
                "name": ["alpha"],
                "updated_at": [datetime(2026, 1, 1, 0, 0, 1, 123456)],
            }
        )


class PrimitiveSchemaReader:
    source_kind = "fake"
    source_object = "primitive_schema_source"
    column_specs = [
        ("int_col", "Int32", False),
        ("float_col", "Float32", False),
        ("bool_col", "Bool", False),
        ("date_col", "Date", False),
        ("timestamp_col", "DateTime64(3)", False),
        ("text_col", "LowCardinality(String)", True),
    ]
    columns = ["int_col", "float_col", "bool_col", "date_col", "timestamp_col", "text_col"]

    def read_batches(self):
        from datetime import date, datetime

        yield SourceBatch(
            column_values={
                "int_col": [1],
                "float_col": [1.5],
                "bool_col": [True],
                "date_col": [date(2026, 1, 1)],
                "timestamp_col": [datetime(2026, 1, 1, 0, 0, 1, 123000)],
                "text_col": ["x"],
            }
        )


class StringTemporalSchemaReader:
    source_kind = "fake"
    source_object = "string_temporal_source"
    column_specs = [
        ("sold_date", "Date", False),
        ("sold_at", "DateTime64(6)", False),
    ]
    columns = ["sold_date", "sold_at"]

    def read_batches(self):
        yield SourceBatch(
            column_values={
                "sold_date": ["2026-01-01"],
                "sold_at": ["2026-01-01T00:00:01.123456Z"],
            }
        )


class ArrowBatchReader:
    source_kind = "fake"
    source_object = "arrow_batch_source"
    column_specs = [("id", "Int64", False), ("name", "String", True)]
    columns = ["id", "name"]

    def read_batches(self):
        import pyarrow as pa

        yield SourceBatch(arrow_table=pa.table({"id": [1, 2], "name": ["alpha", None]}))


class RowsetExtractRuntimeContractTest(unittest.TestCase):
    def test_clickhouse_reader_prefers_describe_query_schema(self) -> None:
        class FakeClickHouseClient:
            def query(self, sql, parameters=None):
                self.sql = sql
                self.parameters = parameters
                return types.SimpleNamespace(
                    result_rows=[
                        ("is_priority", "Bool"),
                        ("discount_rate", "Float32"),
                        ("sold_date", "Date"),
                    ]
                )

        reader = ClickHouseSelectReader(
            source_conn="fake",
            source_object="fake_source",
            query="select is_priority, discount_rate, sold_date from source.sales where store_id = :store_id",
            params={"store_id": 1},
            batch_size=100,
        )
        client = FakeClickHouseClient()
        reader._client = client

        reader._set_schema_from_clickhouse_query()

        self.assertEqual(reader.columns, ["is_priority", "discount_rate", "sold_date"])
        self.assertEqual(
            _column_spec_tuples(reader.column_specs),
            [("is_priority", "Bool", False), ("discount_rate", "Float32", False), ("sold_date", "Date", False)],
        )
        self.assertEqual(reader.column_specs[0].logical_type, "boolean")
        self.assertEqual(reader.column_specs[1].precision, 32)
        self.assertEqual(client.parameters, {"store_id": 1})
        self.assertIn("DESCRIBE TABLE", client.sql)
        self.assertIn("where store_id = {store_id:Int64}", client.sql)

    def test_rowset_writer_creates_parquet_with_watermark(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "orders_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=FakeReader(),
                path=path,
                watermark_column="updated_at",
                context={"run_id": "run_1", "task_id": "fetch_orders"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(result.columns, ("id", "updated_at"))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(FakeReader.column_specs))
            metadata = table.schema.metadata or {}
            self.assertIn(ROWSET_COLUMN_SPECS_METADATA_KEY, metadata)
            self.assertEqual(result.new_watermark, datetime(2026, 1, 1, 0, 5))
            self.assertTrue(result.uri.startswith("file://"))

    def test_rowset_writer_creates_empty_parquet_with_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=EmptyRowsReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_empty"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 0)
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(result.columns, ("id", "updated_at"))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(EmptyRowsReader.column_specs))

    def test_rowset_writer_uses_column_specs_for_decimal_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "decimal_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=DecimalDriftReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_decimal"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(result.rows, 4)
            self.assertEqual(schema.field("amount").type, pa.decimal128(18, 2))
            self.assertEqual(_column_spec_tuples(result.column_specs), list(DecimalDriftReader.column_specs))
            self.assertEqual(result.column_specs[1].logical_type, "decimal")
            self.assertEqual(result.column_specs[1].precision, 18)
            self.assertEqual(result.column_specs[1].scale, 2)

    def test_rowset_writer_accepts_float_values_for_decimal_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class FloatDecimalReader:
            source_kind = "oracle"
            source_object = "source.decimal_float"
            column_specs = [("amount", "Decimal(18, 2)", True)]
            columns = ["amount"]

            def read_batches(self):
                yield SourceBatch(column_values={"amount": [1.23, None]})

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "float_decimal_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=FloatDecimalReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_float_decimal"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("amount").type, pa.decimal128(18, 2))

    def test_rowset_writer_casts_null_first_batch_to_fixed_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "null_first_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=NullFirstBatchReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_null_first"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.schema.field("id").type, pa.int64())
            self.assertEqual(table.schema.field("updated_at").type, pa.timestamp("us"))
            self.assertEqual(table.column("id").to_pylist(), [None, 1])

    def test_rowset_writer_preserves_primitive_column_spec_types(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "primitive_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=PrimitiveSchemaReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_primitive"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("int_col").type, pa.int32())
            self.assertEqual(schema.field("float_col").type, pa.float32())
            self.assertEqual(schema.field("bool_col").type, pa.bool_())
            self.assertEqual(schema.field("date_col").type, pa.date32())
            self.assertEqual(schema.field("timestamp_col").type, pa.timestamp("ms"))
            self.assertEqual(schema.field("text_col").type, pa.string())

    def test_rowset_writer_coerces_string_temporal_values_to_schema(self) -> None:
        from datetime import date, datetime
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "string_temporal_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=StringTemporalSchemaReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_string_temporal"},
            )

            table = pq.read_table(path)
            self.assertEqual(table.schema.field("sold_date").type, pa.date32())
            self.assertEqual(table.schema.field("sold_at").type, pa.timestamp("us"))
            self.assertEqual(table.column("sold_date").to_pylist(), [date(2026, 1, 1)])
            self.assertEqual(table.column("sold_at").to_pylist(), [datetime(2026, 1, 1, 0, 0, 1, 123456)])

    def test_rowset_writer_accepts_arrow_source_batch(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "arrow_rows.parquet"

            result = write_reader_to_parquet_rowset(
                reader=ArrowBatchReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_arrow"},
            )

            table = pq.read_table(path)
            self.assertEqual(result.rows, 2)
            self.assertEqual(table.column("name").to_pylist(), ["alpha", None])

    def test_rowset_writer_preserves_unsigned_integer_schema(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class UIntReader:
            source_kind = "clickhouse"
            source_object = "source.uints"
            column_specs = [("id", "UInt64", False)]
            columns = ["id"]

            def read_batches(self):
                yield SourceBatch(rows=[(9223372036854775808,)])

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "uint_rows.parquet"

            write_reader_to_parquet_rowset(
                reader=UIntReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_uints"},
            )

            schema = pq.ParquetFile(path).schema_arrow
            self.assertEqual(schema.field("id").type, pa.uint64())

    def test_clickhouse_stage_preserves_nullable_column_specs(self) -> None:
        spec = ColumnSpec.from_type("optional_count", "Int64", True)

        self.assertEqual(_clickhouse_type_from_column_spec(spec), "Nullable(Int64)")

    def test_oracle_stage_uses_arrow_executemany_with_typed_binds(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        import pyarrow as pa
        import pyarrow.parquet as pq

        class FakeCursor:
            def __init__(self):
                self.input_sizes = None
                self.executed = []
                self.executemany_calls = []

            def execute(self, sql, params=None):
                self.executed.append((sql, params))

            def fetchone(self):
                return (0,)

            def executemany(self, sql, payload):
                self.executemany_calls.append((sql, payload))

            def setinputsizes(self, *sizes):
                self.input_sizes = sizes

            def close(self):
                pass

        class FakeConn:
            def __init__(self):
                self.cursor_obj = FakeCursor()
                self.committed = False

            def cursor(self):
                return self.cursor_obj

            def commit(self):
                self.committed = True

            def close(self):
                pass

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "oracle_stage.parquet"
            pq.write_table(
                pa.table(
                    {
                        "id": [1, 2],
                        "updated_at": [datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 1, 0, 1)],
                    }
                ),
                path,
            )
            rowset = _resolved_test_parquet(
                path,
                (
                    ColumnSpec.from_type("id", "Int64", False, source_backend="oracle", source_type="NUMBER(19,0)"),
                    ColumnSpec.from_type(
                        "updated_at",
                        "DateTime64(6)",
                        True,
                        source_backend="oracle",
                        source_type="TIMESTAMP(6)",
                    ),
                ),
                "fetch.rows",
            )
            conn = FakeConn()
            fake_oracledb = types.ModuleType("oracledb")
            fake_oracledb.DB_TYPE_NUMBER = "NUMBER"
            fake_oracledb.DB_TYPE_TIMESTAMP = "TIMESTAMP"
            fake_oracledb.DB_TYPE_DATE = "DATE"
            fake_oracledb.DB_TYPE_BINARY_DOUBLE = "BINARY_DOUBLE"
            fake_oracledb.DB_TYPE_BINARY_FLOAT = "BINARY_FLOAT"
            fake_oracledb.DB_TYPE_CLOB = "CLOB"
            fake_oracledb.DB_TYPE_BLOB = "BLOB"
            fake_oracledb.DB_TYPE_VARCHAR = "VARCHAR"

            with (
                patch.dict(
                    sys.modules,
                    {"oracledb": fake_oracledb},
                ),
                patch("zeta4s.runtime.backends.oracle.stage.get_oracle_conn", return_value=conn),
            ):
                from zeta4s.runtime.backends.oracle.stage import stage_oracle_rowset

                loaded, target_ref = stage_oracle_rowset(
                    rowset=rowset,
                    stage_conn="oracle_default",
                    target_table="orders_stage",
                    target_namespace=None,
                )

            self.assertEqual(loaded, 2)
            self.assertEqual(target_ref, "ORDERS_STAGE")
            self.assertTrue(conn.committed)
            self.assertIsNotNone(conn.cursor_obj.input_sizes)
            self.assertEqual(len(conn.cursor_obj.input_sizes), 2)
            self.assertEqual(len(conn.cursor_obj.executemany_calls), 1)
            _, payload = conn.cursor_obj.executemany_calls[0]
            self.assertIsInstance(payload, pa.Table)

    def test_oracle_dataframe_fetch_keeps_clickhouse_integer_width(self) -> None:
        import pyarrow as pa

        fake_oracledb = types.ModuleType("oracledb")
        fake_oracledb.LOB = type("LOB", (), {})
        with patch.dict(sys.modules, {"oracledb": fake_oracledb}):
            from zeta4s.runtime.backends.oracle.extract import oracle_dataframe_arrow_type

        spec = ColumnSpec(
            name="amount",
            type="Nullable(Int64)",
            nullable=True,
            logical_type="integer",
            precision=5,
            scale=0,
            source_backend="oracle",
            source_type="NUMBER(5,0)",
        )

        self.assertEqual(oracle_dataframe_arrow_type(spec), pa.int64())

    def test_rowset_writer_rejects_empty_extract_without_schema(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"

            with self.assertRaisesRegex(ValueError, "no rows and no schema"):
                write_reader_to_parquet_rowset(
                    reader=EmptySchemaReader(),
                    path=path,
                    context={"run_id": "run_1", "task_id": "fetch_empty"},
                )

    def test_query_source_watermark_preserves_user_sql(self) -> None:
        sql = "select id, updated_at as watermark_ts from orders where updated_at > :select_from order by watermark_ts"

        adapter = _FakeStepStateAdapter(
            {
                ("retail", "orders", "extract_orders", "watermark:orders_rows:watermark_ts"): {
                    "state_value": {"value": "2026-01-01T00:00:00"}
                }
            }
        )

        with (
            patch("zeta4s.runtime.rowset_extract.load_extract_sql", return_value=sql),
            patch(
                "zeta4s.runtime.step_state.metastore_adapter_factory",
                return_value=adapter,
            ),
        ):
            from datetime import datetime

            query, _, params = _select_query(
                source={"kind": "query", "query": "sql/orders.sql"},
                source_type="oracle",
                source_kind="query",
                project_root="/tmp/project",
                params={},
                watermark={"column": "watermark_ts"},
                time_window=None,
                loaded_at=datetime(2026, 1, 1, 1, 0),
                metadata_name="orders",
                job_id="orders",
                step_id="extract_orders",
                output_name="orders_rows",
                context={},
                kwargs={"project_id": "retail"},
            )

        self.assertEqual(query, sql)
        self.assertIn("select_from", params)
        self.assertIn("cur_wm", params)
        self.assertEqual(params["select_from"], datetime(2026, 1, 1, 0, 0))

    def test_query_source_watermark_rejects_missing_job_id(self) -> None:
        with patch("zeta4s.runtime.rowset_extract.load_extract_sql", return_value="select * from orders"):
            from datetime import datetime

            with self.assertRaisesRegex(ValueError, "project_id, job_id, and step_id"):
                _select_query(
                    source={"kind": "query", "query": "sql/orders.sql"},
                    source_type="oracle",
                    source_kind="query",
                    project_root="/tmp/project",
                    params={},
                    watermark={"column": "updated_at"},
                    time_window=None,
                    loaded_at=datetime(2026, 1, 1, 1, 0),
                    metadata_name="extract_orders",
                    job_id=None,
                    step_id="extract_orders",
                    output_name="orders_rows",
                    context={},
                    kwargs={"project_id": "retail"},
                )

    def test_extract_rowset_records_watermark_to_step_state(self) -> None:
        from datetime import datetime
        from tempfile import TemporaryDirectory

        with (
            TemporaryDirectory() as tmp,
            patch.dict(os.environ, {"ZETA4S_API_HOME": tmp}),
            patch("zeta4s.runtime.rowset_extract.record_extract_history") as record_extract_history,
            patch("zeta4s.runtime.rowset_extract._open_reader", return_value=_ContextReader(FakeReader())),
            patch("zeta4s.runtime.rowset_extract.set_step_watermark") as set_step_watermark,
        ):
            result = _run_extract_rowset_impl(
                source_conn="orders_source",
                source={"kind": "table", "table": "orders"},
                output_name="orders_rows",
                source_type="oracle",
                project_root="/tmp/project",
                job_id="orders",
                step_id="extract_orders",
                params={},
                watermark={"column": "updated_at"},
                time_window=None,
                batch_size=None,
                context={"run_id": "run_1", "task_id": "extract_orders"},
                kwargs={"project_id": "retail"},
            )

        self.assertEqual(result.new_watermark, datetime(2026, 1, 1, 0, 5))
        set_step_watermark.assert_called_once_with(
            project_id="retail",
            job_id="orders",
            step_id="extract_orders",
            output_name="orders_rows",
            watermark_column="updated_at",
            watermark_value=datetime(2026, 1, 1, 0, 5),
            run_id="run_1",
        )
        self.assertEqual(record_extract_history.call_count, 2)
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].project_id, "retail")
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].job_id, "orders")
        self.assertEqual(record_extract_history.call_args_list[-1].args[0].step_id, "extract_orders")

    def test_extract_rowset_rejects_watermark_state_without_job_id(self) -> None:
        from tempfile import TemporaryDirectory

        with (
            TemporaryDirectory() as tmp,
            patch.dict(os.environ, {"ZETA4S_API_HOME": tmp}),
            patch("zeta4s.runtime.rowset_extract.record_extract_history") as record_extract_history,
            patch("zeta4s.runtime.rowset_extract._open_reader", return_value=_ContextReader(FakeReader())),
            patch("zeta4s.runtime.rowset_extract.set_step_watermark") as set_step_watermark,
        ):
            with self.assertRaisesRegex(ValueError, "rowset extract requires runtime identity: job_id"):
                _run_extract_rowset_impl(
                    source_conn="orders_source",
                    source={"kind": "table", "table": "orders"},
                    output_name="orders_rows",
                    source_type="oracle",
                    project_root="/tmp/project",
                    job_id=None,
                    step_id="extract_orders",
                    params={},
                    watermark={"column": "updated_at"},
                    time_window=None,
                    batch_size=None,
                    context={"run_id": "run_1", "task_id": "extract_orders"},
                    kwargs={"project_id": "retail"},
                )

        record_extract_history.assert_not_called()
        set_step_watermark.assert_not_called()

    def test_watermark_state_parses_offset_as_utc_naive(self) -> None:
        from datetime import datetime

        self.assertEqual(
            parse_watermark_value("2026-01-01T09:00:00+09:00"),
            datetime(2026, 1, 1, 0, 0),
        )

    def test_resolve_rowset_ref_reads_task_result_artifact(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rowset_path = root / "runs" / "run_1" / "artifacts" / "rowsets" / "fetch_orders" / "orders_rows.parquet"
            rowset_path.parent.mkdir(parents=True)
            rowset_path.write_bytes(b"parquet")
            result_path = root / "runs" / "run_1" / "tasks" / "fetch_orders.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "task_id": "fetch_orders",
                        "details": {
                            "outputs": {
                                "orders_rows": {
                                    "kind": "rowset",
                                    "storage": "parquet",
                                    "uri": rowset_path.as_uri(),
                                    "rows": 3,
                                    "bytes": 7,
                                    "columns": ["id"],
                                    "column_specs": [{"name": "id", "type": "Int64", "nullable": False}],
                                    "schema_fingerprint": "test-fingerprint",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            resolved = resolve_rowset_ref(
                source_ref="fetch_orders.orders_rows",
                context={"run_id": "run_1"},
                home=root,
            )

        self.assertEqual(resolved.uri, rowset_path.as_uri())
        self.assertEqual(resolved.rows, 3)
        self.assertEqual(resolved.columns, ("id",))
        self.assertEqual(_column_spec_tuples(resolved.column_specs), [("id", "Int64", False)])

    def test_resolve_rowset_ref_uses_z4_run_id(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rowset_path = root / "runs" / "z4_run_1" / "artifacts" / "rowsets" / "fetch_orders" / "orders_rows.parquet"
            rowset_path.parent.mkdir(parents=True)
            rowset_path.write_bytes(b"parquet")
            result_path = root / "runs" / "z4_run_1" / "tasks" / "fetch_orders.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "task_id": "fetch_orders",
                        "details": {
                            "outputs": {
                                "orders_rows": {
                                    "kind": "rowset",
                                    "storage": "parquet",
                                    "uri": rowset_path.as_uri(),
                                    "rows": 0,
                                    "bytes": 7,
                                    "columns": ["id"],
                                    "column_specs": [{"name": "id", "type": "Int64", "nullable": False}],
                                    "schema_fingerprint": "test-fingerprint",
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            resolved = resolve_rowset_ref(
                source_ref="fetch_orders.orders_rows",
                context={"run_id": "adapter_run_1", "z4_run_id": "z4_run_1"},
                home=root,
            )

        self.assertEqual(resolved.uri, rowset_path.as_uri())

    def test_runtime_home_uses_api_home_key_and_ignores_unknown_home_key(self) -> None:
        self.assertEqual(runtime_home({"zeta4s_api_home": "/tmp/api-home"}), Path("/tmp/api-home"))
        self.assertEqual(runtime_home({"runtime_home": "/tmp/runtime-home"}), Path("/tmp/runtime-home"))
        self.assertIsNone(runtime_home({"zeta4s_home": "/tmp/unused-home"}))

    def test_stage_column_specs_prefer_rowset_metadata(self) -> None:
        from tempfile import TemporaryDirectory

        import pyarrow.parquet as pq

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty_rows.parquet"
            write_reader_to_parquet_rowset(
                reader=EmptyRowsReader(),
                path=path,
                context={"run_id": "run_1", "task_id": "fetch_empty"},
            )
            parquet_file = pq.ParquetFile(path)
            specs = _column_specs(
                types.SimpleNamespace(column_specs=(), source_ref="fetch_empty.empty_rows"),
                parquet_file.schema_arrow,
            )

        self.assertEqual(_column_spec_tuples(specs), EmptyRowsReader.column_specs)


if __name__ == "__main__":
    unittest.main()
