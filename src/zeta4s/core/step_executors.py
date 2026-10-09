"""Core StepExecutor factory for built-in step graph types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from zeta4s.common.runtime_limits import WRITE_BATCH_SIZE_DEFAULT, WRITE_ELASTICSEARCH_BATCH_SIZE_DEFAULT
from zeta4s.common.sql_identifiers import validate_table_identifier
from zeta4s.core.execution_contract import StepExecutionState, StepResult
from zeta4s.core.runner import (
    RuntimeCallableStepExecutor,
    StepExecutor,
    build_step_runtime_context,
)
from zeta4s.dbt.graph import DbtGraph, DbtNode, dbt_project_dir, load_dbt_graph
from zeta4s.project.execution_plan import ExecutionPlan, ExecutionStep, StepOutput
from zeta4s.project.loader import ProjectContext
from zeta4s.project.step_graph import BUILTIN_STEP_TYPE_VALUES, StepGraphStep, StepTypeDescriptor
from zeta4s.project.step_types import registered_step_type_descriptor


# StepExecutor 를 조립하는 builder 시그니처. 모든 builder 는 균일하게
# (project, plan, step, common) 을 받는다. 일부 축은 builder 별로 쓰지 않을 수 있다.
StepExecutorBuilder = Callable[..., StepExecutor]


def supported_builtin_step_types() -> tuple[str, ...]:
    return SUPPORTED_BUILT_IN_STEP_TYPES


def built_in_step_executor(
    *,
    project: ProjectContext,
    plan: ExecutionPlan,
    plan_step: ExecutionStep,
    runtime_home: str,
) -> StepExecutor:
    step = plan_step.step
    common = {
        "runtime_home": runtime_home,
        "zeta4s_api_home": runtime_home,
    }
    builder = _BUILT_IN_STEP_EXECUTOR_BUILDERS.get(step.type)
    if builder is not None:
        return builder(project=project, plan=plan, step=step, common=common)
    descriptor = registered_step_type_descriptor(step.type)
    if descriptor is not None:
        return _external_executor(descriptor, project=project, plan=plan, step=step, common=common)
    return _unsupported_executor(project=project, step=step, common=common)


def _external_executor(
    descriptor: StepTypeDescriptor,
    *,
    project: ProjectContext,
    plan: ExecutionPlan,
    step: StepGraphStep,
    common: dict[str, Any],
) -> StepExecutor:
    payload = descriptor.payload_builder(project, plan, step, common)
    if not isinstance(payload, dict):
        raise ValueError(f"external step type payload_builder must return a dict: {descriptor.type}")
    return RuntimeCallableStepExecutor(
        descriptor.runtime_callable,
        payload,
        connection_ids=_external_connection_ids(step, descriptor),
    )


def _external_connection_ids(step: StepGraphStep, descriptor: StepTypeDescriptor) -> tuple[str, ...]:
    ids: list[str] = []
    for field_name in descriptor.connection_id_fields:
        value = getattr(step, field_name, None)
        if isinstance(value, str) and value:
            ids.append(value)
    return tuple(dict.fromkeys(ids))


def _noop_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    return NoopStepExecutor()


def _extract_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.rowset_extract:run_extract_rowset",
        {
            "source_conn": step.conn,
            "source": step.source or {},
            "output": step.output or {},
            "source_type": step.type.split(".", 1)[0],
            "project_root": str(project.root),
            "job_id": plan.job_id,
            "step_id": step.id,
            "params": step.params,
            "watermark": step.watermark,
            "time_window": step.time_window,
            "batch_size": step.batch_size,
            "project_timezone": project.timezone,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _native_sql_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    native_step, default_engine = _native_step_from_graph_step(step)
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.native:run_native_steps",
        {
            "project_root": str(project.root),
            "steps": [native_step],
            "default_conn": step.conn,
            "default_engine": default_engine,
            "job_id": plan.job_id,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _sql_scalar_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    native_step = step.model_dump(exclude_none=True)
    native_step["type"] = "scalar"
    native_step["name"] = step.id
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.native:run_sql_scalar",
        {
            "project_root": str(project.root),
            "step": native_step,
            "default_conn": step.conn,
            "default_engine": "auto",
            "job_id": plan.job_id,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _elasticsearch_command_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.backends.elasticsearch.command:run_elasticsearch_command",
        {
            "conn_id": step.conn,
            "operation": step.operation,
            "project_root": str(project.root),
            "source": step.source or {},
            "target": step.target or {},
            "body": step.body,
            "request": step.request or {},
            "refresh": step.refresh,
            "job_id": plan.job_id,
            "step_id": step.id,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _dbt_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    return DbtStepExecutor(
        project=project,
        step=step,
        resource_type="model" if step.type == "dbt.run" else "test",
        common=common,
        connection_ids=_step_connection_ids(step),
    )


def _unsupported_executor(*, project: ProjectContext, step: StepGraphStep, common: dict[str, Any]) -> StepExecutor:
    return RuntimeCallableStepExecutor(
        "zeta4s.core:run_unsupported_step",
        {"step_id": step.id, "step_type": step.type, "project_id": project.project_id, **common},
    )


class NoopStepExecutor:
    def execute(self, step: ExecutionStep, context) -> StepResult:
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED)


@dataclass(frozen=True)
class SequentialStepExecutor:
    executors: tuple[StepExecutor, ...]

    def execute(self, step: ExecutionStep, context) -> StepResult:
        outputs: dict[str, Any] = {}
        raw_results: list[Any] = []
        for executor in self.executors:
            result = executor.execute(step, context)
            if result.failed:
                return result
            outputs.update(result.outputs)
            if result.raw_result is not None:
                raw_results.append(result.raw_result)
        return StepResult(
            step_id=step.id,
            step_type=step.type,
            state=StepExecutionState.SUCCEEDED,
            outputs=outputs,
            raw_result={"status": "success", "details": {"results": raw_results}},
        )


@dataclass(frozen=True)
class DbtStepExecutor:
    project: ProjectContext
    step: StepGraphStep
    resource_type: str
    common: dict[str, Any]
    connection_ids: tuple[str, ...]

    def execute(self, step: ExecutionStep, context) -> StepResult:
        if not self.step.conn:
            raise ValueError(f"{self.step.type} step requires conn: {self.step.id}")
        project_dir = dbt_project_dir(self.project, self.step.conn)
        loaded_graph = load_dbt_graph(project_dir, _selectors(self.step))
        graph = DbtGraph(nodes=tuple(node for node in loaded_graph.nodes if node.resource_type == self.resource_type))
        if not graph.nodes:
            raise ValueError(f"{self.step.type} step selected no {self.resource_type} nodes: {self.step.id}")
        outputs: dict[str, Any] = {}
        for node in _topological_dbt_nodes(graph):
            result = RuntimeCallableStepExecutor(
                "zeta4s.runtime.dbt:run_dbt_node",
                {
                    "conn_id": self.step.conn,
                    "dbt_project_path": str(project_dir),
                    "unique_id": node.unique_id,
                    "resource_type": node.resource_type,
                    "node_name": node.name,
                    "command": node.command,
                    "project_id": self.project.project_id,
                    "runtime_context": {
                        **build_step_runtime_context(
                            step,
                            context,
                            task_id=_dbt_task_id(self.step.id, node.name),
                        ),
                        "dbt_target_id": self.step.id,
                    },
                    **self.common,
                },
                connection_ids=self.connection_ids,
            ).execute(step, context)
            if result.failed:
                return result
            outputs.update(result.outputs)
        return StepResult(step_id=step.id, step_type=step.type, state=StepExecutionState.SUCCEEDED, outputs=outputs)


def _dbt_task_id(step_id: str, node_name: str) -> str:
    return f"{step_id}.{node_name}"


def _step_connection_ids(step: StepGraphStep) -> tuple[str, ...]:
    ids: list[str] = []
    if step.conn:
        ids.append(step.conn)
    api = step.api or {}
    api_conn = api.get("conn") if isinstance(api, dict) else None
    if api_conn:
        ids.append(str(api_conn))
    return tuple(dict.fromkeys(ids))


def _native_step_from_graph_step(step: StepGraphStep) -> tuple[dict, str]:
    native_type_by_step_type = {
        "sql.check": ("check", "auto"),
        "oracle.sql": ("sql", "oracle"),
        "clickhouse.sql": ("sql", "clickhouse"),
        "oracle.call": ("call", "oracle"),
    }
    native_type, default_engine = native_type_by_step_type[step.type]
    payload = step.model_dump(exclude_none=True)
    payload["type"] = native_type
    payload["name"] = step.id
    return payload, default_engine


def _stage_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    items = list((step.map or {}).items())
    if not items:
        raise ValueError(f"{step.type} requires map: {step.id}")
    return SequentialStepExecutor(
        tuple(
            _stage_map_executor(
                project=project,
                plan=plan,
                step=step,
                source_ref=source_ref,
                target_table=target_table,
                common=common,
            )
            for source_ref, target_table in items
        )
    )


def _stage_map_executor(
    *,
    project: ProjectContext,
    plan: ExecutionPlan,
    step: StepGraphStep,
    source_ref: str,
    target_table: Any,
    common: dict[str, Any],
) -> StepExecutor:
    namespace, table = _split_table_name(str(target_table), label=f"{step.type}.map[{source_ref!r}]")
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.rowset_stage:run_stage_rowset",
        {
            "stage_type": step.type.split(".", 1)[0],
            "stage_conn": step.conn,
            "job_id": plan.job_id,
            "source_ref": str(source_ref),
            "target_table": table,
            "target_namespace": namespace,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _write_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    items = list((step.map or {}).items())
    if not items:
        raise ValueError(f"{step.type} requires map: {step.id}")
    return SequentialStepExecutor(
        tuple(
            _write_map_executor(
                project=project,
                plan=plan,
                step=step,
                source_ref=source_ref,
                target_spec=target_spec,
                common=common,
            )
            for source_ref, target_spec in items
        )
    )


def _write_map_executor(
    *,
    project: ProjectContext,
    plan: ExecutionPlan,
    step: StepGraphStep,
    source_ref: str,
    target_spec: Any,
    common: dict[str, Any],
) -> StepExecutor:
    target_type = step.type.split(".", 1)[0]
    target = dict(target_spec)
    namespace, table = _target_table(target, target_type)
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.write:run_write_rowset",
        {
            "target_type": target_type,
            "target_conn": step.conn,
            "source_ref": str(source_ref),
            "target_table": table,
            "target_namespace": namespace,
            "mode": target.get("mode"),
            "columns": target.get("columns") or [],
            "key": _write_key(target, target_type),
            "writer_options": _write_options(target, target_type),
            "job_id": plan.job_id,
            "write_name": _write_name(str(source_ref), target, target_type),
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _http_lookup_executor(
    *, project: ProjectContext, plan: ExecutionPlan, step: StepGraphStep, common: dict[str, Any]
) -> StepExecutor:
    lookup = step.lookup or {}
    target = step.target or {}
    api = dict(step.api or {})
    request = api.get("request") if isinstance(api.get("request"), dict) else {}
    response = api.get("response") if isinstance(api.get("response"), dict) else {}
    http = None
    if api:
        http = api
        http["conn"] = http.get("conn")
        if response.get("json_paths"):
            http["response_json_paths"] = response.get("json_paths")
        if http.get("method") == "POST":
            http["request_json_field"] = (
                request.get("json_field")
                or request.get("field")
                or http.get("request_json_field")
                or lookup.get("column")
            )
        else:
            http["request_query_param"] = (
                request.get("query_param") or http.get("request_query_param") or lookup.get("column")
            )
    return RuntimeCallableStepExecutor(
        "zeta4s.runtime.external_lookup:run_external_lookup",
        {
            "name": step.id,
            "mode": step.mode or ("http" if http else "mock"),
            "conn": step.conn,
            "source_table": _table_output_name(plan, str(lookup.get("table")), label=f"{step.type}.lookup.table"),
            "target_table": target.get("table"),
            "input_column": lookup.get("column"),
            "output_columns": response.get("columns") or [],
            "concurrency": step.concurrency or 1,
            "batch_size": step.batch_size or 1_000,
            "http": http,
            "project_id": project.project_id,
            **common,
        },
        connection_ids=_step_connection_ids(step),
    )


def _selectors(step: StepGraphStep) -> list[str]:
    if step.models:
        return [f"path:models/{model}.sql" for model in step.models]
    raise ValueError(f"{step.type} step requires models[]: {step.id}")


def _topological_dbt_nodes(graph: DbtGraph) -> tuple[DbtNode, ...]:
    nodes = graph.by_unique_id
    ordered: list[DbtNode] = []
    visited: set[str] = set()

    def visit(node: DbtNode) -> None:
        if node.unique_id in visited:
            return
        for dep in node.depends_on:
            if dep in nodes:
                visit(nodes[dep])
        visited.add(node.unique_id)
        ordered.append(node)

    for node in graph.nodes:
        visit(node)
    return tuple(ordered)


def _table_output_name(plan: ExecutionPlan, ref: str, *, label: str) -> str:
    output = _plan_output(plan, ref, label=label, required_kind="table")
    table = output.table_ref.table if output.table_ref else output.name
    return validate_table_identifier(table, label, max_parts=2)


def _plan_output(plan: ExecutionPlan, ref: str, *, label: str, required_kind: str | None = None) -> StepOutput:
    step_id, output_name = _step_output_ref(ref, label=label)
    upstream = plan.step_by_id.get(step_id)
    if upstream is None:
        raise ValueError(f"{label} references unknown step: {ref}")
    outputs = {output.name: output for output in upstream.outputs}
    output = outputs.get(output_name)
    if output is None:
        raise ValueError(f"{label} references unknown output: {ref}")
    if required_kind and output.kind != required_kind:
        raise ValueError(f"{label} requires {required_kind} output: {ref} kind={output.kind}")
    return output


def _step_output_ref(value: str, *, label: str) -> tuple[str, str]:
    if not isinstance(value, str) or "." not in value:
        raise ValueError(f"{label} must use <step_id>.<output_name>")
    step_id, output_name = value.split(".", 1)
    if not step_id.strip() or not output_name.strip():
        raise ValueError(f"{label} must use <step_id>.<output_name>")
    return step_id.strip(), output_name.strip()


def _split_table_name(value: str, *, label: str) -> tuple[str | None, str]:
    table = validate_table_identifier(value, label, max_parts=2)
    parts = table.split(".", 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]


def _target_table(target: dict, target_type: str) -> tuple[str | None, str | None]:
    if target_type == "elasticsearch":
        return None, None
    namespace, table = _split_table_name(str(target.get("table")), label=f"{target_type}.write.map.table")
    return namespace, table


def _write_options(target: dict, target_type: str) -> dict:
    if target_type == "clickhouse":
        return {
            "engine": target.get("engine"),
            "order_by": target.get("order_by"),
            "partition_by": target.get("partition_by"),
            "settings": target.get("settings"),
            "batch_size": target.get("batch_size") or WRITE_BATCH_SIZE_DEFAULT,
        }
    if target_type == "elasticsearch":
        return {
            "index": target.get("index"),
            "index_template": target.get("index_template"),
            "index_timezone": target.get("index_timezone"),
            "document_id": target.get("document_id"),
            "create_index_if_missing": target.get("create_index_if_missing"),
            "refresh": target.get("refresh"),
            "bulk_batch_size": target.get("batch_size") or WRITE_ELASTICSEARCH_BATCH_SIZE_DEFAULT,
        }
    return {"batch_size": target.get("batch_size") or WRITE_BATCH_SIZE_DEFAULT}


def _write_key(target: dict, target_type: str) -> list[str]:
    if target_type == "elasticsearch":
        document_id = target.get("document_id")
        if isinstance(document_id, dict):
            columns = document_id.get("columns")
            if isinstance(columns, list):
                return [str(column) for column in columns]
        key = target.get("key")
        if key is not None:
            return key if isinstance(key, list) else [str(key)]
        return []
    key = target.get("key")
    if key is None:
        return []
    return key if isinstance(key, list) else [str(key)]


def _write_name(source_ref: str, target: dict, target_type: str) -> str:
    return target.get("table") or target.get("index") or target.get("index_template") or source_ref


# built-in step type → StepExecutor builder. project 계층 STEP_TYPE 정본과 아래 가드로
# congruence 를 강제한다. 새 step type 은 이 registry 에 항목을 더해야 실행된다.
_BUILT_IN_STEP_EXECUTOR_BUILDERS: dict[str, StepExecutorBuilder] = {
    "noop": _noop_executor,
    "oracle.extract": _extract_executor,
    "clickhouse.extract": _extract_executor,
    "elasticsearch.extract": _extract_executor,
    "clickhouse.stage": _stage_executor,
    "oracle.stage": _stage_executor,
    "http.lookup": _http_lookup_executor,
    "dbt.run": _dbt_executor,
    "dbt.test": _dbt_executor,
    "sql.scalar": _sql_scalar_executor,
    "clickhouse.write": _write_executor,
    "oracle.write": _write_executor,
    "elasticsearch.write": _write_executor,
    "elasticsearch.command": _elasticsearch_command_executor,
    "sql.check": _native_sql_executor,
    "oracle.sql": _native_sql_executor,
    "clickhouse.sql": _native_sql_executor,
    "oracle.call": _native_sql_executor,
}

SUPPORTED_BUILT_IN_STEP_TYPES: tuple[str, ...] = tuple(_BUILT_IN_STEP_EXECUTOR_BUILDERS)

_unregistered_step_types = sorted(set(BUILTIN_STEP_TYPE_VALUES) - set(_BUILT_IN_STEP_EXECUTOR_BUILDERS))
_unknown_executor_step_types = sorted(set(_BUILT_IN_STEP_EXECUTOR_BUILDERS) - set(BUILTIN_STEP_TYPE_VALUES))
if _unregistered_step_types or _unknown_executor_step_types:
    raise RuntimeError(
        "built-in step executor registry diverges from step type registry: "
        f"missing_executor={_unregistered_step_types} unknown_type={_unknown_executor_step_types}"
    )
