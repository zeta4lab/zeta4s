"""Backend-independent execution plan for step graph jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from zeta4s.common.sql_identifiers import validate_table_identifier
from zeta4s.project.step_graph import (
    STEP_EXPR_REF_RE,
    STEP_TYPE_POOL_STAGES,
    ScheduleConfig,
    StepGraphJob,
    StepGraphStep,
)


@dataclass(frozen=True)
class TableRef:
    conn: str
    table: str


@dataclass(frozen=True)
class RowsetRef:
    step_id: str
    output_name: str
    format: str


@dataclass(frozen=True)
class StepOutputRef:
    step_id: str
    output_name: str


@dataclass(frozen=True)
class StepInput:
    name: str
    kind: str
    ref: dict[str, Any]
    table_ref: TableRef | None = None


@dataclass(frozen=True)
class StepOutput:
    name: str
    kind: str
    ref: dict[str, Any]
    table_ref: TableRef | None = None


@dataclass(frozen=True)
class StepDataBinding:
    downstream_id: str
    source: StepOutputRef
    target: dict[str, Any]
    field: str
    required_kind: str


@dataclass(frozen=True)
class FlowControl:
    depends_on: tuple[str, ...]
    when: dict[str, Any] | None
    join_rule: str
    retry: dict[str, Any] | None
    timeout: dict[str, Any] | None


@dataclass(frozen=True)
class RuntimeBinding:
    conn_id: str | None
    pool: str | None


@dataclass(frozen=True)
class StagingBackendRef:
    name: str


SINGLE_CONN_TRANSFORM_TYPES = {"dbt.run", "http.lookup"}


@dataclass(frozen=True)
class ExecutionStep:
    id: str
    type: str
    step: StepGraphStep
    inputs: tuple[StepInput, ...]
    outputs: tuple[StepOutput, ...]
    data_bindings: tuple[StepDataBinding, ...]
    flow: FlowControl
    runtime: RuntimeBinding
    staging_backend: StagingBackendRef | None


@dataclass(frozen=True)
class ExecutionEdge:
    upstream_id: str
    downstream_id: str
    kind: str


@dataclass(frozen=True)
class ExecutionPlan:
    job_id: str
    schedule: ScheduleConfig | None
    execution: Any
    defaults: Any
    steps: tuple[ExecutionStep, ...]
    edges: tuple[ExecutionEdge, ...]

    @property
    def step_by_id(self) -> dict[str, ExecutionStep]:
        return {step.id: step for step in self.steps}

    @property
    def upstream_ids_by_step(self) -> dict[str, tuple[str, ...]]:
        upstreams: dict[str, list[str]] = {step.id: [] for step in self.steps}
        for edge in self.control_edges:
            if edge.upstream_id not in upstreams[edge.downstream_id]:
                upstreams[edge.downstream_id].append(edge.upstream_id)
        return {step_id: tuple(ids) for step_id, ids in upstreams.items()}

    @property
    def root_step_ids(self) -> tuple[str, ...]:
        upstreams = self.upstream_ids_by_step
        return tuple(step.id for step in self.steps if not upstreams[step.id])

    @property
    def terminal_step_ids(self) -> tuple[str, ...]:
        non_terminal_ids = {edge.upstream_id for edge in self.control_edges}
        return tuple(step.id for step in self.steps if step.id not in non_terminal_ids)

    @property
    def control_edges(self) -> tuple[ExecutionEdge, ...]:
        return tuple(edge for edge in self.edges if edge.kind == "control")

    @property
    def data_edges(self) -> tuple[ExecutionEdge, ...]:
        return tuple(edge for edge in self.edges if edge.kind == "data")

    @property
    def data_bindings(self) -> tuple[StepDataBinding, ...]:
        return tuple(binding for step in self.steps for binding in step.data_bindings)


def build_step_graph_execution_plan(job: StepGraphJob) -> ExecutionPlan:
    steps = tuple(_plan_step(job, step) for step in job.steps)
    edges = _execution_edges(steps)
    plan = ExecutionPlan(
        job_id=job.job_id,
        schedule=job.schedule,
        execution=job.execution,
        defaults=job.defaults,
        steps=steps,
        edges=edges,
    )
    _validate_execution_plan(plan)
    return plan


def _execution_edges(steps: tuple[ExecutionStep, ...]) -> tuple[ExecutionEdge, ...]:
    edges: list[ExecutionEdge] = []
    seen: set[tuple[str, str, str]] = set()
    for step in steps:
        for upstream_id in _control_upstream_ids(step.step):
            _append_edge(edges, seen, upstream_id=upstream_id, downstream_id=step.id, kind="control")
        for binding in step.data_bindings:
            _append_edge(edges, seen, upstream_id=binding.source.step_id, downstream_id=step.id, kind="data")
    return tuple(edges)


def _append_edge(
    edges: list[ExecutionEdge],
    seen: set[tuple[str, str, str]],
    *,
    upstream_id: str,
    downstream_id: str,
    kind: str,
) -> None:
    key = (upstream_id, downstream_id, kind)
    if key in seen:
        return
    seen.add(key)
    edges.append(ExecutionEdge(upstream_id=upstream_id, downstream_id=downstream_id, kind=kind))


def _plan_step(job: StepGraphJob, step: StepGraphStep) -> ExecutionStep:
    outputs = _step_outputs(step)
    data_bindings = _step_data_bindings(step)
    return ExecutionStep(
        id=step.id,
        type=step.type,
        step=step,
        inputs=_step_inputs(step),
        outputs=outputs,
        data_bindings=data_bindings,
        flow=FlowControl(
            depends_on=tuple(step.depends_on),
            when=step.when.model_dump(exclude_none=True) if step.when else None,
            join_rule=step.join.rule if step.join else "all_success",
            retry=step.retry.model_dump(exclude_none=True) if step.retry else None,
            timeout=step.timeout.model_dump(exclude_none=True) if step.timeout else None,
        ),
        runtime=RuntimeBinding(
            conn_id=step.conn,
            pool=step.pool or _default_pool(job, step.type),
        ),
        staging_backend=_staging_backend(outputs),
    )


def _default_pool(job: StepGraphJob, step_type: str) -> str | None:
    pools = job.defaults.pools if job.defaults and job.defaults.pools else None
    if not pools:
        return None
    value = pools.model_extra.get(step_type)
    if value:
        return str(value)
    stage = STEP_TYPE_POOL_STAGES.get(step_type)
    value = pools.model_extra.get(stage) if stage else None
    return str(value) if value else None


def _upstream_ids(step: StepGraphStep) -> list[str]:
    upstream_ids: list[str] = []
    for upstream_id in step.depends_on:
        if upstream_id not in upstream_ids:
            upstream_ids.append(upstream_id)
    if step.when:
        for upstream_id in (step.when.success, step.when.failed):
            if upstream_id and upstream_id not in upstream_ids:
                upstream_ids.append(upstream_id)
        for upstream_id in _expr_refs(step.when.expr):
            if upstream_id not in upstream_ids:
                upstream_ids.append(upstream_id)
    return upstream_ids


def _control_upstream_ids(step: StepGraphStep) -> list[str]:
    return _upstream_ids(step)


def _expr_refs(expr: str | None) -> list[str]:
    if not expr:
        return []
    return [match.group(1) for match in STEP_EXPR_REF_RE.finditer(expr)]


def _step_inputs(step: StepGraphStep) -> tuple[StepInput, ...]:
    return ()


def _step_outputs(step: StepGraphStep) -> tuple[StepOutput, ...]:
    items = _canonical_step_outputs(step)
    _validate_unique_io_names([name for name, _kind, _ref, _table_ref in items], label=f"step {step.id} outputs")
    return tuple(StepOutput(name=name, kind=kind, ref=ref, table_ref=table_ref) for name, kind, ref, table_ref in items)


def _canonical_step_outputs(step: StepGraphStep) -> list[tuple[str, str, dict[str, Any], TableRef | None]]:
    if step.type in {"oracle.extract", "clickhouse.extract", "elasticsearch.extract"}:
        return _normalize_io_contract(step.output, label=f"step {step.id} output")
    if step.type in {"clickhouse.stage", "oracle.stage"}:
        return [
            (
                str(table),
                "table",
                {"kind": "table", "conn": step.conn, "table": str(table)},
                TableRef(
                    conn=str(step.conn),
                    table=validate_table_identifier(str(table), f"step {step.id} map target", max_parts=2),
                ),
            )
            for table in (step.map or {}).values()
        ]
    if step.type == "dbt.run":
        return [
            (
                str(model),
                "table",
                {"kind": "table", "conn": step.conn, "table": str(model)},
                TableRef(
                    conn=str(step.conn),
                    table=validate_table_identifier(str(model), f"step {step.id} models[]", max_parts=1),
                ),
            )
            for model in (step.models or [])
        ]
    if step.type == "http.lookup":
        target_table = str((step.target or {}).get("table"))
        return [
            (
                target_table,
                "table",
                {"kind": "table", "conn": step.conn, "table": target_table},
                TableRef(
                    conn=str(step.conn),
                    table=validate_table_identifier(target_table, f"step {step.id} target.table", max_parts=2),
                ),
            )
        ]
    if step.type == "sql.scalar":
        return _normalize_io_contract(step.outputs, label=f"step {step.id} outputs")
    return []


def _normalize_io_contract(
    value: dict[str, Any] | None, *, label: str
) -> list[tuple[str, str, dict[str, Any], TableRef | None]]:
    if value is None:
        return []
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    if not value:
        raise ValueError(f"{label} cannot be empty")
    if _looks_like_ref(value):
        return [_normalize_ref_item("default", value, label=label)]
    items = []
    for name, raw_ref in value.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{label} has empty item name")
        if not isinstance(raw_ref, dict):
            raise ValueError(f"{label}.{name} must be a mapping")
        items.append(_normalize_ref_item(name.strip(), raw_ref, label=f"{label}.{name}"))
    return items


def _looks_like_ref(value: dict[str, Any]) -> bool:
    ref_keys = {
        "kind",
        "backend",
        "namespace",
        "schema",
        "database",
        "name",
        "table",
        "model",
        "type",
        "step",
        "output",
        "path",
        "uri",
    }
    return bool(set(value) & ref_keys)


def _normalize_ref_item(
    name: str, value: dict[str, Any], *, label: str
) -> tuple[str, str, dict[str, Any], TableRef | None]:
    kind = str(value.get("kind") or _infer_kind(value)).strip().lower()
    if not kind:
        raise ValueError(f"{label}.kind is required")
    explicit_kind = str(value.get("kind") or "").strip().lower()
    if kind == "scalar" and "type" not in value and explicit_kind != "scalar":
        raise ValueError(f"{label} scalar output requires kind=scalar or type")
    if kind == "table" and not _is_step_output_ref(value) and not _table_ref_name(value):
        raise ValueError(f"{label} table reference requires table, name, or model")
    ref = dict(value)
    ref["kind"] = kind
    backend = ref.get("backend")
    if backend is not None:
        ref["backend"] = str(backend).strip().lower()
        if not ref["backend"]:
            raise ValueError(f"{label}.backend cannot be empty")
    table_ref = _normalize_table_ref(ref, label=label) if kind == "table" and not _is_step_output_ref(ref) else None
    return name, kind, ref, table_ref


def _normalize_table_ref(value: dict[str, Any], *, label: str) -> TableRef:
    raw_name = _table_ref_name(value)
    if not raw_name:
        raise ValueError(f"{label} table reference requires table, name, or model")
    table_name = validate_table_identifier(raw_name, f"{label}.table", max_parts=2)
    conn = value.get("conn")
    if not conn:
        raise ValueError(f"{label}.conn is required for table reference")
    return TableRef(conn=str(conn), table=table_name)


def _validate_unique_io_names(names: list[str], *, label: str) -> None:
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"{label} has duplicated item names: {', '.join(duplicates)}")


def _table_ref_name(value: dict[str, Any]) -> str | None:
    for key in ("table", "name", "model"):
        raw = value.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return None


def _is_step_output_ref(value: dict[str, Any]) -> bool:
    if (value.get("step") is None) != (value.get("output") is None):
        raise ValueError("step output reference requires both step and output")
    if value.get("step") is not None and value.get("output") is not None:
        return True
    if isinstance(value.get("ref"), str):
        ref = str(value["ref"]).strip()
        if ref.startswith("$steps.") and STEP_EXPR_REF_RE.fullmatch(ref) is None:
            raise ValueError("step output ref must use $steps.<id>.outputs.<name>")
        return STEP_EXPR_REF_RE.fullmatch(ref) is not None
    return False


def _infer_kind(value: dict[str, Any]) -> str:
    if any(key in value for key in ("table", "model", "schema", "database", "namespace")):
        return "table"
    if "type" in value:
        return "scalar"
    if any(key in value for key in ("path", "uri")):
        return "file"
    return "table"


def _staging_backend(outputs: tuple[StepOutput, ...]) -> StagingBackendRef | None:
    backends = sorted({str(output.table_ref.conn) for output in outputs if output.kind == "table" and output.table_ref})
    if not backends:
        return None
    if len(backends) > 1:
        raise ValueError(f"step outputs reference multiple staging backends: {', '.join(backends)}")
    return StagingBackendRef(backends[0])


def _step_data_bindings(step: StepGraphStep) -> tuple[StepDataBinding, ...]:
    bindings: list[StepDataBinding] = []
    if step.type in {"clickhouse.stage", "oracle.stage"}:
        for source_ref, target_table in (step.map or {}).items():
            bindings.append(
                StepDataBinding(
                    downstream_id=step.id,
                    source=_parse_output_ref(str(source_ref), label=f"step {step.id} map key"),
                    target={"kind": "table", "conn": step.conn, "table": str(target_table)},
                    field="map",
                    required_kind="rowset",
                )
            )
    if step.type == "http.lookup":
        lookup = step.lookup or {}
        target = step.target or {}
        bindings.append(
            StepDataBinding(
                downstream_id=step.id,
                source=_parse_output_ref(str(lookup.get("table")), label=f"step {step.id} lookup.table"),
                target={"kind": "table", "conn": step.conn, "table": str(target.get("table"))},
                field="lookup.table",
                required_kind="table",
            )
        )
    if step.type in {"clickhouse.write", "oracle.write", "elasticsearch.write"}:
        for source_ref, target_spec in (step.map or {}).items():
            bindings.append(
                StepDataBinding(
                    downstream_id=step.id,
                    source=_parse_output_ref(str(source_ref), label=f"step {step.id} map key"),
                    target=dict(target_spec) if isinstance(target_spec, dict) else {"target": target_spec},
                    field="map",
                    required_kind="rowset",
                )
            )
    return tuple(bindings)


def _parse_output_ref(value: str, *, label: str) -> StepOutputRef:
    if not isinstance(value, str) or "." not in value:
        raise ValueError(f"{label} must use <step_id>.<output_name>")
    step_id, output_name = value.split(".", 1)
    if not step_id.strip() or not output_name.strip():
        raise ValueError(f"{label} must use <step_id>.<output_name>")
    return StepOutputRef(step_id=step_id.strip(), output_name=output_name.strip())


def _validate_execution_plan(plan: ExecutionPlan) -> None:
    step_by_id = plan.step_by_id
    _validate_plan_edges(plan, step_by_id)
    _validate_acyclic_edges(plan)
    for plan_step in plan.steps:
        _validate_step_inputs(plan_step, step_by_id)
        _validate_step_data_bindings(plan_step, step_by_id)
        if plan_step.flow.when and plan_step.flow.when.get("expr"):
            _validate_when_expr_outputs(plan_step, step_by_id)
    if not plan.root_step_ids:
        raise ValueError(f"execution plan has no root step: {plan.job_id}")
    if not plan.terminal_step_ids:
        raise ValueError(f"execution plan has no terminal step: {plan.job_id}")


def _validate_plan_edges(plan: ExecutionPlan, step_by_id: dict[str, ExecutionStep]) -> None:
    valid_kinds = {"control", "data"}
    for edge in plan.edges:
        if edge.kind not in valid_kinds:
            raise ValueError(f"execution plan edge has unsupported kind: {edge.kind}")
        if edge.upstream_id not in step_by_id:
            raise ValueError(
                f"execution plan edge references unknown upstream step: {edge.upstream_id} -> {edge.downstream_id}"
            )
        if edge.downstream_id not in step_by_id:
            raise ValueError(
                f"execution plan edge references unknown downstream step: {edge.upstream_id} -> {edge.downstream_id}"
            )
        if edge.upstream_id == edge.downstream_id:
            raise ValueError(f"execution plan self edge is not allowed: {edge.upstream_id}")


def _validate_acyclic_edges(plan: ExecutionPlan) -> None:
    graph: dict[str, set[str]] = {step.id: set() for step in plan.steps}
    for edge in plan.edges:
        graph[edge.downstream_id].add(edge.upstream_id)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(step_id: str, stack: list[str]) -> None:
        if step_id in visited:
            return
        if step_id in visiting:
            cycle = stack[stack.index(step_id) :] + [step_id]
            raise ValueError("execution plan has a cycle: " + " -> ".join(cycle))
        visiting.add(step_id)
        stack.append(step_id)
        for upstream in sorted(graph.get(step_id, set())):
            visit(upstream, stack)
        stack.pop()
        visiting.remove(step_id)
        visited.add(step_id)

    for step_id in sorted(graph):
        visit(step_id, [])


def _validate_step_inputs(step: ExecutionStep, step_by_id: dict[str, ExecutionStep]) -> None:
    for step_input in step.inputs:
        source_step_id, output_name = _input_output_ref(step_input.ref)
        if not source_step_id:
            continue
        upstream = step_by_id.get(source_step_id)
        if upstream is None:
            raise ValueError(
                f"step {step.id} input {step_input.name} references unknown step: "
                f"$steps.{source_step_id}.outputs.{output_name or '<unknown>'}"
            )
        if not output_name:
            raise ValueError(f"step {step.id} input {step_input.name} step reference requires output")
        outputs_by_name = {output.name: output for output in upstream.outputs}
        output = outputs_by_name.get(output_name)
        if output is None:
            raise ValueError(
                f"step {step.id} input {step_input.name} references unknown output: "
                f"$steps.{source_step_id}.outputs.{output_name}"
            )
        if output.kind != step_input.kind:
            raise ValueError(
                f"step {step.id} input {step_input.name} kind mismatch: "
                f"$steps.{source_step_id}.outputs.{output_name} kind={output.kind}, input kind={step_input.kind}"
            )


def _validate_step_data_bindings(step: ExecutionStep, step_by_id: dict[str, ExecutionStep]) -> None:
    explicit_depends_on = set(step.flow.depends_on)
    for binding in step.data_bindings:
        upstream = step_by_id.get(binding.source.step_id)
        ref_label = f"{binding.source.step_id}.{binding.source.output_name}"
        if upstream is None:
            raise ValueError(f"step {step.id} {binding.field} references unknown step: {ref_label}")
        if binding.source.step_id not in explicit_depends_on:
            raise ValueError(
                f"step {step.id} {binding.field} references {binding.source.step_id} but it is not listed in depends_on"
            )
        outputs_by_name = {output.name: output for output in upstream.outputs}
        output = outputs_by_name.get(binding.source.output_name)
        if output is None:
            raise ValueError(f"step {step.id} {binding.field} references unknown output: {ref_label}")
        if output.kind != binding.required_kind:
            raise ValueError(
                f"step {step.id} {binding.field} kind mismatch: {ref_label} "
                f"kind={output.kind}, required={binding.required_kind}"
            )
        if step.type in SINGLE_CONN_TRANSFORM_TYPES and binding.required_kind == "table":
            if output.table_ref is None:
                raise ValueError(f"step {step.id} {binding.field} must reference a table output: {ref_label}")
            if output.table_ref.conn != step.step.conn:
                raise ValueError(
                    f"step {step.id} {binding.field} conn mismatch: {ref_label} "
                    f"conn={output.table_ref.conn}, step conn={step.step.conn}"
                )


def _input_output_ref(value: dict[str, Any]) -> tuple[str | None, str | None]:
    step_id = value.get("step")
    output_name = value.get("output")
    if (step_id is None) != (output_name is None):
        raise ValueError("step output reference requires both step and output")
    if step_id is None and isinstance(value.get("ref"), str):
        ref = str(value["ref"]).strip()
        if ref.startswith("$steps.") and STEP_EXPR_REF_RE.fullmatch(ref) is None:
            raise ValueError("step output ref must use $steps.<id>.outputs.<name>")
        match = STEP_EXPR_REF_RE.fullmatch(ref)
        if match:
            return match.group(1), match.group(2)
    if step_id is None:
        return None, None
    return str(step_id).strip(), str(output_name).strip() if output_name is not None else None


def _validate_when_expr_outputs(step: ExecutionStep, step_by_id: dict[str, ExecutionStep]) -> None:
    expr = str(step.flow.when["expr"])
    for match in STEP_EXPR_REF_RE.finditer(expr):
        upstream_id = match.group(1)
        output_name = match.group(2)
        upstream = step_by_id[upstream_id]
        outputs_by_name = {output.name: output for output in upstream.outputs}
        output = outputs_by_name.get(output_name)
        if output is None:
            raise ValueError(
                f"step {step.id} when.expr references unknown output: $steps.{upstream_id}.outputs.{output_name}"
            )
        if output.kind != "scalar":
            raise ValueError(
                f"step {step.id} when.expr references non-scalar output: "
                f"$steps.{upstream_id}.outputs.{output_name} kind={output.kind}"
            )
