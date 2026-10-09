"""Step graph job schema validation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from zeta4s.common.sql_identifiers import validate_sql_identifier, validate_table_identifier


JOB_CONFIG_SUFFIXES = (".yml", ".yaml")
JOB_ID_RE = re.compile(r"^[a-z0-9_-]+$")
STEP_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
STEP_EXPR_REF_RE = re.compile(r"\$steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z_][A-Za-z0-9_]*)")
STEP_EXPR_RE = re.compile(
    r"^\s*\$steps\.([A-Za-z0-9_-]+)\.outputs\.([A-Za-z_][A-Za-z0-9_]*)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$"
)

PROJECT_POOL_STAGES = ("extract", "stage", "transform", "write")
# STEP_TYPE_VALUES / STEP_TYPE_POOL_STAGES / _STEP_TYPE_VALIDATORS 는 파일 하단
# _STEP_TYPE_SPECS registry 에서 파생한다. registry 가 built-in step type 의 정본이다.


def validate_job_id(value: Any, label: str = "job_id") -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} 은 비어 있을 수 없다.")
    normalized = value.strip()
    if not JOB_ID_RE.fullmatch(normalized):
        raise ValueError(f"{label} 는 lowercase letters, digits, underscore, hyphen 만 사용할 수 있다.")
    return normalized


def validate_step_id(value: Any, label: str = "step_id") -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} 은 비어 있을 수 없다.")
    normalized = value.strip()
    if not STEP_ID_RE.fullmatch(normalized):
        raise ValueError(f"{label} 는 letters, digits, underscore, hyphen 만 사용할 수 있다.")
    return normalized


def is_step_graph_config_path(path: Path) -> bool:
    return path.suffix in JOB_CONFIG_SUFFIXES


def validate_step_graph_config_path(path: Path) -> None:
    if is_step_graph_config_path(path):
        return
    raise ValueError(f"job config file must be a .yml or .yaml file: {path.name}")


def _job_config_candidates(jobs_dir: Path) -> list[Path]:
    return sorted({path for suffix in JOB_CONFIG_SUFFIXES for path in jobs_dir.glob(f"*{suffix}")})


def step_graph_config_paths(jobs_dir: Path) -> list[Path]:
    return [path for path in _job_config_candidates(jobs_dir) if is_step_graph_config_path(path)]


def unsupported_config_paths(jobs_dir: Path) -> list[Path]:
    if not jobs_dir.exists():
        return []
    return sorted(
        path for path in _job_config_candidates(jobs_dir) if path.is_file() and not is_step_graph_config_path(path)
    )


def _validate_ref(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} 은 비어 있을 수 없다.")
    normalized = value.strip().replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field} 은 project 내부 상대 경로여야 한다.")
    return path.as_posix()


class StepGraphPoolDefaults(BaseModel):
    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def _v_pool_keys(self) -> "StepGraphPoolDefaults":
        allowed = set(STEP_TYPE_VALUES) | set(PROJECT_POOL_STAGES)
        invalid = sorted(key for key in self.model_extra if key not in allowed)
        if invalid:
            raise ValueError(
                "defaults.pools key must be a canonical step type or project pool stage: " + ", ".join(invalid)
            )
        return self


class ExecutionConfig(BaseModel):
    pools: dict[str, str] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}

    @field_validator("pools")
    @classmethod
    def _v_pools(cls, value: dict[str, str]) -> dict[str, str]:
        normalized = {}
        for stage, pool in value.items():
            stage = validate_sql_identifier(stage, "execution.pools key")
            if stage not in PROJECT_POOL_STAGES:
                raise ValueError("execution.pools key must be one of " + ", ".join(PROJECT_POOL_STAGES) + f": {stage}")
            normalized[stage] = str(pool)
        return normalized


class StepGraphDefaults(BaseModel):
    pools: StepGraphPoolDefaults | None = None

    model_config = {"extra": "forbid"}


class StepGraphWhen(BaseModel):
    success: str | None = None
    failed: str | None = None
    expr: str | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def _v_condition(self) -> "StepGraphWhen":
        conditions = [bool(self.success), bool(self.failed), bool(self.expr)]
        if sum(conditions) > 1:
            raise ValueError("when 은 success, failed, expr 중 하나만 사용할 수 있다.")
        if self.expr and not STEP_EXPR_RE.match(self.expr):
            raise ValueError("when.expr 은 $steps.<id>.outputs.<name> <op> <literal> 형식이어야 한다.")
        return self


class StepGraphJoin(BaseModel):
    rule: Literal["all_success", "none_failed_min_one_success", "all_done"] = "all_success"

    model_config = {"extra": "forbid"}


class StepGraphRetry(BaseModel):
    max_attempts: int = Field(default=1, ge=1)
    delay_seconds: int = Field(default=300, ge=0)

    model_config = {"extra": "forbid"}


class StepGraphTimeout(BaseModel):
    seconds: int = Field(gt=0)

    model_config = {"extra": "forbid"}


def _step_type_json_schema(schema: dict[str, Any]) -> None:
    # STEP_TYPE Literal 을 str + membership 검증으로 바꾸면서 JSON Schema 에서 enum 이
    # 사라지지 않도록, registry 정본(STEP_TYPE_VALUES)에서 enum 을 파생해 주입한다.
    # 이 콜백은 model_json_schema() 호출 시점(runtime)에 실행되므로 하단 registry 를 본다.
    schema["enum"] = list(STEP_TYPE_VALUES)


class StepGraphStep(BaseModel):
    step_id: str
    display_name: str | None = None
    type: str = Field(json_schema_extra=_step_type_json_schema)
    depends_on: list[str] = Field(default_factory=list)
    conn: str | None = None
    pool: str | None = None
    when: StepGraphWhen | None = None
    join: StepGraphJoin | None = None
    retry: StepGraphRetry | None = None
    timeout: StepGraphTimeout | None = None
    source: dict[str, Any] | None = None
    target: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    map: dict[str, Any] | None = None
    lookup: dict[str, Any] | None = None
    models: list[str] | None = None
    sql: str | None = None
    query: str | None = None
    call: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    mode: str | None = None
    batch_size: int | None = None
    concurrency: int | None = None
    api: dict[str, Any] | None = None
    operation: str | None = None
    request: dict[str, Any] | None = None
    body: dict[str, Any] | None = None
    refresh: bool | str | None = None
    watermark: dict[str, Any] | None = None
    time_window: dict[str, Any] | None = None

    model_config = {"extra": "forbid"}

    @property
    def id(self) -> str:
        return self.step_id

    @field_validator("step_id")
    @classmethod
    def _v_step_id(cls, value: str) -> str:
        return validate_step_id(value, "steps[].step_id")

    @field_validator("display_name")
    @classmethod
    def _v_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("conn", "pool")
    @classmethod
    def _v_optional_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("conn/pool 은 비어 있을 수 없다.")
        return value

    @field_validator("query")
    @classmethod
    def _v_query_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_ref(value, "steps[].query")

    @field_validator("type")
    @classmethod
    def _v_type(cls, value: str) -> str:
        if value not in STEP_TYPE_VALUES:
            raise ValueError("steps[].type must be one of " + ", ".join(sorted(STEP_TYPE_VALUES)) + f": {value}")
        return value

    @model_validator(mode="after")
    def _v_type_contract(self) -> "StepGraphStep":
        _STEP_TYPE_VALIDATORS[self.type](self)
        if self.type != "elasticsearch.command":
            _reject_fields(self, "operation", "request", "body", "refresh")
        return self


def _require_conn(step: StepGraphStep) -> None:
    if not step.conn:
        raise ValueError(f"step {step.id} type={step.type} requires conn")


def _require_sql_or_query(step: StepGraphStep) -> None:
    if not (step.sql or step.query):
        raise ValueError(f"step {step.id} type={step.type} requires sql or query")


def _require_mapping(value: dict[str, Any] | None, field: str, step: StepGraphStep) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"step {step.id} type={step.type} requires {field}")
    return value


def _reject_fields(step: StepGraphStep, *field_names: str) -> None:
    for field_name in field_names:
        value = getattr(step, field_name)
        if value is not None and value != {} and value != []:
            raise ValueError(f"step {step.id} type={step.type} does not use {field_name}")


def _validate_output_rowset(step: StepGraphStep) -> None:
    output = _require_mapping(step.output, "output", step)
    if len(output) != 1:
        raise ValueError(f"step {step.id} type={step.type} requires exactly one output")
    for name, spec in output.items():
        validate_sql_identifier(str(name), f"{step.type}.output name")
        if not isinstance(spec, dict):
            raise ValueError(f"step {step.id} output.{name} must be a mapping")
        if spec.get("kind") != "rowset":
            raise ValueError(f"step {step.id} output.{name}.kind must be rowset")
        if "format" in spec:
            raise ValueError(f"step {step.id} output.{name} does not allow format")
        unknown = sorted(set(spec) - {"kind"})
        if unknown:
            raise ValueError(f"step {step.id} output.{name} has unsupported fields: {', '.join(unknown)}")


def _validate_extract(step: StepGraphStep) -> None:
    _require_conn(step)
    source = _require_mapping(step.source, "source", step)
    if step.watermark and step.time_window:
        raise ValueError(f"step {step.id} type={step.type} cannot use watermark and time_window together")
    _validate_extract_watermark(step)
    _validate_extract_time_window(step)
    source_kind = source.get("kind")
    allowed_kinds = {"search"} if step.type == "elasticsearch.extract" else {"table", "query"}
    if source_kind not in allowed_kinds:
        raise ValueError(
            f"step {step.id} type={step.type} requires source.kind one of {', '.join(sorted(allowed_kinds))}"
        )
    if source_kind == "table":
        table = source.get("table")
        if not table:
            raise ValueError(f"step {step.id} type={step.type} source.kind=table requires source.table")
        validate_table_identifier(str(table), f"{step.type}.source.table", max_parts=2)
    if source_kind == "query" and not source.get("query"):
        raise ValueError(f"step {step.id} type={step.type} source.kind=query requires source.query")
    if source_kind == "search" and not (source.get("index") or source.get("index_template")):
        raise ValueError(
            f"step {step.id} type={step.type} source.kind=search requires source.index or source.index_template"
        )
    if source_kind == "search":
        fields = source.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ValueError(f"step {step.id} type={step.type} source.kind=search requires source.fields[]")
        _validate_elasticsearch_extract_fields(step, fields)
    _validate_output_rowset(step)
    _reject_fields(step, "target", "outputs", "map", "lookup", "mode")


def _validate_elasticsearch_extract_fields(step: StepGraphStep, fields: list[Any]) -> None:
    allowed_types = {"bool", "int", "float", "decimal", "str", "date", "timestamp"}
    for index, field in enumerate(fields, start=1):
        label = f"step {step.id} source.fields[{index}]"
        if isinstance(field, str):
            validate_sql_identifier(field, f"{label}")
            continue
        if not isinstance(field, dict):
            raise ValueError(f"{label} must be a string or mapping")
        column = field.get("column") or field.get("name")
        if not column:
            raise ValueError(f"{label} requires column")
        validate_sql_identifier(str(column), f"{label}.column")
        field_type = str(field.get("type") or "str").strip()
        if field_type not in allowed_types:
            raise ValueError(f"{label}.type must be one of {', '.join(sorted(allowed_types))}: {field_type}")
        for numeric_key in ("precision", "scale", "datetime_precision"):
            if field.get(numeric_key) is not None:
                value = int(field[numeric_key])
                if value < 0:
                    raise ValueError(f"{label}.{numeric_key} must be non-negative")
        mode = field.get("mode")
        if mode is not None and mode not in {"scalar", "json_string"}:
            raise ValueError(f"{label}.mode must be scalar or json_string")


def _validate_extract_watermark(step: StepGraphStep) -> None:
    if not step.watermark:
        return
    watermark = _require_mapping(step.watermark, "watermark", step)
    if not watermark.get("column"):
        raise ValueError(f"step {step.id} type={step.type} watermark requires column")
    validate_sql_identifier(str(watermark["column"]), f"{step.type}.watermark.column")
    if watermark.get("overlap_window") is not None:
        _validate_short_interval(
            str(watermark["overlap_window"]), f"{step.type}.watermark.overlap_window", allow_zero=True
        )


def _validate_extract_time_window(step: StepGraphStep) -> None:
    if not step.time_window:
        return
    time_window = _require_mapping(step.time_window, "time_window", step)
    if not time_window.get("column"):
        raise ValueError(f"step {step.id} type={step.type} time_window requires column")
    validate_sql_identifier(str(time_window["column"]), f"{step.type}.time_window.column")
    lookback = time_window.get("lookback") or time_window.get("lookback_window")
    if lookback is None:
        raise ValueError(f"step {step.id} type={step.type} time_window requires lookback")
    _validate_short_interval(str(lookback), f"{step.type}.time_window.lookback", allow_zero=False)


def _validate_short_interval(value: str, label: str, *, allow_zero: bool) -> None:
    match = re.fullmatch(r"(\d+)([sm])", value.strip())
    if not match:
        raise ValueError(f"{label} must use <integer>s or <integer>m")
    amount = int(match.group(1))
    if amount < 0 or (amount == 0 and not allow_zero):
        raise ValueError(f"{label} must be greater than zero")


def _validate_stage(step: StepGraphStep) -> None:
    _require_conn(step)
    mappings = _require_mapping(step.map, "map", step)
    for ref, table in mappings.items():
        if not isinstance(ref, str) or "." not in ref:
            raise ValueError(f"step {step.id} map key must use <upstream_step>.<output_name>: {ref}")
        validate_table_identifier(str(table), f"{step.type}.map[{ref!r}]", max_parts=2)
    _reject_fields(step, "source", "target", "output", "outputs", "lookup", "mode")


def _validate_sql(step: StepGraphStep) -> None:
    _require_conn(step)
    _require_sql_or_query(step)
    _reject_fields(step, "source", "target", "output", "map", "lookup", "mode")
    if step.type != "sql.scalar":
        _reject_fields(step, "outputs")


def _validate_sql_scalar(step: StepGraphStep) -> None:
    _validate_sql(step)
    outputs = _require_mapping(step.outputs, "outputs", step)
    allowed_types = {"int", "float", "bool", "str"}
    for name, spec in outputs.items():
        validate_sql_identifier(str(name), "sql.scalar.outputs name")
        if not isinstance(spec, dict):
            raise ValueError(f"step {step.id} outputs.{name} must be a mapping")
        if spec.get("kind") != "scalar":
            raise ValueError(f"step {step.id} outputs.{name}.kind must be scalar")
        scalar_type = spec.get("type")
        if scalar_type not in allowed_types:
            raise ValueError(f"step {step.id} outputs.{name}.type must be one of {', '.join(sorted(allowed_types))}")
        column = spec.get("column", 1)
        if not isinstance(column, int) or column < 1:
            raise ValueError(f"step {step.id} outputs.{name}.column must be a positive integer")


def _validate_oracle_call(step: StepGraphStep) -> None:
    _require_conn(step)
    if not step.call:
        raise ValueError(f"step {step.id} type=oracle.call requires call")
    _reject_fields(step, "source", "target", "output", "outputs", "map", "lookup", "mode")


def _validate_dbt(step: StepGraphStep) -> None:
    _require_conn(step)
    if not isinstance(step.models, list) or not step.models:
        raise ValueError(f"step {step.id} type={step.type} requires models[]")
    for index, model in enumerate(step.models):
        validate_sql_identifier(str(model), f"{step.type}.models[{index}]")
    _reject_fields(step, "source", "target", "output", "outputs", "map", "lookup", "mode")


def _validate_http_lookup(step: StepGraphStep) -> None:
    _require_conn(step)
    lookup = _require_mapping(step.lookup, "lookup", step)
    target = _require_mapping(step.target, "target", step)
    api = _require_mapping(step.api, "api", step)
    lookup_table = lookup.get("table")
    lookup_column = lookup.get("column")
    if not isinstance(lookup_table, str) or "." not in lookup_table:
        raise ValueError(f"step {step.id} type=http.lookup requires lookup.table as <upstream_step>.<output_name>")
    if not lookup_column:
        raise ValueError(f"step {step.id} type=http.lookup requires lookup.column")
    validate_sql_identifier(str(lookup_column), "http.lookup.lookup.column")
    if not target.get("table"):
        raise ValueError(f"step {step.id} type=http.lookup requires target.table")
    validate_table_identifier(str(target["table"]), "http.lookup.target.table", max_parts=2)
    if not api.get("conn"):
        raise ValueError(f"step {step.id} type=http.lookup requires api.conn")
    if not api.get("method"):
        raise ValueError(f"step {step.id} type=http.lookup requires api.method")
    response = api.get("response")
    columns = response.get("columns") if isinstance(response, dict) else None
    if not isinstance(columns, list) or not columns:
        raise ValueError(f"step {step.id} type=http.lookup requires api.response.columns[]")
    for index, column in enumerate(columns):
        if not isinstance(column, dict):
            raise ValueError(f"step {step.id} api.response.columns[{index}] must be a mapping")
        if not column.get("name") or not column.get("type"):
            raise ValueError(f"step {step.id} api.response.columns[{index}] requires name and type")
        validate_sql_identifier(str(column["name"]), f"http.lookup.api.response.columns[{index}].name")
    _reject_fields(step, "source", "output", "outputs", "map", "params", "mode")


def _validate_write(step: StepGraphStep) -> None:
    _require_conn(step)
    if step.mode is not None:
        raise ValueError(f"step {step.id} type={step.type} does not support top-level mode; use map.*.mode")
    mappings = _require_mapping(step.map, "map", step)
    if len(mappings) != 1:
        raise ValueError(f"step {step.id} type={step.type} requires exactly one map entry")
    for ref, target_spec in mappings.items():
        if not isinstance(ref, str) or "." not in ref:
            raise ValueError(f"step {step.id} map key must use <upstream_step>.<output_name>: {ref}")
        if not isinstance(target_spec, dict):
            raise ValueError(f"step {step.id} map.{ref} target spec must be a mapping")
        _validate_write_target_keys(step, ref, target_spec)
        columns = target_spec.get("columns")
        if not isinstance(columns, list) or not columns:
            raise ValueError(f"step {step.id} map.{ref} requires columns[]")
        mode = str(target_spec.get("mode") or "").strip()
        if not mode:
            raise ValueError(f"step {step.id} map.{ref} requires mode")
        if mode == "merge":
            raise ValueError(f"step {step.id} map.{ref} does not support mode=merge; use mode=upsert")
        if mode not in {"replace", "append", "upsert"}:
            raise ValueError(f"step {step.id} map.{ref}.mode must be replace, append or upsert")
        if step.type in {"clickhouse.write", "oracle.write"}:
            if not target_spec.get("table"):
                raise ValueError(f"step {step.id} map.{ref} requires table")
            validate_table_identifier(str(target_spec["table"]), f"{step.type}.map.table", max_parts=2)
            if mode == "upsert" and not target_spec.get("key"):
                raise ValueError(f"step {step.id} map.{ref} mode=upsert requires key[]")
        if step.type == "elasticsearch.write":
            if bool(target_spec.get("index")) == bool(target_spec.get("index_template")):
                raise ValueError(f"step {step.id} map.{ref} requires exactly one of index or index_template")
            if mode == "upsert" and not _elasticsearch_write_has_document_id_columns(target_spec):
                raise ValueError(f"step {step.id} map.{ref} mode=upsert requires key[] or document_id columns")
    _reject_fields(step, "source", "target", "output", "outputs", "lookup")


def _validate_write_target_keys(step: StepGraphStep, ref: str, target_spec: dict[str, Any]) -> None:
    common = {"mode", "columns", "batch_size"}
    if step.type == "clickhouse.write":
        allowed = common | {"table", "key", "engine", "order_by", "partition_by", "settings"}
    elif step.type == "oracle.write":
        allowed = common | {"table", "key"}
    elif step.type == "elasticsearch.write":
        allowed = common | {
            "index",
            "index_template",
            "key",
            "document_id",
            "refresh",
            "create_index_if_missing",
            "index_timezone",
        }
    else:
        allowed = common
    invalid = sorted(str(key) for key in target_spec if key not in allowed)
    if invalid:
        raise ValueError(f"step {step.id} map.{ref} unsupported keys: {', '.join(invalid)}")


def _elasticsearch_write_has_document_id_columns(target_spec: dict[str, Any]) -> bool:
    key = target_spec.get("key")
    if key:
        values = key if isinstance(key, list) else [key]
        for value in values:
            validate_sql_identifier(str(value), "elasticsearch.write.key[]")
        return True
    document_id = target_spec.get("document_id")
    if isinstance(document_id, str):
        validate_sql_identifier(document_id, "elasticsearch.write.document_id")
        return True
    if not isinstance(document_id, dict):
        return False
    mode = str(document_id.get("mode") or "columns").strip()
    if mode != "columns":
        return False
    columns = document_id.get("columns")
    values = [columns] if isinstance(columns, str) else columns
    if not isinstance(values, list) or not values:
        return False
    for value in values:
        validate_sql_identifier(str(value), "elasticsearch.write.document_id.columns[]")
    return True


def _validate_elasticsearch_command(step: StepGraphStep) -> None:
    _require_conn(step)
    if not step.operation:
        raise ValueError(f"step {step.id} type=elasticsearch.command requires operation")
    operation = step.operation
    if operation not in {"bulk", "reindex", "update_by_query", "delete_by_query", "request"}:
        raise ValueError(
            f"step {step.id} type=elasticsearch.command operation must be one of "
            "bulk, delete_by_query, reindex, request, update_by_query"
        )
    if operation == "bulk":
        source = _require_mapping(step.source, "source", step)
        if not source.get("file"):
            raise ValueError(f"step {step.id} operation=bulk requires source.file")
        if source.get("format") != "ndjson":
            raise ValueError(f"step {step.id} operation=bulk requires source.format=ndjson")
        _reject_fields(step, "body", "request")
    if operation == "reindex":
        body = _require_mapping(step.body, "body", step)
        if not isinstance(body.get("source"), dict) or not isinstance(body.get("dest"), dict):
            raise ValueError(f"step {step.id} operation=reindex requires body.source and body.dest")
        _reject_fields(step, "source", "target", "request")
    if operation == "update_by_query":
        target = _require_mapping(step.target, "target", step)
        body = _require_mapping(step.body, "body", step)
        if not (target.get("index") or target.get("index_template")):
            raise ValueError(f"step {step.id} operation=update_by_query requires target.index or target.index_template")
        if not isinstance(body.get("query"), dict) or not isinstance(body.get("script"), dict):
            raise ValueError(f"step {step.id} operation=update_by_query requires body.query and body.script")
        _reject_fields(step, "source", "request")
    if operation == "delete_by_query":
        target = _require_mapping(step.target, "target", step)
        body = _require_mapping(step.body, "body", step)
        if not (target.get("index") or target.get("index_template")):
            raise ValueError(f"step {step.id} operation=delete_by_query requires target.index or target.index_template")
        if not isinstance(body.get("query"), dict):
            raise ValueError(f"step {step.id} operation=delete_by_query requires body.query")
        _reject_fields(step, "source", "request")
    if operation == "request":
        request = _require_mapping(step.request, "request", step)
        method = request.get("method")
        path = request.get("path")
        if not isinstance(method, str) or not method.strip():
            raise ValueError(f"step {step.id} operation=request requires request.method")
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError(f"step {step.id} operation=request requires request.path starting with /")
        _reject_fields(step, "source", "target", "body")
    _reject_fields(step, "output", "outputs", "map", "lookup", "mode", "params", "api")


def _validate_noop(step: StepGraphStep) -> None:
    if step.conn:
        raise ValueError(f"step {step.id} type=noop does not use conn")
    _reject_fields(step, "source", "target", "output", "outputs", "map", "lookup", "mode")


@dataclass(frozen=True)
class StepTypeSpec:
    """Built-in step type 의 정본 선언.

    한 step type 의 선언 축(pool stage, schema 검증)을 한 곳에 모은다. STEP_TYPE 목록,
    pool stage 매핑, schema validator 매핑은 모두 이 registry 에서 파생한다. 실행 축(runtime
    callable / executor 조립)은 core 계층에 있고, core 가 이 registry 의 type 집합에 대해
    import-time 합치성 가드로 congruence 를 강제한다.
    """

    type: str
    pool_stage: str | None
    schema_validator: Callable[["StepGraphStep"], None]


@dataclass(frozen=True)
class StepTypeDescriptor:
    """외부(비 built-in) step type 의 등록 정본.

    built-in 은 선언 축(StepTypeSpec)과 실행 축(core executor builder)이 분리돼 있지만,
    외부 type 은 core 를 모른 채 등록돼야 한다. 그래서 descriptor 하나가 선언 축(pool_stage,
    schema_validator)과 실행 축(runtime_callable + payload_builder)을 함께 담는다.

    scheduler·core 중립: runtime_callable 은 `"module:func"` dotted string 이고 실제 해석은
    실행 시점 core `_resolve_runtime_callable` 이 담당한다. payload_builder 는 순수 dict 를
    만든다. 이 descriptor 는 project 계층에 있어 airflow/prefect/core 를 import 하지 않는다.
    """

    type: str
    pool_stage: str | None
    schema_validator: Callable[["StepGraphStep"], None]
    runtime_callable: str
    payload_builder: Callable[..., dict[str, Any]]
    connection_id_fields: tuple[str, ...] = ("conn",)


_STEP_TYPE_SPECS: tuple[StepTypeSpec, ...] = (
    StepTypeSpec("oracle.extract", "extract", _validate_extract),
    StepTypeSpec("clickhouse.extract", "extract", _validate_extract),
    StepTypeSpec("elasticsearch.extract", "extract", _validate_extract),
    StepTypeSpec("clickhouse.stage", "stage", _validate_stage),
    StepTypeSpec("oracle.stage", "stage", _validate_stage),
    StepTypeSpec("clickhouse.sql", "transform", _validate_sql),
    StepTypeSpec("oracle.sql", "transform", _validate_sql),
    StepTypeSpec("sql.check", "transform", _validate_sql),
    StepTypeSpec("sql.scalar", "transform", _validate_sql_scalar),
    StepTypeSpec("oracle.call", "transform", _validate_oracle_call),
    StepTypeSpec("dbt.run", "transform", _validate_dbt),
    StepTypeSpec("dbt.test", "transform", _validate_dbt),
    StepTypeSpec("http.lookup", "transform", _validate_http_lookup),
    StepTypeSpec("clickhouse.write", "write", _validate_write),
    StepTypeSpec("oracle.write", "write", _validate_write),
    StepTypeSpec("elasticsearch.write", "write", _validate_write),
    StepTypeSpec("elasticsearch.command", "write", _validate_elasticsearch_command),
    StepTypeSpec("noop", None, _validate_noop),
)

# built-in 정본 파생물. register 로 안 바뀌는 frozen 기준이며 congruence 가드가 이걸 쓴다.
BUILTIN_STEP_TYPE_VALUES: tuple[str, ...] = tuple(spec.type for spec in _STEP_TYPE_SPECS)
_BUILTIN_POOL_STAGES: dict[str, str] = {
    spec.type: spec.pool_stage for spec in _STEP_TYPE_SPECS if spec.pool_stage is not None
}
_BUILTIN_VALIDATORS: dict[str, Callable[["StepGraphStep"], None]] = {
    spec.type: spec.schema_validator for spec in _STEP_TYPE_SPECS
}

if len(BUILTIN_STEP_TYPE_VALUES) != len(set(BUILTIN_STEP_TYPE_VALUES)):
    _duplicated = sorted({t for t in BUILTIN_STEP_TYPE_VALUES if BUILTIN_STEP_TYPE_VALUES.count(t) > 1})
    raise RuntimeError("duplicated step type in registry: " + ", ".join(_duplicated))

# 활성(built-in + 외부 등록) 파생 테이블. pydantic validator(_v_type/_v_type_contract),
# JSON schema enum(_step_type_json_schema), pool stage 소비처(execution_plan/pools)가 읽는다.
# 외부 소비처가 STEP_TYPE_POOL_STAGES 를 이름으로 import 하므로, register 시 dict 는 in-place
# 로 갱신하고 STEP_TYPE_VALUES(tuple)만 rebind 한다.
STEP_TYPE_VALUES: tuple[str, ...] = BUILTIN_STEP_TYPE_VALUES
STEP_TYPE_POOL_STAGES: dict[str, str] = dict(_BUILTIN_POOL_STAGES)
_STEP_TYPE_VALIDATORS: dict[str, Callable[["StepGraphStep"], None]] = dict(_BUILTIN_VALIDATORS)


def rebuild_step_type_tables(external: dict[str, StepTypeDescriptor]) -> None:
    """활성 파생 테이블을 built-in + 외부 등록 descriptor 로 재구성한다.

    project/step_types.py 의 register 가 호출한다. dict 는 in-place 로 갱신해 이름으로
    import 한 소비처(execution_plan/pools)까지 반영되게 하고, tuple 은 rebind 한다.
    """
    global STEP_TYPE_VALUES
    STEP_TYPE_VALUES = BUILTIN_STEP_TYPE_VALUES + tuple(external)
    STEP_TYPE_POOL_STAGES.clear()
    STEP_TYPE_POOL_STAGES.update(_BUILTIN_POOL_STAGES)
    STEP_TYPE_POOL_STAGES.update(
        {step_type: descriptor.pool_stage for step_type, descriptor in external.items() if descriptor.pool_stage}
    )
    _STEP_TYPE_VALIDATORS.clear()
    _STEP_TYPE_VALIDATORS.update(_BUILTIN_VALIDATORS)
    _STEP_TYPE_VALIDATORS.update({step_type: descriptor.schema_validator for step_type, descriptor in external.items()})


class ScheduleConfig(BaseModel):
    cron: str | None = None
    interval_seconds: int | None = Field(default=None, gt=0)
    timezone: str | None = None
    paused: bool = False

    model_config = {"extra": "forbid"}

    @field_validator("cron")
    @classmethod
    def _v_cron(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("schedule.cron must not be empty")
        return normalized

    @field_validator("timezone")
    @classmethod
    def _v_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        try:
            ZoneInfo(normalized)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unsupported schedule.timezone: {normalized}") from exc
        return normalized

    @model_validator(mode="after")
    def _v_kind(self) -> "ScheduleConfig":
        if (self.cron is None) == (self.interval_seconds is None):
            raise ValueError("schedule requires exactly one of cron or interval_seconds")
        return self

    def effective_timezone(self, project_timezone: str) -> str:
        """scheduler 가 cron/interval 을 해석할 IANA timezone 을 돌려준다.

        job `schedule.timezone` 이 있으면 그것을, 없으면 project `timezone` 을 쓴다.
        Airflow 와 Prefect projection 은 모두 이 함수로 같은 값을 얻는다.
        """
        return self.timezone or project_timezone


class StepGraphJob(BaseModel):
    job_id: str
    display_name: str | None = None
    schedule: ScheduleConfig | None = None
    execution: ExecutionConfig | None = None
    defaults: StepGraphDefaults | None = None
    steps: list[StepGraphStep]

    model_config = {"extra": "forbid"}

    @field_validator("job_id")
    @classmethod
    def _v_job_id(cls, value: str) -> str:
        return validate_job_id(value, "job_id")

    @field_validator("display_name")
    @classmethod
    def _v_job_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def _v_graph(self) -> "StepGraphJob":
        if not self.steps:
            raise ValueError("steps[] 는 최소 1개 필요하다.")
        ids = [step.id for step in self.steps]
        duplicated = sorted({step_id for step_id in ids if ids.count(step_id) > 1})
        if duplicated:
            raise ValueError(f"steps[].step_id 가 중복된다: {', '.join(duplicated)}")
        known = set(ids)
        for step in self.steps:
            unknown = sorted(dep for dep in step.depends_on if dep not in known)
            if unknown:
                raise ValueError(f"step {step.id} depends_on unknown step id: {', '.join(unknown)}")
            if step.when:
                refs = [step.when.success, step.when.failed]
                refs.extend(_when_expr_step_refs(step.when.expr))
                unknown_when = sorted(ref for ref in refs if ref and ref not in known)
                if unknown_when:
                    raise ValueError(f"step {step.id} when references unknown step id: {', '.join(unknown_when)}")
        _validate_acyclic(self.steps)
        return self


def _validate_acyclic(steps: list[StepGraphStep]) -> None:
    graph = {step.id: _step_upstream_ids(step) for step in steps}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(step_id: str, stack: list[str]) -> None:
        if step_id in visited:
            return
        if step_id in visiting:
            cycle = stack[stack.index(step_id) :] + [step_id]
            raise ValueError("step graph cycle 이 있다: " + " -> ".join(cycle))
        visiting.add(step_id)
        stack.append(step_id)
        for upstream in sorted(graph.get(step_id, set())):
            visit(upstream, stack)
        stack.pop()
        visiting.remove(step_id)
        visited.add(step_id)

    for step_id in sorted(graph):
        visit(step_id, [])


def _when_expr_step_refs(expr: str | None) -> list[str]:
    if not expr:
        return []
    return [match.group(1) for match in STEP_EXPR_REF_RE.finditer(expr)]


def _step_upstream_ids(step: StepGraphStep) -> set[str]:
    upstream = set(step.depends_on)
    if step.when:
        if step.when.success:
            upstream.add(step.when.success)
        if step.when.failed:
            upstream.add(step.when.failed)
        upstream.update(_when_expr_step_refs(step.when.expr))
    return upstream


def validate_step_graph_config(path: Path, config: dict[str, Any]) -> StepGraphJob:
    try:
        return StepGraphJob.model_validate(config)
    except ValidationError as e:
        formatted = "\n".join(f"  - {'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in e.errors())
        raise ValueError(f"step graph schema 검증 실패 [{path.name}]:\n{formatted}") from e


def validate_step_graph_configs(config_items: list[tuple[Path, dict[str, Any]]], project_root: Path) -> dict[str, Any]:
    jobs = [validate_step_graph_config(path, config) for path, config in config_items]
    _validate_query_files(config_items, jobs, project_root)
    from zeta4s.project.execution_plan import build_step_graph_execution_plan

    plans = [build_step_graph_execution_plan(job) for job in jobs]
    return {
        "configs": len(jobs),
        "steps": sum(len(job.steps) for job in jobs),
        "execution_plans": len(plans),
        "execution_edges": sum(len(plan.edges) for plan in plans),
        "control_edges": sum(len(plan.control_edges) for plan in plans),
        "data_edges": sum(len(plan.data_edges) for plan in plans),
        "data_bindings": sum(len(plan.data_bindings) for plan in plans),
        "types": sorted({step.type for job in jobs for step in job.steps}),
    }


def _validate_query_files(
    config_items: list[tuple[Path, dict[str, Any]]], jobs: list[StepGraphJob], project_root: Path
) -> None:
    errors: list[str] = []
    for (config_path, _), job in zip(config_items, jobs):
        for step in job.steps:
            refs = []
            if step.query:
                refs.append(step.query)
            if step.type == "elasticsearch.command":
                refs.extend(_elasticsearch_command_file_refs(step))
            for query_ref in refs:
                query_path = project_root / query_ref
                if not query_path.is_file():
                    errors.append(f"{config_path.name}:{step.id} referenced file not found: {query_ref}")
    if errors:
        raise ValueError("step graph referenced file validation failed:\n  - " + "\n  - ".join(errors))


def _elasticsearch_command_file_refs(step: StepGraphStep) -> list[str]:
    refs: list[str] = []
    for mapping, key in (
        (step.source, "file"),
        (step.target, "settings"),
        (step.target, "mappings"),
        (step.request, "body"),
    ):
        if not isinstance(mapping, dict):
            continue
        value = mapping.get(key)
        if isinstance(value, str):
            refs.append(_validate_ref(value, f"{step.type}.{key}"))
    return refs
