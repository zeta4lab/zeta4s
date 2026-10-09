"""외부 step type descriptor 의 등록.

built-in step type 은 `step_graph._STEP_TYPE_SPECS` 선언 정본과 `core` 실행 매핑으로
나뉘지만, 외부 type 은 core 를 모른 채 runtime image 에 설치되는 Python 패키지로 배포된다.
플러그인 패키지는 `zeta4s.step_types` entry-point group 에 factory 를 선언하고, 설치 자체가
등록이다. 이 모듈은 설치된 배포판을 discover 해 `StepTypeDescriptor` 를 얻고, project 계층
파생 테이블(`step_graph.STEP_TYPE_VALUES` 등)을 재구성해 core 실행 경로가 조회할 registry 를
유지한다.

등록은 project check / api deploy / DAG parse / runtime task 프로세스가 검증·실행 이전에
명시적으로 호출하는 load-time 단계다(무인자 discovery). airflow/prefect/core 를 import 하지
않아 scheduler 중립을 유지한다 — discovery 는 stdlib `importlib.metadata` 만 쓴다.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Sequence
from importlib.metadata import entry_points
from threading import RLock
from typing import Any

from zeta4s.project.step_graph import (
    BUILTIN_STEP_TYPE_VALUES,
    PROJECT_POOL_STAGES,
    StepGraphStep,
    StepTypeDescriptor,
    rebuild_step_type_tables,
)

STEP_TYPE_ENTRY_POINT_GROUP = "zeta4s.step_types"

__all__ = [
    "StepTypeDescriptor",
    "StepTypeContractError",
    "STEP_TYPE_ENTRY_POINT_GROUP",
    "register_step_type_descriptors",
    "registered_step_type_descriptor",
    "reset_step_type_registry",
    "load_step_type_descriptors",
    "discover_installed_step_type_factories",
    "register_installed_step_types",
]


class StepTypeContractError(ValueError):
    """외부 step type 등록이 계약을 위반했을 때 로드 시점에 보고한다."""


# 외부(비 built-in) 등록 descriptor. built-in 은 여기 담지 않는다.
_registered: dict[str, StepTypeDescriptor] = {}
_registry_lock = RLock()


def registered_step_type_descriptor(step_type: str) -> StepTypeDescriptor | None:
    """등록된 외부 step type descriptor 를 반환한다. built-in 은 None."""
    with _registry_lock:
        return _registered.get(step_type)


def reset_step_type_registry() -> None:
    """외부 등록을 모두 지우고 built-in 만 남긴다 (테스트 격리 / 프로젝트 간 오염 방지)."""
    with _registry_lock:
        _commit_registry({})


def register_step_type_descriptors(descriptors: Sequence[StepTypeDescriptor], *, source: str) -> None:
    """외부 descriptor 를 검증 후 등록하고 파생 테이블을 재구성한다.

    동일 descriptor 재등록은 idempotent no-op. built-in 또는 다른 외부 type 과의 이름 충돌,
    schema 위반, runtime_callable 해석 실패는 `StepTypeContractError` 로 보고한다.
    """
    with _registry_lock:
        candidate = _validated_registry(descriptors, source=source, initial=_registered)
        _commit_registry(candidate)


def _validated_registry(
    descriptors: Sequence[StepTypeDescriptor],
    *,
    source: str,
    initial: dict[str, StepTypeDescriptor],
) -> dict[str, StepTypeDescriptor]:
    candidate = dict(initial)
    for descriptor in descriptors:
        if not isinstance(descriptor, StepTypeDescriptor):
            raise StepTypeContractError(f"{source}: expected StepTypeDescriptor, got {type(descriptor).__name__}")
        _validate_descriptor(descriptor, source)
        existing = candidate.get(descriptor.type)
        if existing is not None and existing != descriptor:
            raise StepTypeContractError(f"{source}: step type already registered: {descriptor.type}")
        candidate[descriptor.type] = descriptor
    return candidate


def _commit_registry(candidate: dict[str, StepTypeDescriptor]) -> None:
    with _registry_lock:
        # 설치된 entry-point 집합은 프로세스 수명 동안 고정이다. Airflow task/API 요청마다
        # 같은 descriptor 를 재등록할 때 파생 dict 를 clear→update 하면 동시 validator 가
        # 중간 상태를 볼 수 있으므로, 동일 registry 는 mutation 없는 fast path 로 끝낸다.
        if _registered == candidate:
            return
        _registered.clear()
        _registered.update(candidate)
        rebuild_step_type_tables(_registered)


def load_step_type_descriptors(factory_refs: Iterable[str]) -> list[StepTypeDescriptor]:
    """`"module:factory"` ref 를 import·호출해 descriptor 목록을 얻는다.

    factory 는 인자 없이 호출돼 `StepTypeDescriptor` iterable 을 반환해야 한다. import/호출/
    타입 실패는 계약 위반이다.
    """
    descriptors: list[StepTypeDescriptor] = []
    for ref in factory_refs:
        factory = _resolve_dotted(ref, label="step_types factory")
        try:
            produced = factory()
        except Exception as exc:  # noqa: BLE001 - 계약 위반으로 표면화
            raise StepTypeContractError(f"step_types factory raised: {ref}: {exc}") from exc
        try:
            iterator = iter(produced)
        except TypeError as exc:
            raise StepTypeContractError(f"step_types factory must return an iterable: {ref}") from exc
        try:
            for descriptor in iterator:
                if not isinstance(descriptor, StepTypeDescriptor):
                    raise StepTypeContractError(
                        f"step_types factory must yield StepTypeDescriptor: {ref} produced {type(descriptor).__name__}"
                    )
                descriptors.append(descriptor)
        except StepTypeContractError:
            raise
        except Exception as exc:  # noqa: BLE001 - lazy iterable 실패도 계약 위반으로 표면화
            raise StepTypeContractError(f"step_types factory iterable raised: {ref}: {exc}") from exc
    return descriptors


def discover_installed_step_type_factories() -> list[str]:
    """설치된 배포판의 `zeta4s.step_types` entry points 에서 factory ref 를 모은다.

    각 entry point value 는 `"module:attr"` 형식이고, attr 는 인자 없이 호출돼
    `StepTypeDescriptor` iterable 을 반환하는 factory 다. stdlib `importlib.metadata` 만
    사용해 scheduler 중립을 유지한다.
    """
    return [entry_point.value for entry_point in entry_points(group=STEP_TYPE_ENTRY_POINT_GROUP)]


def register_installed_step_types(*, allowed_entry_point_names: frozenset[str] | None = None) -> int:
    """설치된 `zeta4s.step_types` 플러그인을 discover 해 활성 registry 를 원자적으로 교체한다.

    설치가 곧 등록이다. broken 플러그인(import/호출/타입/schema/이름 충돌 실패)은
    `StepTypeContractError` 로 표면화한다. 발견한 factory 수를 반환한다.
    """
    with _registry_lock:
        installed = list(entry_points(group=STEP_TYPE_ENTRY_POINT_GROUP))
        if allowed_entry_point_names is None:
            factory_refs = [entry_point.value for entry_point in installed]
        else:
            factory_refs = []
            for entry_point in installed:
                if entry_point.name not in allowed_entry_point_names:
                    raise StepTypeContractError(
                        f"entry-points:{STEP_TYPE_ENTRY_POINT_GROUP}: commercial plugin is not entitled"
                    )
                factory_refs.append(entry_point.value)
        descriptors = load_step_type_descriptors(factory_refs) if factory_refs else []
        candidate = _validated_registry(
            descriptors,
            source=f"entry-points:{STEP_TYPE_ENTRY_POINT_GROUP}",
            initial={},
        )
        _commit_registry(candidate)
        return len(factory_refs)


def _validate_descriptor(descriptor: StepTypeDescriptor, source: str) -> None:
    if (
        not isinstance(descriptor.type, str)
        or not descriptor.type.strip()
        or descriptor.type != descriptor.type.strip()
    ):
        raise StepTypeContractError(f"{source}: step type must be a non-empty trimmed string: {descriptor.type!r}")
    if descriptor.type in BUILTIN_STEP_TYPE_VALUES:
        raise StepTypeContractError(f"{source}: cannot override built-in step type: {descriptor.type}")
    if descriptor.pool_stage is not None and descriptor.pool_stage not in PROJECT_POOL_STAGES:
        raise StepTypeContractError(
            f"{source}: step type {descriptor.type} pool_stage must be one of "
            + ", ".join(PROJECT_POOL_STAGES)
            + f" or null: {descriptor.pool_stage}"
        )
    if not callable(descriptor.schema_validator):
        raise StepTypeContractError(f"{source}: step type {descriptor.type} schema_validator must be callable")
    if not callable(descriptor.payload_builder):
        raise StepTypeContractError(f"{source}: step type {descriptor.type} payload_builder must be callable")
    if not isinstance(descriptor.connection_id_fields, tuple) or not all(
        isinstance(field_name, str) and field_name in StepGraphStep.model_fields
        for field_name in descriptor.connection_id_fields
    ):
        raise StepTypeContractError(
            f"{source}: step type {descriptor.type} connection_id_fields must name StepGraphStep fields"
        )
    # runtime_callable 을 등록 시점에 eager 해석해 계약을 강제한다. 실제 실행은 지연 호출.
    _resolve_dotted(descriptor.runtime_callable, label=f"step type {descriptor.type} runtime_callable")


def _resolve_dotted(ref: Any, *, label: str) -> Any:
    """`"module:attr"` dotted ref 를 project-local importlib 로 해석한다.

    core `_resolve_runtime_callable` 을 import 하지 않아 project→core 금지를 유지한다.
    """
    if not isinstance(ref, str) or ":" not in ref:
        raise StepTypeContractError(f"{label} must use 'module:attr' format: {ref!r}")
    module_name, _, attr_name = ref.partition(":")
    if not module_name or not attr_name:
        raise StepTypeContractError(f"{label} must use 'module:attr' format: {ref!r}")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001 - 계약 위반으로 표면화
        raise StepTypeContractError(f"{label} module could not be imported: {module_name}: {exc}") from exc
    try:
        attr = module
        for part in attr_name.split("."):
            if not part:
                raise AttributeError(attr_name)
            attr = getattr(attr, part)
    except AttributeError as exc:
        raise StepTypeContractError(f"{label} attribute not found: {ref}") from exc
    if not callable(attr):
        raise StepTypeContractError(f"{label} is not callable: {ref}")
    return attr
