# Built-in Step Type Abstraction

## 목표

내장 `steps[].type` 구현을 두 registry 경계 — project 선언 정본과 core 실행 매핑 — 으로
정리한다. 이 작업은 외부 플러그인 SDK 를 만드는 일이 아니다. 내장 step type 의 schema
validation, 실행 매핑, runtime payload, result contract 를 한 곳에서 추적 가능하게 만든다.

scheduler 바인딩은 type 별 코드를 요구하지 않는다. 모든 step type 은 동일한 generic task
바인딩으로 실행되고, type 별 실행 dispatch 는 task 프로세스 안 core executor 가 수행한다.

내장 `steps[].type` 구현은 다음 경계로 분리한다.

- `src/zeta4s/project/step_graph.py`: `_STEP_TYPE_SPECS`(`StepTypeSpec`) registry 가
  built-in step type 의 선언 정본이다. `STEP_TYPE_VALUES`, pool stage, schema validator,
  JSON Schema enum 을 모두 이 registry 에서 파생한다. `StepGraphStep.type` 은 `str` +
  field_validator membership 검증이다.
- `src/zeta4s/core/step_executors.py`: `_BUILT_IN_STEP_EXECUTOR_BUILDERS` 가 type →
  StepExecutor builder 매핑이다. import-time 가드로 project registry type 정본과의
  congruence 를 강제한다. Airflow·Prefect·CLI 실행 경로가 모두 `built_in_step_executor` 로
  dispatch 한다.
- `src/zeta4s/airflow/dag_generator.py`: DAG 생성, flow control, edge wiring
- `src/zeta4s/airflow/step_binding.py`: type-agnostic generic task 바인딩
  (`core_step_operator`). type 별 분기가 없다.
- `src/zeta4s/airflow/operators.py`: Airflow callable facade (`run_core_step`)
- `src/zeta4s/runtime/*`: 실제 runtime implementation

새 built-in step type 을 추가할 때는 project registry(`_STEP_TYPE_SPECS`), core executor
builder registry, runtime callable facade, runtime implementation 의 경계를 명시적으로
수정한다. project registry 와 core executor registry 의 불일치는 import-time 에
`RuntimeError` 로 걸린다. scheduler 바인딩은 generic 이라 손대지 않는다.

## 비목표

- plugin SDK 배포 (`zeta4s-plugin-sdk`)
- third-party adapter compatibility policy (descriptor 계약 버전 range)
- plugin validation CLI (`z4s plugin check` 류)
- Airflow Dataset/Asset trigger 재도입
- runtime 중 동적 step 생성
- generated dbt source metadata 또는 `sources_raw.yml` 생성

## Schema 검증 경계

step type 고유 필수 field 와 조합 규칙은 project registry 의 schema validator
(`_STEP_TYPE_SPECS[*].schema_validator`)가 검증한다. 예를 들어 `dbt.run` 은 `models[]` 를
요구하고, `oracle.extract` 는 source 종류와 source 선택 field 조합을 검증한다. 이 validator 는
`StepGraphStep` pydantic 검증 시점에 실행되므로, DAG 생성/실행 경로가 별도로 재검증하지
않는다.

## 모듈 구조

구현 모듈 경계는 다음과 같다.

```text
src/zeta4s/project/step_graph.py    # 선언 정본(_STEP_TYPE_SPECS) + schema validator
src/zeta4s/core/step_executors.py   # 실행 매핑(_BUILT_IN_STEP_EXECUTOR_BUILDERS)
src/zeta4s/airflow/step_binding.py  # generic task 바인딩(core_step_operator)
src/zeta4s/airflow/dag_generator.py # DAG 생성, flow control, edge wiring
src/zeta4s/airflow/operators.py     # run_core_step facade
```

`step_binding.py` 는 `StepBindingContext`, `StepGraphTaskBinding`, `single_task_binding`,
`core_step_operator` 를 정의한다. type 별 adapter 클래스나 registry 는 없다 — 모든 step type 이
같은 `core_step_operator` 로 바인딩된다.

## DAG Generator 변경

`dag_generator.py` 는 다음 책임만 가진다.

- `StepGraphJob` 검증
- `ExecutionPlan` 생성
- DAG 생성과 invariant 검증
- generic step 바인딩
- flow control 적용
- `ExecutionPlan.edges` wiring
- terminal step success marker wiring

task 생성은 `step_binding.py` 의 `core_step_operator` 에 둔다.

```python
binding = single_task_binding(core_step_operator(ctx))
binding = _apply_step_graph_trigger_rule(binding, step)
```

미지원 type 도 같은 generic 바인딩을 타고, 실행 시점에 core `_unsupported_executor` 가 계약
위반을 보고한다. Flow control (`when.expr`, `join.rule`, retry, timeout) 은 바인딩 밖에서 공통
적용한다.

## Runtime Callable Boundary

`zeta4s.airflow.operators` 는 계속 facade 로 둔다. `step_binding.py` 는 `run_core_step` facade
callable 을 참조하고, facade 함수 내부에서 `built_in_step_executor` 로 실제 executor 를 조립해
`zeta4s.runtime.*` implementation 을 import 한다.

이 규칙은 유지한다.

- DAG parse 단계에서 `oracledb`, `duckdb`, `pyarrow`, provider client 를 import 하지 않는다.
- runtime implementation 은 task process 에서 callable 실행 시점에 import 한다.
- `step_binding.py` 도 parse-safe dependency 만 import 한다.

## Result Contract

runtime result 에 대해 각 step type 은 다음 정보를 문서화한다.

- `stage`: `extract`, `stage`, `transform`, `validation`, `write`, `noop` 등
- `metrics`: row count, affected rows, validation count, dbt status 등
- `outputs`: `when.expr` 와 후행 step metadata 에 노출할 scalar/table output
- failure message convention

result writer 를 새로 만들지 않는다. 기존 `record_success`, `result_context`,
task result note 경계를 유지하고 step type 별 기대 result shape 를 문서화한다.

## Step Type Input/Output Contract

Step type 별 YAML 계약은 이 문서에 중복해서 적지 않는다. 중복 예시는 세부 계약 문서와 쉽게
어긋나므로, 이 문서는 step type abstraction 의 공통 경계만 다룬다.

세부 계약 문서는 다음을 기준으로 한다.

- `docs/usage/step-types/extract.md`
- `docs/usage/step-types/stage.md`
- `docs/usage/step-types/sql.md`
- `docs/usage/step-types/http-lookup.md`
- `docs/usage/step-types/dbt.md`
- `docs/usage/step-types/write.md`

공통 규칙은 다음이다.

- 실행 순서와 fan-in/fan-out 은 `depends_on`, `when.*`, `join.rule` 로 명시한다.
- Step 간 data reference 는 step type 별 계약 field 로 표현한다.
- DB table 이름은 step type 계약에서 별도로 허용하지 않는 한 lowercase `schema.table` 형식으로 쓴다.
- 현재 계약의 `schema.table` 단일 문자열 표기에서는 quoted identifier 를 표현하지 않는다.
- Step type 별 필수 field, 쓰지 않는 field, result/metrics 는 각 `step-types/*.md` 문서를 따른다.

## 검증 기준

- `uv run python -m compileall -q src/zeta4s`
- `bash scripts/check_static_cli_contract.sh`
- 목표 contract showcase 에 대해 `z4s project check`
- Airflow DAG parse 경로가 runtime implementation module 을 직접 import 하지 않음
- `dag_generator.py` 가 per-type adapter registry(`builtin_step_adapters`/`step_adapters`)
  없이 generic `core_step_operator` 로만 바인딩함
- release DAG run matrix 가 release gate 에서 통과함

## 외부 Step Type 등록 (설치 = 등록)

빌트인이 아닌 step type 은 별도 Python 패키지로 배포하고, 그 패키지를 실행 환경에 설치하는
것으로 등록한다. 패키지는 `zeta4s.step_types` entry-point group 에 factory 를 선언하고,
zeta4s 를 쓰는 각 프로세스가 설치된 배포판을 discover 해 등록한다. 정본은
`src/zeta4s/project/step_types.py`, 사용법은 `../usage/step-type-plugins.md` 다.

- `StepTypeDescriptor`(`step_graph.py`)가 외부 type 의 등록 정본이다. 선언 축(pool_stage,
  schema_validator)과 실행 축(`runtime_callable` dotted string + `payload_builder`)을 함께
  담고, scheduler·core 를 import 하지 않는다.
- `register_installed_step_types` 는 `importlib.metadata.entry_points` 로 설치된 factory 를
  discover 해(`discover_installed_step_type_factories`) 검증 후 등록한다. project 계층 파생
  테이블(`STEP_TYPE_VALUES`, `STEP_TYPE_POOL_STAGES`, `_STEP_TYPE_VALIDATORS`)을 built-in +
  외부 등록으로 재구성한다(`rebuild_step_type_tables`). dict 는 in-place 로 갱신해 이름으로
  import 한 소비처(execution_plan/pools)까지 반영한다. discovery 는 stdlib 만 쓰므로 scheduler
  중립이다.
- core `built_in_step_executor` 는 builder miss 시 등록 descriptor 를 조회해 generic
  `RuntimeCallableStepExecutor(runtime_callable, payload_builder(...))` 를 만든다. 빌트인
  compound executor 는 core 에 그대로 남는다.
- 등록은 검증·실행 이전의 명시 load-time 단계다(import-time side effect 없음). CLI
  (`z4s project check`/`run`/`api deploy`), API 서버(deploy gate + runtime-step 실행), Airflow
  (DAG parse + task), Prefect 워커가 config 검증(`StepGraphJob`) 이전에
  `register_installed_step_types()` 를 호출한다. 빌트인 재정의 / 잘못된 pool stage / 해석 불가
  `runtime_callable` / 비-iterable factory 는 로드 시점 계약 위반이다.

## 이후 확장

완전한 외부 package plugin 경로의 남은 항목은 별도 milestone 으로 다룬다
(`docs/roadmap/backlog/plugin-runtime-entry-points-sdk.md`). 그때 필요한 항목은 다음이다.

- adapter package compatibility range (descriptor 계약 버전)
- `zeta4s-plugin-sdk` (외부 저자용 최소 표면)
- plugin validation CLI (`z4s plugin check` 류)
- runtime image 에 plugin dependency 를 포함하는 배포 방식
