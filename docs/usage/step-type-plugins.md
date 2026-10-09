# 외부 Step Type 플러그인

빌트인이 아닌 step type 은 별도 Python 패키지로 배포하고, 그 패키지를 실행 환경에 설치하는
것으로 등록한다. **설치가 곧 등록이다** — profile 이나 project 에 나열하지 않는다. 설치된
패키지가 `zeta4s.step_types` entry-point group 에 factory 를 선언하면, zeta4s 를 쓰는 모든
프로세스(host CLI, API 서버, Airflow scheduler/worker, Prefect worker)가 검증·실행 이전에
설치된 배포판을 discover 해 등록한다.

등록된 외부 type 은 빌트인과 같은 Step Graph 계약(schema 검증, pool stage, 실행)을 통과한다.

## 플러그인 패키징 계약

플러그인 패키지의 `pyproject.toml` 에 entry point 를 선언한다. group 이름은 반드시
`zeta4s.step_types` 다. value 는 `"module:attr"` dotted path 이며, attr 는 인자 없이 호출돼
`StepTypeDescriptor` iterable 을 반환하는 factory 다.

```toml
[project.entry-points."zeta4s.step_types"]
acme = "acme_zeta_steps:descriptors"
```

```python
# acme_zeta_steps/__init__.py
from zeta4s.project.step_graph import StepTypeDescriptor

def descriptors():
    return [
        StepTypeDescriptor(
            type="acme.echo",
            pool_stage="transform",           # PROJECT_POOL_STAGES 중 하나, 또는 None
            schema_validator=_validate_echo,  # (step) -> None, 위반 시 raise
            runtime_callable="acme_zeta_steps.runtime:run_echo",  # "module:func" 지연 해석
            payload_builder=_echo_payload,    # (project, plan, step, common) -> dict
            connection_id_fields=(),          # StepGraphStep 의 connection id 필드명
        ),
    ]
```

`StepTypeDescriptor` 는 선언 축(`type`, `pool_stage`, `schema_validator`)과 실행 축
(`runtime_callable` dotted string + `payload_builder`)을 함께 담고 **scheduler·core 를 모른다**.
실행은 빌트인과 같은 core runtime callable 경로(`RuntimeCallableStepExecutor`)를 탄다.

## 설치 위치

플러그인 패키지는 외부 type 을 **검증하거나 실행하는 모든 환경**에 설치돼 있어야 한다.

- runtime image(Airflow worker / Prefect worker): task 실행에 필요.
- Airflow scheduler: DAG parse 시 `StepGraphJob` 검증에 필요.
- API 서버: `z4s api deploy` 검증과 runtime-step API 실행에 필요.
- host `z4s` CLI 환경: `z4s project check` / `z4s run` 이 외부 type 프로젝트를 정적 검증하려면
  descriptor(특히 `schema_validator`)를 로드해야 하므로 필요.

## 계약 위반

등록은 검증·실행 이전의 명시 load-time 단계다(import-time side effect 없음). 다음은 로드
시점에 계약 위반으로 보고돼 실행 경로로 새지 않는다.

- 빌트인 type 재정의 / 외부 type 끼리 이름 충돌
- `pool_stage` 가 `PROJECT_POOL_STAGES` 밖
- `schema_validator` / `payload_builder` 가 callable 아님
- `runtime_callable` dotted path 해석 실패(등록 시점 eager 확인, 실제 호출은 지연)
- factory 가 `StepTypeDescriptor` iterable 을 반환하지 않음

정본은 `src/zeta4s/project/step_types.py` 다.
