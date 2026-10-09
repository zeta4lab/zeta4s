# Core Runner Scheduler Blueprint

## 목적

zeta4s 는 AI Agent 가 생성한 Contract 를 Step Graph 로 실행하는 범용 Runtime Engine 이다.
이 청사진은 zeta4s 가 무엇을 소유하고 무엇을 위임하는지 고정한다.

zeta4s 가 소유하는 것:

- canonical contract 와 정적 검증 (`project.yml`, `jobs/*.yml`, `ExecutionPlan`)
- flow control **의미론(semantics)**: `when.*`, `join.rule`, skip/failure propagation,
  retry/timeout policy 의 의미, terminal result aggregation 규칙
- step 실행 경계: `StepExecutor` 와 step type 별 runtime 구현
- 실행 metadata: metastore 의 run/step state/event/output binding 기록 계약

zeta4s 가 소유하지 않는 것 (orchestration **기반(mechanism)**):

- 상시 스케줄링 데몬, cron/backfill
- 병렬 실행, worker/process 분배
- 장애 복구, run 재개, 취소 인프라
- 운영 관측 UI

orchestration 기반은 항상 외부 scheduler engine 에 위임한다. 운영 환경의 engine 은
Airflow 와 Prefect 두 가지다.
zeta4s 는 자체 오케스트레이터를 만들지 않는다. 1인 유지보수 범위에서 오케스트레이션
기반은 소유할 수 없는 비용이며, zeta4s 의 가치는 계약과 의미론의 단일성에 있다.

## 핵심 원칙

- `zeta4s Step` 은 `Prefect Task` 가 아니다.
- `zeta4s ExecutionPlan` 은 `Prefect Flow` 가 아니다.
- `zeta4s ExecutionPlan` 은 `Airflow DAG` 가 아니다.
- Airflow 와 Prefect 는 `ExecutionPlan` 을 실행하는 adapter/projection 이다.
- flow control 의미론의 정의는 core 가 단일 소유한다. adapter 는 core 의미론 함수를
  호출하거나, 등가가 conformance test 로 증명되는 engine native 기능으로만 매핑한다.
- orchestration 기반은 engine 이 소유한다. core 에 스케줄링/병렬/복구/취소 기반 코드를
  두지 않는다.
- core 패키지는 Airflow, Prefect 를 import 하지 않는다.
- 하위호환성을 위해 과거 실행 경로를 유지하지 않는다.

## 의미론과 기반의 분리

| 층 | 내용 | 소유 |
|----|------|------|
| 의미론 | `when.*`/`join.rule` 판정, skip/failure propagation 규칙, retry/timeout policy 의 의미, terminal aggregation | `zeta4s.core` 순수 함수/계약 |
| 기반 | step 실행 순서 집행, 병렬성, retry timer/timeout 집행, 스케줄링, 복구, 취소, 관측 UI | 실행 mode 의 orchestration owner |

의미론을 engine 에 위임하면 같은 `jobs/*.yml` 이 backend 에 따라 다르게 동작한다.
Airflow 는 trigger rule 이 있고 Prefect 는 없으므로, join/when 의미론은 engine 이
공통으로 제공할 수 없다. 따라서 의미론은 core 가 정의하고 모든 실행 경로가 같은
정의를 사용한다.

## 실행 모드

두 실행 mode 를 공식 용어로 둔다.

### core-orchestrated (검증 전용)

core 의 sequential verification Runner 가 plan 전체를 topological order 로 실행한다.
용도는 `z4s run` 의 로컬 즉시 실행, CI 계약 테스트, 의미론의 참조 구현(reference
implementation) 이다.

이 mode 의 영구 비목표: 병렬 실행, run 재개/복구, 상시 스케줄링, 취소 인프라.
verification Runner 는 production 상시 실행 경로가 아니며, 그 방향으로 확장하지 않는다.

### scheduler-projected (운영 실행)

scheduler engine 이 `ExecutionPlan` 을 자신의 graph 로 projection 하고 step 실행 조율을
소유한다. 각 step 실행 단위는 core 의 단일 step 실행 경계를 호출하고, 실행 직전
eligibility 는 core 의미론 함수로 판정하며, 결과는 core reporter 계약으로 metastore 에
기록한다.

운영 실행은 항상 이 mode 다. engine 은 두 가지이며 모두 `z4s api deploy` 경로를 쓴다.

- Airflow: DAG/task projection.
- Prefect: deployment/step 실행 unit projection.

## 실행 흐름

로컬 검증 실행:

```text
z4s run
  -> ExecutionPlan
  -> core verification Runner (sequential)
  -> StepExecutor
  -> runtime callable
```

Airflow 실행:

```text
z4s api deploy (profile scheduler=airflow)
  -> ExecutionPlan projection -> Airflow DAG/task
  -> task 별 run_core_step -> core step 실행 경계
  -> StepExecutor
  -> runtime callable
```

Prefect 실행:

```text
z4s api deploy (profile scheduler=prefect)
  -> canonical schedule definition + ExecutionPlan
  -> Prefect deployment projection
  -> step 별 실행 unit -> core step 실행 경계
  -> StepExecutor
  -> runtime callable
```

세 경로 모두 같은 `ExecutionPlan`, 같은 core 의미론, 같은 `StepExecutor`, 같은
metastore 기록 계약을 사용한다. runtime callable 을 core 경계 밖에서 직접 호출하는
경로는 존재하지 않는다.

## Prefect scheduler engine 정책

- Prefect dependency 는 `zeta4s-api` distribution 의 adapter layer 에만 둔다.
  `zeta4s.core`, `zeta4s.project`, `zeta4s-cli` 에는 들어가지 않는다.
- Prefect 버전은 `uv.lock` 으로 고정하고, 업그레이드는 명시적 결정으로만 수행한다.
  Prefect 신기능 추종을 위한 리팩토링은 하지 않는다.
- Prefect 의 Flow/Task/Deployment model 을 canonical authoring/normalization model 로
  사용하지 않는다.

### Scheduler pool projection

부하 조절의 목표 계약은 scheduler 고유 authoring 이 아니라 Step Graph 에서 자동 산출하는 pool 이다.
사용자 지정 pool 은 예외적 override 이며 기본 authoring 으로 권장하지 않는다. 정확한 schema, pool
산출, projection payload 의 정본은 `../README.md` 의 코드 위치 포인터를 따른다.

Pool 이름 선택과 resource 구현의 경계는 다음과 같다.

- Backend-neutral resolver가 explicit override를 먼저 적용하고, 없으면 step type의 canonical stage에서
  project-scoped 자동 pool 이름을 산출한다.
- Airflow adapter는 effective pool을 Airflow task binding으로 projection 한다.
- Prefect deploy는 자동 pool을 Global Concurrency Limit으로 생성·변경하고, Prefect task는 같은
  effective pool을 실행 동안 점유한다.
- 사용자 지정 pool은 resolver가 보존하지만 자동 resource provisioning 대상은 아니다. 예외적 override로만
  유지하며 기본 authoring으로 권장하지 않는다.

`zeta4s-api`가 scheduler projection state를 소유한다. Prefect undeploy는 deployment를 제거하지만
concurrency limit garbage collection은 현재 계약에 포함하지 않는다. Project 간 공유 resource를 job
단위 undeploy가 임의로 삭제하지 않기 위한 경계다.

### Checkpoint recovery 경계

Scheduler retry 와 zeta4s checkpoint 는 서로 다른 책임이다. Scheduler 는 실패한 실행 unit 을 새
attempt 로 다시 실행하고, zeta4s runtime 은 이미 commit 한 step checkpoint 를 검증해 지원되는
step type 의 input position 을 복원한다. Scheduler state 자체를 checkpoint 로 사용하지 않는다.

복구 가능 범위는 step type의 checkpoint 계약에 달려 있다. Checkpoint를 지원하지 않는 step을 일반적인
Prefect retry만으로 exact resume 할 수 있다고 간주하지 않는다. 목표 release gate도 외부 data backend
장애 뒤 retry attempt가 committed checkpoint에서 재개해 output과 downstream 실행을 완성하는 경로만
다룬다. 현재 Prefect 장애 주입 timing은 이 목표를 안정적으로 증명하지 못하므로 완료 계약이 아니다.
Prefect의 모든 state transition, worker crash, cancel, pause, cache 동작을 zeta4s recovery contract로
확장하지 않는다.

Docker `prefect` profile 은 Prefect server 와 process worker 를 기동하고, worker 는 API 와
같은 artifact volume 및 metastore 를 사용한다. 정확한 dependency와 image version은 코드와
compose 설정이 정본이다.

## Schedule definition

canonical source 는 `jobs/*.yml` 의 `schedule` 이다.

```yaml
schedule:
  cron: "0 2 * * *"       # 또는 interval_seconds: 300
  timezone: Asia/Seoul
  paused: false
```

- `cron` 과 `interval_seconds` 는 정확히 하나만 선언한다.
- `timezone` 은 IANA timezone 이름이며 암묵적으로 profile/project 값에서 상속하지 않는다.
- identity 는 `project_id`, `job_id`, `profile` 조합이다.
- Schedule deployment 는 project 의 `z4s api deploy` 에 포함된다.

## 소유권 매트릭스

| 책임 | core-orchestrated | scheduler-projected |
|------|-------------------|---------------------|
| control semantics 정의 | core | core |
| step readiness 집행 | verification Runner | engine graph projection |
| 실행 직전 eligibility 판정 | verification Runner (core 함수) | core step 경계 (core 함수) |
| retry/timeout policy 의미 | core contract | core contract |
| retry/timeout 집행 | verification Runner | engine native 기능 매핑 |
| 병렬 실행 | 없음 (영구 비목표) | engine |
| 스케줄링/복구/취소 | 없음 (영구 비목표) | engine |
| step lifecycle 기록 | core reporter | core reporter |
| terminal aggregation 계산 | core 함수 | core 함수 (adapter/reconciler 가 호출) |

retry/timeout 집행 owner 는 mode 당 정확히 하나다. scheduler-projected mode 에서 core
step 경계는 retry 를 재집행하지 않는다. engine 의 infrastructure retry(worker 장애 등)는
canonical step attempt 와 구분해 기록한다.

verification Runner 의 timeout 은 실행 환경과 무관하게 runtime callable 완료 후 elapsed time을
판정하는 단일 참조 동작이다. 실행 중 worker 강제 회수와 취소는 제공하지 않는다. 운영 mode 의
hard timeout 과 worker 회수는 scheduler native timeout 이 소유한다.

## 패키지 책임

```text
zeta4s.project
  canonical input load/validation -> StepGraph -> ExecutionPlan

zeta4s.core
  flow control semantics (순수 함수)
  step 실행 경계 (StepExecutor, ExecutionContext, ConnectionResolver,
                  ArtifactStore, RunReporter)
  verification Runner (sequential, 검증 전용)

zeta4s.prefect
  Prefect deployment와 worker 실행 adapter

zeta4s.airflow
  Airflow enterprise adapter (DAG projection, run_core_step facade)
```

- `Airflow*`, `Prefect*` 이름은 `zeta4s.core` 안에 두지 않는다.
- adapter 는 canonical `ExecutionPlan`/`ExecutionStep` 만 실행 경계로 전달한다.

## 경계 불변조건

다음 네 조건이 성립하는 한 adapter 를 추가해도 zeta4s 는 scheduler 종속 없이 유지된다.
네 조건 모두 CI 로 강제한다.

1. `zeta4s.core` 와 `zeta4s.project` 는 airflow/prefect 를 import 하지 않는다.
2. YAML 의 모든 flow control field 는 core 에 단일 의미 정의를 가진다. adapter 는 그
   함수를 호출하거나 등가 증명(conformance test: 같은 plan 의 terminal result 비교)이
   있는 native 기능으로만 매핑한다.
3. metastore 기록 스키마는 실행 backend 와 무관하게 동일하다.
4. adapter 디렉토리를 제거해도 core/project 가 compile 된다 (import 방향 단방향).

## CLI UX

```text
z4s run <project> <job> --profile <profile>           # 로컬 즉시 실행 (verification Runner)
z4s api deploy <project> --profile <profile>          # profile 이 Airflow/Prefect 선택
```

- schedule 의 canonical source 는 `jobs/*.yml` 의 `schedule` field 다. CLI option 으로
  덮어쓰지 않는다.
- 별도 `z4s schedule` 그룹과 schedule 전용 API endpoint 는 제공하지 않는다.
