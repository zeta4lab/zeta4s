---
created_at: 2026-07-01
description: "step graph runtime roadmap"
---

# Step Graph Runtime Roadmap

zeta4s 의 궁극 목표는 AI Agent 가 생성한 계약(Contract)을 Step Graph 로 실행하는 범용
Runtime Engine 이 되는 것이다.

이 문서는 목표와 완성 여부만 관리한다. 구현 계획은 `../plans/`, 현재 기준은
`../usage/`, `../design/`, `../gate/` 에 있다.

## 판단 기준

- AI Agent 또는 사용자가 만든 contract 는 정적 검증 가능한 project artifact 로 저장되어야
  한다.
- 같은 contract 가 검증, 배포, 실행, 관측으로 이어져야 한다.
- `ExecutionPlan` 은 scheduler backend 와 독립적인 내부 실행 contract 다.
- scheduler backend 는 교체 가능한 실행 기반이며 canonical 표현이 아니다.

## 마일스톤

| 버전 | 내용 | 상태 |
|------|------|------|
| 1.0.0 | Step Graph Runtime Foundation | 완료 (2026-07-11) |
| 1.0.1 | 개발 인프라 — CI gate, ruff, format | 완료 (2026-07-15) |
| 1.0.2 | version single source of truth | 완료 (2026-07-15) |
| 1.0.3 | 문서 갈래 재편, etl/elt/framework 용어 제거 | 완료 (2026-07-15) |
| 1.0.4 | Airflow headless 와 project별 scheduler backend | 완료 (2026-07-16) |
| 1.0.5 | 다중 Work 컨텍스트 및 프로필 기반 API 연결 제어 | 완료 (2026-07-17) |
| 1.0.6 | Prefect profile 및 scheduler projection 기반 | 완료 (2026-07-17) |
| 1.0.7 | Scheduler backend 자동 pool binding | 완료 (2026-07-18) |
| 1.0.8 | Prefect checkpoint recovery gate 안정화 | 완료 (2026-07-18) |
| 1.0.9 | Airflow image 결합 제거 | 완료 (2026-07-17) |
| 1.0.10 | zeta4s scheduler stack 최신 stable 갱신 | 완료 (2026-07-17) |
| 1.0.11 | Scheduler 중립 run CLI/API 계약 | 완료 (2026-07-17) |
| 1.0.12 | LocalRunner 병렬 실행 및 취소 인프라 | 완료 (2026-07-19) |
| 1.0.13 | built-in step type registry 정본화 | 완료 (2026-07-20) |
| 1.0.14 | scheduler 바인딩 generic 화 + 명시 registry 등록 | 완료 (2026-07-20) |
| 1.0.15 | 외부 step type 설치=등록 전환 | 완료 (2026-07-20) |
| 1.0.16 | Zeta4S commercial license entitlement | 폐기 (1.0.20) |
| 1.0.17 | License revocation과 key rotation | 폐기 (1.0.20) |
| 1.0.18 | Internal license release evidence | 폐기 (1.0.20) |
| 1.0.19 | Secret 경계 단일화, master key 회전, k3s 배포 자산 | 완료 (2026-08-13) |
| 1.0.20 | Apache-2.0 오픈소스 전환, 상용 라이선스 집행 폐기 | 완료 (2026-10-09) |

### 1.0.0 Step Graph Runtime Foundation

상태: 완료 (2026-07-11)

하위호환 없는 첫 내부 구현 기준선이다. `project.yml`, `jobs/*.yml`, SQL/dbt 파일과
`ExecutionPlan` 을 canonical contract 로 고정했다.

담은 것:

- canonical `jobs/*.yml` 과 `ExecutionPlan`
- ClickHouse/Oracle/Elasticsearch runtime adapter
- dbt/SQL transform provider
- 내장 step adapter 추상화
- step flow control semantics
- storage-neutral rowset 과 step checkpoint
- metastore 소유권 경계와 scheduler snapshot
- Prefect scheduler engine

### 1.0.4 Airflow headless 와 project별 scheduler backend

상태: 완료 (2026-07-16)

`zeta4s-api` 를 scheduler engine 과 분리하고 Airflow 와 Prefect 를 같은 외부 backend 계약으로
연결했다. workspace profile 은 `airflow` 또는 `prefect` 를 선택하며, 생략하면 Prefect 를
사용한다. deploy 결과의 `scheduler_backend` 는 project별 metadata 로 저장되고 이후 undeploy 와
배포 해제는 저장된 backend 를 따른다.

schedule 배포는 `api deploy` 한 경로로 통합하고 `z4s schedule` 그룹은 삭제했다. 기존
`zeta4s.scheduler` package 는 별도 호환 계층 없이 `zeta4s.prefect` 로 이름만 변경했다.
Schedule 운영 interface와 backend 간 실행 동등성 gate는 후속 계획 범위다.

`1.0.4`는 이 구현 묶음을 표시하는 내부 체크포인트이며 정식 공개 릴리즈나 하위호환 보장을
뜻하지 않는다.

### 1.0.5 다중 Work 컨텍스트 및 프로필 기반 API 연결 제어

상태: 완료 (2026-07-17)

기존 단일 workspace 관리 구조를 하위 호환성 없이 폐기하고, 다중 workspace 컨텍스트 기반으로 전면 개편했다. 등록된 workspace 목록 관리와 스위칭(`use`)이 가능해졌으며, 프로필 단위의 `api_endpoint` 설정을 추가해 CLI 전역 API 통신 설정을 오버라이드할 수 있게 구성했다.

### 1.0.7 Scheduler backend 자동 pool binding

상태: 완료 (2026-07-18)

Step Graph에서 effective pool을 backend-neutral하게 해석하고 Airflow와 Prefect adapter가 같은 자동
project/stage pool binding을 각 scheduler resource로 구현한다. 자동 pool이 기본이며 사용자 지정 pool은
예외적 override로만 유지한다. 현재 계약과 검증 범위는 `../usage/project-contract.md`,
`../design/core-runner-scheduler-blueprint.md`, `../gate/README.md`가 관리한다. 실제 limit 초과 contention
시간은 현재 gate가 직접 측정하지 않으며 이 한계를 완료 범위와 구분한다.

### 1.0.8 Prefect checkpoint recovery gate 안정화

상태: 완료 (2026-07-18)

Prefect release runtime에서 외부 data backend 장애가 checkpoint 이후 정상 완료 전에 주입되도록
gate를 안정화했다. Elasticsearch checkpoint showcase는 짧은 request timeout을 사용하고, Prefect
gate는 Elasticsearch pause timeout으로 retry를 유도한다. 완료 판정은 같은 run 안의 attempt 증가,
연속 checkpoint sequence, 최종 output binding, downstream 확인 step 성공을 함께 요구한다.

### 1.0.9 Airflow image 결합 제거

상태: 완료 (2026-07-17)

zeta4s가 Airflow base image에 runtime wheel과 dependency를 설치하던 image build 경로를
삭제했다. Local stack은 공식 Airflow image를 사용하고, `zeta4s-api`가 standalone DAG source를
projection한다. Generated task는 active deployment identity를 인증된 internal endpoint에 전달하며
credential 해석과 core step 실행은 `zeta4s-api` process 안에서 수행한다.

### 1.0.10 zeta4s scheduler stack 최신 stable 갱신

상태: 완료 (2026-07-17)

host CLI와 server/adapter를 같은 zeta4s version으로 빌드하고, Airflow와 Prefect를 scheduler
stack 구성요소로 함께 검증한다. Prefect SDK와 공식 server image pin을 동기화했으며 Airflow는
공식 최신 stable image pin을 유지했다. Scheduler별 release gate는 선택한 backend profile을
기본으로 사용하고, 격리 포트와 cross-backend reset으로 빈 volume 검증을 보장한다.

### 1.0.11 Scheduler 중립 run CLI/API 계약

상태: 완료 (2026-07-17)

Profile의 scheduler 선택을 active deployment에 한 번 기록하고, 실행 시에는 동일한
`project_id`/`job_id`/`run_id` 계약으로 Airflow와 Prefect adapter를 dispatch한다. CLI와 public/common
internal API는 scheduler native 용어를 사용하지 않으며, metastore가 canonical run 상태와 parameters의
source of truth다. Native identity와 원본 상태는 adapter 진단 metadata로 격리한다.

### 1.0.12 LocalRunner 병렬 실행 및 취소 인프라

상태: 완료 (2026-07-19)

z4s CLI를 통해 로컬에서 실행하는 LocalRunner에 `ThreadPoolExecutor` 기반 위상 정렬 병렬 실행 기능과 Graceful/Hard Stop 취소 매커니즘, `fcntl`을 이용한 단일 실행 잠금 기능을 코어 외부에 추가했다. 

### 1.0.13 built-in step type registry 정본화

상태: 완료 (2026-07-20)

네 곳에 흩어져 하드코딩되던 built-in step type 목록을 단일 정본에서 파생하도록 통합했다.
`project` 계층의 `_STEP_TYPE_SPECS`(`StepTypeSpec`) registry 가 선언 정본이고, `STEP_TYPE`
enum·pool stage·schema validator·JSON Schema enum 을 여기서 파생한다. `core` 계층의
`_BUILT_IN_STEP_EXECUTOR_BUILDERS` 가 실행 매핑이며, import-time 가드로 project type 정본과의
congruence 를 강제한다. `StepGraphStep.type` 은 `Literal` 에서 `str` + field_validator
membership 검증으로 바꿔 이후 동적 등록의 전제를 만들었다. runtime 플러그인화 로드맵의
1단계이며, 다음 단계(명시 registry 등록, entry_points/plugin SDK)는 `backlog/` 에 있다.

### 1.0.14 scheduler 바인딩 generic 화 + 명시 registry 등록

상태: 완료 (2026-07-20)

runtime 플러그인화 로드맵의 2단계다. 두 갈래로 착지했다.

- **scheduler 바인딩 generic 화**: type 별 Airflow step adapter 층(`bind_airflow` + per-type
  registry)을 단일 generic 바인딩(`src/zeta4s/airflow/step_binding.py` 의
  `core_step_operator`)으로 붕괴시켰다. 모든 step type 이 같은 task 로 바인딩되고 실행 축
  dispatch 는 core `built_in_step_executor` 가 담당한다. Prefect 는 이미 generic 이었다.
- **명시 registry 등록**: 빌트인이 아닌 step type 을 profile `step_types` 로 명시 등록하는
  경로를 열었다. `StepTypeDescriptor`(`step_graph.py`)가 선언·실행 축을 함께 담고,
  `src/zeta4s/project/step_types.py` 가 project 파생 테이블을 재구성하며, core 가 generic
  `RuntimeCallableStepExecutor` 경로로 외부 type 을 실행한다. 등록은 CLI(`z4s project
  check`/`run`/`api deploy`)와 Prefect 워커의 명시 load-time 단계다.

Airflow deploy 경로의 외부 type 배선은 다음 마일스톤에서 profile `step_types` 대신
설치 패키지 entry-point discovery 로 해결한다.

### 1.0.15 외부 step type 을 설치=등록(entry-point discovery)으로 전환

상태: 완료 (2026-07-20)

runtime 플러그인화 로드맵의 3단계다. 외부 step type 등록 트리거를 profile `step_types` 명시
refs 에서 **설치 패키지 entry-point discovery** 로 바꿨다. 외부 type 은 runtime image 에 pip
설치되는 Python 패키지이고, 패키지가 `zeta4s.step_types` entry-point 에 factory 를 선언하면
설치 자체가 등록이다.

- profile `step_types` 필드와 Airflow deploy 조기 거부 가드를 제거했다. persist·thread 배선
  (registration snapshot / task op_kwargs / artifact metadata)이 불필요해졌다 — 설치된
  패키지는 어느 프로세스에서든 그대로 discover 되기 때문.
- `register_installed_step_types()`(`src/zeta4s/project/step_types.py`, stdlib
  `importlib.metadata` 만 사용)를 CLI, API 서버(deploy gate + runtime-step 실행), Airflow
  (DAG parse + task), Prefect 워커가 `StepGraphJob` 검증 이전에 호출한다.
- 이로써 Airflow·Prefect·runtime-step 세 실행 경로가 모두 외부 type 을 빌트인과 같은 계약으로
  실행한다. 남은 plugin SDK / compatibility range / plugin CLI 는
  `backlog/plugin-runtime-entry-points-sdk.md` 가 관리한다.

사용법: `../usage/step-type-plugins.md`. 설계: `../design/builtin-step-adapter-abstraction.md`.

### 1.0.16 Zeta4S commercial license entitlement

상태: 폐기 (1.0.20). 2026-07-27 에 완료했으나 1.0.20 에서 제거했다.

Zeta4Lab License Authority의 signed Zeta4S license를 `zeta4s-api`가 offline 검증하고
deployment, scheduler-neutral run capacity와 commercial adapter/plugin을 하나의
server-side decision으로 집행한다. Step Graph와 user project에는 entitlement를 저장하지
않으며 Airflow와 Prefect가 같은 durable admission 결과를 사용한다.

### 1.0.17 Zeta4S license revocation과 key rotation

상태: 폐기 (1.0.20). 2026-07-27 에 완료했으나 1.0.20 에서 제거했다.

Authority signed revocation snapshot과 versioned trust bundle을 license와 함께 offline
검증하고 PostgreSQL monotonic checkpoint로 stale, rollback과 revoked state를 fail-closed
집행한다.

### 1.0.18 Internal license release evidence

상태: 폐기 (1.0.20). 2026-07-27 에 완료했으나 1.0.20 에서 제거했다.

host wheel, API image와 API·Airflow·Prefect license gate를 하나의 immutable Authority
release 및 compatibility matrix에 결합한다. source revision, dependency lock, trust input,
release artifact와 각 gate statement의 digest를 strict product evidence로 만들고 Authority
상위 gate가 mixed revision, stale evidence와 mutable input을 fail-closed로 거절한다.

### 1.0.19 Secret 경계 단일화, master key 회전, k3s 배포 자산

상태: 완료 (2026-08-13)

secret 처리 경로를 AESGCM256 하나로 만든다. Fernet runtime key 경로는 호출부 없이
환경변수·API endpoint·metastore 컬럼·배포 배선만 점유하고 있었고, Airflow secrets backend는
headless 경계와 충돌한 채 배선된 적이 없었다. 둘 다 제거하고 재도입을 gate로 막는다.

master key를 세대 keyring으로 바꿔 회전을 가능하게 한다. ciphertext row가 자신을 암호화한
`key_id`를 기록하고 복호화는 그 세대를 고른다. 회전은 CAS in-place 재암호화이며 secret 쓰기는
advisory lock으로 직렬화한다. keyring 파일은 운영자 소유 입력이고 API는 읽기만 한다 —
Kubernetes Secret이 read-only mount이므로 배포 형태가 이 경계를 강제한다.

k3s 단일 노드 배포 자산을 둔다. master keyring은 `zeta4s-api`에만 0400 read-only로 노출하고
scheduler는 internal token만 받는다. 단일 노드에서는 `local-path` RWO PVC를 같은 노드의 Pod가
함께 mount하므로 generated DAG 공유에 RWX provisioner가 필요 없다.

### 1.0.20 Apache-2.0 오픈소스 전환, 상용 라이선스 집행 폐기

상태: 완료 (2026-10-09)

zeta4s 를 Apache License 2.0 으로 공개한다. 1.0.16 ~ 1.0.18 의 상용 라이선스 집행 —
signed license 검증, revocation checkpoint, deployment/run admission, surface/plugin
entitlement, internal release evidence — 를 제거한다. deploy, run, 외부 step type 등록은
라이선스 판정 없이 동작하고, 설치된 `zeta4s.step_types` plugin 은 모두 등록한다.
private Authority dependency 가 없으므로 CI, wheel, image build 에 별도 credential 이
필요 없다.

## Backlog

착수하지 않은 목표다. 항목마다 `backlog/` 에 파일 하나를 둔다. 목록은 두지 않는다 —
디렉토리를 보면 된다.

착수하면 `../plans/` 에 구현 계획을 쓰고, 구현이 끝나면 `../usage/`, `../design/`,
`../gate/` 를 현행화한 뒤 여기 마일스톤에 담는다.
