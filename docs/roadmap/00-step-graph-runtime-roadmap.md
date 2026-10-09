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

## 완성된 범위

- **Contract** — `project.yml`, `jobs/*.yml`, SQL/dbt 파일이 canonical contract 이고
  `ExecutionPlan` 이 scheduler 중립 내부 실행 contract 다. step flow control, storage-neutral
  rowset, step checkpoint 를 포함한다.
- **내장 step type** — ClickHouse/Oracle/Elasticsearch runtime adapter, dbt/SQL transform,
  `http.lookup` 을 단일 registry 정본(`_STEP_TYPE_SPECS`)에서 선언하고 실행 매핑과의 일치를
  import 시점에 강제한다.
- **외부 step type** — `zeta4s.step_types` entry-point 를 선언한 package 를 설치하면 등록된다.
  CLI, `zeta4s-api`, Airflow, Prefect 가 내장 type 과 같은 계약으로 실행한다.
  사용법: `../usage/step-type-plugins.md`. 설계: `../design/builtin-step-adapter-abstraction.md`.
- **Scheduler backend** — Airflow 와 Prefect 를 같은 외부 backend 계약으로 연결한다. profile 이
  backend 를 고르고, deploy 가 project 별 backend 를 metastore 에 기록하며, undeploy 는 기록된
  backend 를 따른다. Airflow 는 공식 image 를 쓰고 `zeta4s-api` 가 DAG source 를 projection 한다.
  credential 해석과 step 실행은 `zeta4s-api` 안에서 일어난다.
- **Pool binding** — Step Graph 에서 effective pool 을 backend 중립으로 산출하고 각 scheduler
  resource 로 동기화한다. 자동 pool 이 기본이고 사용자 지정 pool 은 예외적 override 다. limit 초과
  contention 시간은 gate 가 직접 측정하지 않는다.
- **Run 계약** — CLI 와 public API 는 `project_id`/`job_id`/`run_id` 로 run 을 다루고 scheduler
  native 용어를 쓰지 않는다. canonical run 상태와 parameters 의 정본은 metastore 다.
- **Checkpoint recovery** — scheduler 실행은 Iceberg snapshot 과 step-local checkpoint 로 외부
  backend 장애 뒤 같은 run 안에서 재개한다.
- **LocalRunner** — `z4s run` 은 위상 정렬 병렬 실행, graceful/hard stop 취소, 단일 실행 잠금을
  제공한다.
- **Workspace** — 다중 workspace 컨텍스트와 profile 단위 `api_endpoint` 를 지원한다.
- **Secret** — secret 은 AESGCM256 세대 keyring 으로 암호화하고 CAS 재암호화로 회전한다.
  keyring 은 `zeta4s-api` 만 읽는다.
- **Scheduler runtime 입력** — Prefect worker 와 generated Airflow DAG 은 internal API endpoint 와
  internal token 만 받고 runtime state volume 을 mount 하지 않는다. compose 와 k3s 의 worker 입력
  선언은 gate 가 대조한다.
- **배포** — Docker compose 가 로컬 검증 계약이고 `deploy/k3s/` 가 단일 노드 k3s 배포 계약이다.

## Backlog

착수하지 않은 목표다. 항목마다 `backlog/` 에 파일 하나를 둔다. 목록은 두지 않는다 —
디렉토리를 보면 된다.

착수하면 `../plans/` 에 구현 계획을 쓰고, 구현이 끝나면 `../usage/`, `../design/`,
`../gate/` 를 현행화한 뒤 위 완성된 범위에 반영한다.
