# docs/design

현재 코드의 설계다. 계획도 지향도 아니다 — 지금 코드가 이렇게 생겼다는 서술이므로
코드가 바뀌면 같이 바뀐다. 파일 목록은 두지 않는다. 목록을 적으면 그 목록이 먼저 낡는다.

- 여기는 zeta4s 를 **구현하기 위한** 내용만 둔다. 사용자가 workspace, profile, project 를
  작성하고 `z4s` 로 실행하는 법은 `../usage/` 에 있다.
- 검증 gate 는 `../gate/` 에 있다.
- 목표와 완성 여부는 `../roadmap/` 이, 진행 중 구현 계획은 `../plans/` 가 관리한다.
- 코드에서 파생 가능한 사실은 복제하지 않는다. 정본 좌표는 `../README.md` 의 표에 있다.

## 설계 기준

- AI Agent 또는 사용자가 생성한 contract 는 `project.yml`, `jobs/*.yml`, SQL/dbt 파일로
  저장되고 Step Graph 로 정규화된다.
- Runtime Engine 의 책임은 같은 Step Graph contract 로 검증, 배포, 실행, 관측을 수행하는
  것이다.
- workspace 는 Git repository 로 관리하는 것을 권장하는 deploy input 이며 `profiles/` 와
  `projects/` 를 포함한다.
- profile 은 project 밖 workspace `profiles/` 에서 관리한다.
- 실행은 host `z4s` CLI 와 Docker stack 안의 `zeta4s-api`/Airflow/Prefect adapter 로
  분리한다.
- `ExecutionPlan` 은 Airflow 와 독립적인 내부 실행 contract 다.
- DB adapter 종류는 project field 가 아니라 profile connection 의 `type` 으로 해석한다.
- Airflow 는 execution backend 이며, project contract 의 canonical 표현이 아니다.
