# API

`zeta4s-api` 가 host CLI, metastore, artifact storage, Airflow, Prefect 와 주고받는
내부 규약이다. 사용자가 `z4s` 를 쓰는 법은 `../usage/z4s.md` 에 있다.

## Scheduler Backend Ownership

Project 배포의 유일한 진입점은 `api deploy` 다. API 는 request profile 의 `scheduler`를
해석해 Airflow 또는 Prefect 에 같은 Step Graph contract 를 배포하고, 실제 선택 결과를
project 별 active deployment metadata 의 `scheduler_backend` 에 기록한다.

Profile 은 배포 입력이고 active deployment metadata 는 배포 결과다. `api undeploy`는 local
profile 을 다시 해석하지 않고 저장된 `scheduler_backend`로 분기한다. 따라서 deploy 뒤
profile file 이 바뀌어도 원래 backend 를 정리한다.

Airflow active registration 만 Airflow scheduler snapshot 으로 발행한다. Prefect registration도
metastore 에는 유지되지만 Airflow DAG parse 입력에는 들어가지 않는다.

## Deploy And Undeploy

두 backend 는 project bundle 검증, profile 검증, artifact 저장, active registration과 report
계약을 공유한다. Airflow 는 DAG와 Airflow projection state를, Prefect 는 deployment를 각각
자기 공개 interface로 관리한다. Backend별 세부 step 이름과 endpoint 목록은 코드가 정본이다.

`api undeploy`는 active registration을 먼저 읽고 해당 backend를 정리한 뒤 registration을
제거한다. `api redeploy`는 이 undeploy와 deploy 계약을 순서대로 수행한다.

## Schedule Surface

Schedule definition은 project deploy에 포함된다. 별도 schedule CLI 그룹과 schedule 전용
endpoint는 없다. Schedule 운영 interface는 현재 계약에 포함하지 않는다.

Prefect 구현 package 는 `zeta4s.prefect`다.

## Failure Semantics

- Project/profile/dbt 검증 실패는 scheduler deployment와 active registration 전에 중단한다.
- 저장된 backend를 해석할 수 없거나 active registration이 없으면 undeploy를 닫힌 실패로
  반환한다.
- Active run 이 남아 있으면 redeploy 를 실패시킨다.
- Final report 는 실패 step 이후 downstream step 을 `skipped` 로 표시한다.
