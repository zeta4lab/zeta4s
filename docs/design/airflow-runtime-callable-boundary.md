# Airflow Runtime Callable Boundary

Airflow는 Step Graph의 control edge, retry, timeout, pool, trigger rule을 scheduler-native task로
projection한다. 실제 step runtime은 Airflow process가 아니라 `zeta4s-api`에서 실행한다.

## Parse-Time Contract

`zeta4s-api`가 publish한 DAG source는 Airflow package와 Python 표준 라이브러리만 import한다.
Parse 단계는 task graph를 구성할 뿐 project artifact를 읽거나 zeta4s module을 import하지 않는다.

DAG source에 포함되는 값은 deployment identity와 scheduler projection policy다.

- project/artifact/profile/job identity
- step identity와 control edge
- pool, retry, delay, timeout, trigger rule
- schedule과 DAG lifecycle 설정

Connection metadata, credential, SQL 본문, rowset state와 runtime callable은 포함하지 않는다.

## Task Execution Contract

Generated task callable은 Airflow context에서 run과 attempt identity를 읽고 authenticated internal
endpoint를 호출한다. `zeta4s-api`는 active deployment identity를 검증하고 canonical artifact에서
`ExecutionPlan`을 다시 만든 뒤 core runner로 해당 step을 실행한다.

이 경계 때문에 Oracle, ClickHouse, Elasticsearch, dbt, Arrow, DuckDB dependency는 Airflow image에
설치하지 않는다. Credential도 Airflow Connection이나 task payload로 전달하지 않고 API process가
profile policy와 encrypted secret store에서 resolve한다.

## Run Finalization

Terminal task 뒤의 finalize task는 모든 terminal state가 정해진 뒤 internal finalize endpoint를
호출한다. API는 metastore의 최신 step execution을 모아 canonical run result를 계산한다. 실패 result는
Airflow task failure로 다시 projection되어 DAG run이 성공으로 오인되지 않는다.

## 검증 기준

- generated source가 compile되고 `zeta4s` import를 포함하지 않는다.
- Airflow service가 공식 image와 generated DAG volume만 사용한다.
- internal endpoint가 token과 active deployment identity를 모두 검증한다.
- `zeta4s-api` image에는 airflow package가 없다.
