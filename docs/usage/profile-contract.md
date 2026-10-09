# Profile Contract

## 목적

Profile 은 project 실행에 필요한 외부 connection 과 환경 값을 담는 사용자별 실행 설정이다.
Project artifact 는 무엇을 실행할지 정의하고, profile 은 어디에 연결해서 실행할지 정의한다.

Profile 은 project artifact 에 포함하지 않는다. Profile 은 workspace 에 속하며, 같은 workspace 의 여러
project 가 공유할 수 있다.

## 저장 위치

Profile 은 workspace 아래에 둔다.

```text
zeta4s-work/
  profiles/
    dev.yml
    prod.yml
  projects/
```

Project 안에 `profiles/` directory 를 만들지 않는다.

## Profile 내용

Profile 은 step 이 참조하는 외부 resource 를 선언한다.

필수 top-level field:

- `connections`

선택 top-level field:

- `scheduler`
- `variables`
- `api_endpoint`
- `token_env`

예:

```yaml
scheduler: prefect

connections:
  oracle_source:
    type: oracle
    host: oracle
    port: 1521
    username: showcase_src
    password_ref: dev.oracle_source.password
    database: freepdb1
    schema: showcase_src
    options:
      service_name: freepdb1

  analytics_clickhouse:
    type: clickhouse
    host: clickhouse
    port: 8123
    username: default
    password_ref: dev.analytics_clickhouse.password
    database: analytics

variables:
  timezone: Asia/Seoul
```

`connections` key 는 project step 의 `conn` 값과 매칭된다. `conn` 은 logical connection id 이며,
물리 endpoint 와 credential 은 profile 에서 해석한다.

## Scheduler Backend

`scheduler` 는 project 를 배포할 scheduler backend 다. 허용값은 `airflow`, `prefect` 두
가지이며 생략하면 `prefect` 다. Scheduler 선택은 실행 환경 설정이므로 project artifact 에
넣지 않는다.

`z4s api deploy` 는 선택한 값을 project 별 active deployment metadata 의
`scheduler_backend` 로 저장한다. 이 metadata 는 실제 배포 결과의 정본이다. 이후 local
profile 의 `scheduler` 값이 바뀌어도 undeploy 는 저장된 backend 를 사용한다.

## Step Type 등록

빌트인이 아닌 외부 step type 은 profile 이 아니라 **설치 패키지 entry-point** 로 등록한다.
설치가 곧 등록이므로 profile 에 나열하지 않는다. 사용법은 `step-type-plugins.md` 를 본다.

## API Endpoint 오버라이드

Profile 은 런타임 제어에 사용할 `zeta4s-api` 엔드포인트를 직접 선언할 수 있다.

```yaml
api_endpoint: https://prod.zeta4s.internal:8443
token_env: PROD_ZETA4S_API_TOKEN
```

이 값이 명시된 Profile 을 선택하여 `z4s api deploy` 나 `z4s api redeploy` 를 실행할 경우, `--api` 인자나 CLI 전역 설정(`~/.zeta4s/config.yml`)에 저장된 통신 설정보다 최우선 적용된다. 인가 토큰은 `token_env` 에 지정된 환경 변수에서 읽는다.

## Connection Schema

`connections` 는 map 이다. Map key 가 connection id 이며 project step 의 `conn` 값과 정확히 일치해야
한다.

공통 field:

| Field | 필수 | 의미 |
|-------|------|------|
| `type` | 필수 | runtime data backend adapter 선택 key |
| `host` | 선택 | host/port 기반 connection 의 host |
| `port` | 선택 | host/port 기반 connection 의 port |
| `url` | 선택 | HTTP endpoint 기반 connection 의 endpoint |
| `username` | 선택 | 인증 사용자 |
| `password_ref` | 선택 | secret store 의 password key 또는 secret URI |
| `database` | 선택 | DB 제품별 database, service, catalog |
| `schema` | 선택 | DB 제품별 schema 또는 user namespace |
| `options` | 선택 | adapter-specific 확장값 |

Adapter 선택은 `type` 으로만 한다. `type` 값은 지원 runtime data backend adapter 와 일치해야 한다.
`z4s profile check` 는 `host`/`port` 기반 connection 을 probe 할 때 driver 별로 HTTP/HTTPS 또는
TLS/plain 후보를 자동 판단한다. `url` 에 scheme 이 있으면 해당 scheme 을 우선한다.

금지 field:

- `conn_id`
- `conn_type`
- `connection`
- `login`
- `password`
- `extra`
- `pools`
- `metastore`
- `checkpoint`
- `rowset_storage`

`conn_id` 는 map key 로 표현한다. `conn_type` 은 `type` 으로 표현한다. `login` 은 `username` 으로
표현한다. `extra` 는 `options` 로 표현한다. Profile 에 평문 `password` field 를 쓰지 않는다.

## Secret Contract

Profile 은 secret value 자체를 저장하지 않는다. Profile 은 secret reference 만 저장한다.

```yaml
connections:
  oracle_source:
    type: oracle
    username: showcase_src
    password_ref: dev.oracle_source.password
```

`z4s api deploy --profile prod` 는 profile 의 connection metadata 와 `password_ref` 를 zeta4s-api
deployment policy 로 등록한다. Airflow task는 credential을 받지 않고 step identity만 internal
endpoint로 전달한다. Credential resolution과 core step 실행은 `zeta4s-api` process 안에서 수행한다.

Production runtime 에서는 Airflow metastore connection password 를 credential 저장소로 사용하지 않는다.
Internal execution과 secret/master-key 경계는 `docs/design/scheduler-internal-execution.md`에서 정의한다.

Release gate 는 profile 에 `password`, `login`, `conn_type`, `connection`, `extra`, `pools`,
`metastore`, `checkpoint`, `rowset_storage` 같은 금지 field 가 없는지 검증해야 한다.

## 금지사항

Profile 은 scheduler pool resource 를 포함하지 않는다.

Scheduler pool resource 는 사용자가 profile 에 작성하지 않는다. 기본 부하 조절은 Step Graph 에서
자동 산출하며, project schema 가 허용하는 explicit pool binding 은 예외적 override 이므로 권장하지
않는다. Airflow Pool 과 Prefect Global Concurrency Limit 은 scheduler projection state 다.
Scheduler resource 형식은 public authoring contract 가 아니다.

Profile 은 metastore connection 을 포함하지 않는다.

Metastore 는 zeta4s platform 상태 저장소이며 `zeta4s-api` service config 로 관리한다. `z4s` CLI 와
project profile 은 metastore endpoint, credential, namespace 를 알 필요가 없다.

Profile 은 checkpoint catalog, object storage, rowset physical format도 포함하지 않는다. 이 값들은
platform deployment 설정이며 verification Runner와 scheduler-projected runtime이 실행 mode에 따라
선택한다.

## CLI Contract

Profile 관리는 `z4s profile` group 이 담당한다.

```bash
z4s profile init local
z4s profile edit local
z4s profile show local
z4s profile check local
z4s profile list
z4s profile delete local
```

`z4s profile edit <profile_id>` 는 profile YAML 을 editor 로 열고 저장 후 schema 를 검증한다.
`z4s profile show <profile_id>` 는 현재 profile YAML 을 출력한다.
`z4s profile delete <profile_id>` 는 profile file 을 삭제한다.

Profile 명령은 Linux/macOS shell, Windows PowerShell, Windows Git Bash 에서 같은 인자 구조로
동작해야 한다. `edit` 는 platform shell script 에 의존하지 않고 host 의 editor 실행 규칙을 따른다.

`z4s profile check <profile_id>` 는 local profile file 을 읽은 뒤 `zeta4s-api` 로 profile payload 를
전송한다. `zeta4s-api` 는 profile schema, `password_ref` secret 상태, connection probe 를 확인하고
operation report 를 반환한다. CLI 는 반환된 report 를 z4s home 의
`reports/<profile_id>/profile-check.latest.json` 에 저장한다. Local report 는 `profile` field 로
검증한 profile id 를 기록한다.

Project 검증과 API deploy 는 profile 이름을 받는다.

```bash
z4s project check retail --profile dev
z4s api deploy retail --profile prod --api prod
```

CLI 는 z4s home config 에 등록된 workspace 기준으로 `retail` 을 `<workspace>/projects/retail`,
`dev` 를 `<workspace>/profiles/dev.yml` 로 해석한다.

`--profile` 은 profile id 를 받는 canonical option 이다. Profile 이 workspace 에 1개만 있으면 그
profile 을 선택한다. Profile 이 여러 개면 `--profile <profile_id>` 가 필요하다. CLI option 에 profile
file 확장자를 쓰지 않는다.

## Secret Reference
Profiles keep connection metadata and secret references only.

```yaml
connections:
  analytics_clickhouse:
    type: clickhouse
    host: clickhouse
    port: 8123
    username: metastore
    password_ref: prod.analytics_clickhouse.password
    database: analytics
```

`password_ref` is a zeta4s secret key. It is not a password value and is not an
Airflow metastore connection field. Project steps continue to reference only the
logical `conn` id.
