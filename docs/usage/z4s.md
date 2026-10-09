# z4s

`z4s` 는 host 에서 실행하는 CLI 다. 사용자는 workspace 에 작성한 project 와 profile 을
`z4s` 로 검증하고 배포하고 실행한다.

명령 목록과 option 은 `z4s --help` 가 정본이다. 여기에 복제하지 않는다. 이 문서는
명령을 언제 어떤 scope 로 쓰는지, 무엇을 알아야 동작하는지를 다룬다.

## z4s Home
`z4s` CLI 는 기본 home 을 사용자 계정 아래에 둔다. `ZETA4S_CLI_HOME` 을 설정하면 해당 경로를
z4s home 으로 사용한다.

```text
~/.zeta4s/
  config.yml
  secrets/
    api.token
  cache/
  reports/
```

Home 은 workspace 가 아니다. Home 은 로컬 CLI 설정, API connection, local secret, cache, report 를 저장한다.

`ZETA4S_HOME` 은 사용하지 않는다.

## API Connection

`z4s api connect [name]` 는 API token 을 생성하거나 기존 token source 를 재사용하고, 같은 name 을 기본
zeta4s-api endpoint 로 등록한다.

```bash
z4s api connect local --url http://127.0.0.1:8088
z4s api connect ci --url https://zeta4s.example.com --token-env ZETA4S_API_TOKEN
```

zeta4s-api endpoint 설정은 `z4s api` 하위 명령으로 관리한다.

```bash
z4s api list
z4s api use prod
z4s api remove prod
```

## API Home

`ZETA4S_API_HOME` 은 `zeta4s-api` service state root 다.

```text
/var/lib/zeta4s/
  artifacts/
  current/
  runs/
  locks/
```

API home 은 workspace 가 아니다. API home 은 artifact storage, scheduler snapshot, run artifact,
task result, lock 같은 service runtime state 를 저장한다. z4s home 과 workspace 는 API home 아래에
두지 않는다.

## API Operations

| Operation | 목적 |
|-----------|------|
| `api bootstrap` | `zeta4s-api` 가 소유한 metastore schema 를 생성하고 검증 |
| `api deploy` | project bundle 과 profile 을 검증하고 profile 이 선택한 scheduler backend 에 project 를 배포 |
| `api redeploy` | 기존 project 배포를 정리한 뒤 deploy |
| `api undeploy` | active deployment 에 저장된 scheduler backend 로 project 배포를 해제 |
| `api run` | active deployment가 선택한 scheduler에서 job run 생성·조회·취소 |

Runtime 변경은 API endpoint 를 통해 노출한다. CLI 로 필요한 runtime 테스트를 수행할 수 없으면
우회 스크립트를 만들지 않고 API/CLI contract 를 보강한다.

## CLI Command Scope

`z4s` CLI 는 host 에서 실행되는 client 다. 정적 검사는 local project files 를 기준으로 CLI 가
직접 책임지고, runtime 상태 변경과 runtime 조회는 `zeta4s-api` endpoint 를 호출해 결과 report 를
받는다. Project 배포와 해제는 `z4s api` 로 수행한다. `z4s schedule` 그룹은 제공하지 않는다.

`z4s api` 명령은 실행 대상 scope 를 명확히 나눈다.

| Scope | 의미 | CLI contract |
|-------|------|--------------|
| Project operation | local project bundle 과 profile 을 읽어 runtime 에 반영한다. | `project_id` 와 profile 선택이 필요하다. |
| Deployed project query | runtime 에 등록된 project/run 상태를 조회한다. | `project_id` 를 등록된 workspace 기준으로 해석한다. local bundle 은 freshness 확인이 필요한 경우에만 읽는다. |
| Runtime system operation | metastore, scheduler, artifact storage 같은 zeta4s runtime 상태를 점검한다. | project 와 profile 에 의존하지 않는다. |
| Backend operation | profile 에 선언된 runtime backend connection 을 대상으로 한다. | profile 을 먼저 선택하고, 필요한 경우 profile 안의 connection id 를 선택한다. Metastore 와 섞지 않는다. |

CLI 는 z4s home config 에 등록되고 현재 활성화된(active) workspace 기준으로 project id 와 profile id 를 해석한다.

```text
retail -> <workspace>/projects/retail
prod   -> <workspace>/profiles/prod.yml
```

Profile 이 workspace 에 1개만 있으면 그 profile 을 선택한다. Profile 이 여러 개면 `--profile <profile_id>`
가 필요하다. CLI option 에 profile file 확장자를 쓰지 않는다.

`api` 하위 명령은 다음 기준에 맞춰 구성한다.

| Command | 목표 scope | 기준 |
|---------|------------|------|
| `api bootstrap` | Runtime system operation | `zeta4s-api` service config 의 metastore backend 를 bootstrap 하고 schema 를 검증한다. |
| `api deploy` | Project operation | bundle 과 profile 을 project 단위로 적용하고 선택한 scheduler backend 를 active deployment metadata 에 기록한다. |
| `api redeploy` | Project operation | 저장된 backend 로 기존 배포를 해제한 뒤 동일한 deploy contract 를 수행한다. backend data cleanup 은 포함하지 않는다. |
| `api undeploy` | Project operation | local profile 이 아니라 active deployment 에 저장된 backend 로 scheduler 배포와 artifact registration 을 해제한다. backend data cleanup 은 포함하지 않는다. |
| `api run` | Deployed project query/operation | active deployment를 project/job/run 기준으로 실행하고 관측한다. |

재구성 기준:

- CLI 는 metastore bootstrap 정보를 profile 에서 읽지 않는다.
- `api deploy` 는 metastore bootstrap 을 암묵 실행하지 않는다. Metastore 가 준비되지 않았으면
  `z4s api bootstrap` 실행을 안내하고 실패한다.
- `api` 명령은 project data backend 를 metastore 로 간주하지 않는다.
- project context 자동 해석은 Project operation 과 Deployed project query 에만 허용한다.
- cleanup 은 현재 CLI/API surface 에 두지 않고 향후 maintenance contract 로 설계한다.

## Reports

Metastore 는 runtime operation report 의 source of truth 다. CLI 는 API report 를
z4s home 의 `reports/<project_id>/` 아래에 local copy 로 저장한다. 최신 local copy 는
`<report-name>.latest.json` 으로 유지한다.

대표 report:

- `profile-check`
- `project-check`
- `api-bootstrap`
- `api-deploy`
- `api-redeploy`
- `api-undeploy`
- `api-run-summary`

Report status 는 `passed` 또는 `failed` 로 판정한다. issue 는 code, severity, message,
step/probe 같은 위치 정보를 포함한다.

## Artifact Freshness

Project bundle 은 deterministic artifact id 를 갖는다. `api run create` 는 local bundle artifact id
와 metastore 의 active deployment artifact id 를 비교할 수 있으며, stale deployment 는 명확히
경고하거나 실패시킬 수 있다.

## Scheduler Run

Canonical identity는 `project_id`, `job_id`, `run_id`다. `z4s api run`은 active deployment의
profile에 기록된 scheduler를 사용하므로 사용자가 Airflow 또는 Prefect를 명령에서 다시 고르지
않는다. Native scheduler identity와 상태는 adapter가 canonical run/task 상태로 정규화하며,
진단에 필요한 native 값만 `adapter_metadata`로 격리한다. Run 입력은 `parameters` 한 계약으로
받아 각 scheduler의 native 입력에 투영한다.

Root `z4s run`은 host process에서 Step Graph를 위상 정렬 기반의 다중 워커 풀로 직접 실행하는 local runner다. 동일한 작업의 중복 실행 방지 및 2단계 취소(Graceful/Hard Stop)를 지원한다. `z4s api run`은 배포된 scheduler operation이므로 둘의 실행 위치와 운영 정책은 다르다.

## Schedule Lifecycle

Schedule identity 는 `project_id`, `job_id`, `profile` 조합이다. Canonical source 는
`jobs/*.yml` 의 `schedule` 이며 CLI option 으로 cron/interval/timezone 을 덮어쓰지 않는다.
Schedule deployment 는 `z4s api deploy` 의 일부다. 별도 schedule 배포 명령은 두지 않는다.
Schedule 전용 CLI 그룹과 API endpoint 는 제공하지 않는다. 운영 interface 는 별도 계획에서
정의한다.

Schedule run 의 step 상태는 scheduler 종류와 무관한 metastore contract 를 사용한다. Canonical
`attempt` 는 step retry 횟수이고 `metadata.adapter_attempt` 는 scheduler infrastructure attempt 로
분리한다.


## Secret
`z4s api bootstrap` prepares the metastore schema and validates the secret
master key file. The master key is never written to the metastore.

Runtime secret helpers:

```bash
z4s api secret set prod.analytics_clickhouse.password
printf '%s' "$PROD_ANALYTICS_CLICKHOUSE_PASSWORD" | z4s api secret set prod.analytics_clickhouse.password
z4s api secret list
z4s api secret check prod.analytics_clickhouse.password
```

`secret set` accepts the plaintext value only through a hidden interactive
prompt or stdin for non-interactive automation, and sends it to `zeta4s-api` for
encryption. It must not accept password values through shell arguments, project
files, deploy payloads, reports, or Airflow Connection rows. `secret list` and
`secret check` return only metadata and status.
