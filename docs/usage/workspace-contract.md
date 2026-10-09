# Workspace Contract

## 목적

Workspace 는 zeta4s 배포 입력의 최상위 작업 디렉토리다. Workspace 는 여러 project 와 그 project 들이
공유하는 profile 을 함께 보관한다. Workspace 는 Git repository 로 관리하는 것을 권장한다.

## Workspace Bootstrap

`z4s work init` 은 현재 디렉토리 아래 기본 workspace 를 만들고 지정한 이름으로 등록한 뒤 즉시 활성화한다.
z4s 는 다수의 workspace 를 등록해두고 컨텍스트 스위칭(`use`)할 수 있다.

```bash
z4s work init
```

결과:

```text
./zeta4s-work/
  profiles/
  projects/
```

이름을 지정하면 현재 디렉토리 아래 지정한 이름으로 만든다.

```bash
z4s work init analytics-work
```

결과:

```text
./analytics-work/
  profiles/
  projects/
```

## Workspace Layout

Canonical workspace 이름은 `zeta4s-work` 다.

```text
zeta4s-work/
  profiles/
    dev.yml
    prod.yml
  projects/
    retail/
      project.yml
      jobs/
      sql/
      dbt/
```

`profiles/` 는 workspace 안의 모든 project 가 공유하는 deploy profile 을 담는다.
`projects/<project_id>/` 는 개별 project artifact 를 담는다.

Project 안에 `profiles/` directory 를 만들지 않는다.

## Workspace Context

등록된 전체 workspace 목록은 `z4s work list` 로 확인한다.
```bash
z4s work list
```

원하는 workspace 로 전환하려면 `z4s work use` 를 사용한다.
```bash
z4s work use analytics-work
```

## Workspace Show

`z4s work show` 는 z4s home config 에 저장된 현재 활성화된(active) workspace 정보를 보여준다. 디렉토리를 검색하지 않는다.

출력 항목:

- `home`: 현재 z4s home path
- `active_workspace_name`: 현재 활성화된 workspace 이름
- `workspace`: 활성화된 workspace path
- `exists`: workspace path 존재 여부
- `profiles_dir`: workspace profiles directory path
- `projects_dir`: workspace projects directory path
- `profile_count`: profile file count
- `project_count`: project directory count

Workspace 는 `profiles/` 와 `projects/` 가 모두 있는 디렉토리다.

## Path Resolution

CLI argument 는 home config 에 등록된 workspace 기준으로 해석한다.

```bash
z4s project init retail
z4s project check retail --profile dev
z4s api deploy retail --profile prod
```

해석:

```text
retail -> <workspace>/projects/retail
dev    -> <workspace>/profiles/dev.yml
prod   -> <workspace>/profiles/prod.yml
```

## Project Init

`z4s project init <project_id>` 는 등록된 workspace 의 `projects/<project_id>` 를 생성한다.

```text
<workspace>/
  projects/
    <project_id>/
      project.yml
      jobs/
      docs/
```

Project init 은 project-local profile 을 만들지 않는다.

## CI/CD Contract

CI/CD 는 Git repository 로 관리하는 workspace checkout 을 기준으로 실행하는 것을 권장한다.

Release gate 가 repo-local showcase project 를 검증할 때는 source checkout 의 project 를 직접
workspace 로 간주하지 않는다. Gate 는 host 에 설치한 `z4s` CLI 로 임시 `zeta4s-work/` 를 만들고,
`z4s project init`, `z4s profile init` 으로 workspace 구조를 생성한 뒤 검증 대상 project 와 profile 을
각각 `projects/<project_id>/`, `profiles/<profile_id>.yml` 로 복사해 실행한다.
기본 release gate workspace 는 임시 디렉토리 아래 만들어지고 shell 종료 시 삭제된다. 운영자가
`RELEASE_WORKSPACE` 를 명시한 경우에만 해당 경로를 보존한다.

```bash
z4s project check retail --profile prod
z4s api deploy retail --profile prod
```

Profile 은 deploy binding 이며 secret value 를 포함하지 않는다. Profile 은 `password_ref` 같은 secret
reference 만 포함한다. Secret value 는 zeta4s-api secret store 에서 해석하며 `z4s api secret` 명령으로
관리한다.
