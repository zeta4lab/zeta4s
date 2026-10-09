# 오픈망 `.env` 생성 스크립트

`configure_open_env.sh` 는 `.env.example` 을 기준으로 로컬 오픈망용 `.env` 를
생성한다. Docker build 나 Compose 기동은 직접 실행하지 않고, env 파일 생성만
담당한다.

template 위에 `ZETA4S_VERSION`, `ZETA4S_API_IMAGE`(`zeta4s-api:<version>` local tag),
새로 생성한 `ZETA4S_RUNTIME_INTERNAL_TOKEN`, `AIRFLOW_UID`, `ZETA4S_RUNTIME_UID` 를 채운다.
version 은 `src/zeta4s/__init__.py` 의 `__version__` 에서 읽는다.
`AIRFLOW_UID`와 `ZETA4S_RUNTIME_UID`는 각각 `50000`으로 생성한다. 두 값은 같은 숫자여도
서로 다른 runtime의 file ownership 계약이다.
`ZETA4S_API_TOKEN` 은 기본적으로 비워 둔다. 비어 있으면 zeta4s-api 인증을 생략하고,
값을 직접 설정한 경우에만 healthz 외 endpoint 에 Bearer token 을 요구한다.

## 사용 예

```bash
bash scripts/configure_open_env.sh --force
```

다른 파일로 생성하려면 다음처럼 실행한다.

```bash
bash scripts/configure_open_env.sh \
  --output .env.dev \
  --force
```

생성 전 내용을 확인하려면 `--dry-run` 을 사용한다.

```bash
bash scripts/configure_open_env.sh --dry-run
```

## 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--output FILE` | `.env` | 생성할 env 파일 |
| `--template FILE` | `.env.example` | 입력 template |
| `--force` | 없음 | 기존 output overwrite |
| `--dry-run` | 없음 | 파일을 쓰지 않고 생성 내용을 stdout 으로 출력 |

## 다음 단계

`ZETA4S_API_IMAGE` 는 local tag 이므로 Compose 기동 전에 image 를 빌드한다.

```bash
bash scripts/build_images.sh --load
docker compose --env-file .env --profile prefect --profile checkpoint up -d --wait zeta4s-api prefect-worker
```

Airflow, Oracle, Elasticsearch, ClickHouse 까지 띄우는 전체 구성은 `--profile airflow --profile asset` 을
더한다.

기본 Compose 명령도 `.env` 를 자동으로 읽지만, 사용하는 env 파일을 명확히 남기려면
`--env-file .env` 를 붙인다.
