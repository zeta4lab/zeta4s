# zeta4s CLI install script

`install_cli.sh` 는 host 에 `z4s` CLI 설치본을 설치하기 위한 스크립트다. repository 표준 Python
도구는 `uv` 이며, 기본값은 현재 checkout 의 `zeta4s` library 와 `zeta4s-cli` entrypoint package 를
`.venv` 에 editable package 로 설치한다.

에이전트 검증에서는 사용자 `.venv` 를 쓰지 않고 `uv` 로 별도 환경을 명시한다.

```bash
bash scripts/install_cli.sh --venv-dir .venv-codex-runtime
```

## 기본 사용법

```bash
uv sync --extra cli
bash scripts/install_cli.sh
```

설치 후 CLI 는 다음 경로에 생긴다.

```bash
# Linux/macOS
.venv/bin/z4s --help

# Windows Git Bash
.venv/Scripts/z4s.exe --help
```

개발용 extra 까지 설치하려면 `--with-dev` 를 붙인다.

```bash
bash scripts/install_cli.sh --with-dev
```

wheel 파일을 설치할 수도 있다.
`--package` 는 반복할 수 있으며, local 배포에서는 `zeta4s` library wheel 과
`zeta4s-cli` entrypoint wheel 을 함께 지정한다.

```bash
bash scripts/install_cli.sh \
  --package dist/zeta4s-<version>-py3-none-any.whl \
  --package dist/zeta4s_cli-<version>-py3-none-any.whl
```

## 주요 옵션

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--env-file FILE` | 없음 | package index 환경 변수를 읽을 env 파일 |
| `--venv-dir DIR` | `.venv` | virtualenv 생성 경로 |
| `--package TARGET` | 없음 | editable checkout 대신 설치할 wheel/path. 반복 가능 |
| `--index-url URL` | `PIP_INDEX_URL` | package index URL |
| `--extra-index-url URL` | `PIP_EXTRA_INDEX_URL` | package extra index URL |
| `--trusted-host HOST` | `PIP_TRUSTED_HOST` | pip trusted host |
| `--with-dev` | 없음 | editable checkout 설치 시 `.[dev]` extras 포함 |
| `--no-upgrade-pip` | 없음 | 설치 전 pip upgrade 생략 |
| `--no-verify` | 없음 | 설치 후 `z4s --help` 검증 생략 |
| `--dry-run` | 없음 | 실행할 명령만 출력 |
