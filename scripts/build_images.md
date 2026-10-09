# zeta4s image build script

`build_images.sh` 는 zeta4s 컨테이너 이미지를 빌드한다. Docker Compose 의 `build:` 정의를
직접 쓰지 않고 image tag 와 push/load 동작을 명시적으로 고정하기 위한 스크립트다.

zeta4s가 빌드하는 image는 `zeta4s-api` 하나다. Airflow와 Prefect server는 각 프로젝트의
공식 image를 그대로 사용한다. `docker/zeta4s-api/Dockerfile`은 `zeta4s-api` distribution을
설치하며 airflow package를 포함하지 않는다.

`zeta4s-api` 이미지는 build 마지막에 `import airflow` 가 실패하는지 확인한다. 누가 의존을
되살리면 배포된 뒤 조용히 결합이 돌아오는 대신 build 가 깨진다.

## 기본 사용법

```bash
bash scripts/configure_open_env.sh --force
bash scripts/build_images.sh --load
docker compose up -d
```

실제 build 명령만 확인하려면 `--dry-run` 을 붙인다.

```bash
bash scripts/build_images.sh --load --dry-run
```
