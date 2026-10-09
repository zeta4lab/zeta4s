# Prefect worker runtime 접근 범위 축소

상태: backlog

`prefect-worker` 는 compose 에서 `zeta4s-env` 앵커를 통째로 받고 runtime state volume 을
전체 mount 한다. metastore 자격증명과 iceberg token 이 모두 들어간다.

worker 가 secret 을 직접 풀지는 않는다 — `prefect/runtime.py` 는 이름과 달리
`zeta4s-api` 안에서 도는 코드이고, worker 의 flow entrypoint(`prefect_engine`)는 step 실행을
internal API 로 위임한다. master keyring 을 `zeta4s-api` 에만 주는 배선의 근거는
`../../design/secret-boundary.md` 에 있다.

남은 것은 그 밖의 입력이다. worker 가 runtime state 아래 무엇을 실제로 읽고 쓰는지, 받은
환경변수 중 무엇을 쓰는지 확인하고 최소 권한으로 좁힌다.

## 착수 조건

검증 수단이 필요하다. 환경변수나 volume 을 검증 없이 줄이면 Prefect 실행 경로가 조용히
깨진다. 단위 test 로는 드러나지 않고 compose stack 을 띄우는 release gate 에서만 확인된다.

## 착수 시 할 일

- worker 가 접근하는 경로와 환경변수를 실행 중 관측으로 확정한다.
- `docker-compose.yml` 의 `prefect-worker` 환경과 volume 을 그만큼으로 좁힌다.
- named volume 은 하위 경로 mount 가 안 되므로 volume 분할이 필요할 수 있다.
- Prefect release gate 로 checkpoint recovery 를 포함한 실행 경로가 그대로 도는지 확인한다.
- `deploy/k3s/patches/prefect-worker.yaml` 에 같은 축소를 반영한다.
