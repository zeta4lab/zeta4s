# k3s 단일 노드 배포

zeta4s를 k3s 단일 노드 namespace에 배포하는 manifest다. compose가 로컬 검증 계약이듯
여기 manifest가 배포 계약이다.

## 전제

- k3s 단일 노드. 기본 `local-path` StorageClass를 쓴다.
- PostgreSQL metastore, Airflow, Prefect, 외부 data backend는 이미 운영 중이다.
  이 manifest는 그것들을 배포하지 않고 접속 정보만 받는다.
- `zeta4s-api` image가 cluster에서 pull 가능하다.

단일 노드를 전제하는 이유: `local-path`는 RWO지만 단일 노드에서는 같은 노드의 Pod들이
함께 mount하므로 generated DAG 공유에 RWX provisioner가 필요 없다. 다중 노드는 RWX storage와
Pod 배치 제약을 다시 판단해야 하는 별도 목표다.

## secret 경계

Secret은 역할별로 나눈다. 하나로 합치면 mount 대상이 같아져 경계가 사라진다.

| Secret | 받는 Pod | 전달 방식 |
|---|---|---|
| `zeta4s-secret-master-keyring` | `zeta4s-api`만 | file mount (0400, read-only) |
| `zeta4s-runtime-internal-token` | `zeta4s-api`, Airflow, Prefect worker | env |
| `zeta4s-external-credentials` | `zeta4s-api`만 | env |
| `zeta4s-airflow-rest-passwords` | `zeta4s-api`만 | file mount (read-only) |

master keyring이 file mount인 것은 코드 계약이 파일 경로
(`ZETA4S_SECRET_MASTER_KEY_FILE`)이기 때문이다. internal token이 env인 것은 generated
DAG source가 `os.environ`으로 읽기 때문이다. 계약을 바꾸지 않는 것이 우선이다.

Airflow와 Prefect worker는 master keyring과 외부 자격증명을 받지 않는다. runtime
database password는 Kubernetes Secret으로 옮기지 않는다 — encrypted secret store에
남아 `zeta4s-api` process 안에서만 resolve된다.

## 적용

```bash
# 1. 네임스페이스
kubectl apply -f 00-namespace.yaml

# 2. Secret 준비 (예시 값을 그대로 쓰지 않는다)
#    master keyring 은 z4s 가 만든 파일을 그대로 넣는다.
python -c 'from pathlib import Path; from zeta4s.runtime.secrets import init_master_key_file; init_master_key_file(Path("master.json"))'
kubectl -n zeta4s create secret generic zeta4s-secret-master-keyring --from-file=master.json=master.json
shred -u master.json

kubectl -n zeta4s create secret generic zeta4s-runtime-internal-token \
  --from-literal=ZETA4S_RUNTIME_INTERNAL_TOKEN="$(openssl rand -hex 32)"

kubectl -n zeta4s create secret generic zeta4s-external-credentials \
  --from-literal=ZETA4S_API_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=ZETA4S_METASTORE_DSN='postgresql://user:password@postgres:5432/zeta4s_metastore' \
  --from-literal=ZETA4S_AIRFLOW_REST_API_PASSWORD='...'

kubectl -n zeta4s create secret generic zeta4s-airflow-rest-passwords \
  --from-file=simple_auth_manager_passwords.json=...

# 3. 나머지
kubectl apply -f 10-rbac.yaml -f 20-storage.yaml -f 30-zeta4s-api.yaml -f 40-networkpolicy.yaml
```

`ZETA4S_API_TOKEN`이 비어 있으면 public API가 무인증이 된다. manifest 검사가 이를
거절한다(`scripts/check_k3s_manifests.sh`).

## host `z4s` CLI 접근

Service는 ClusterIP다. internal execution endpoint를 cluster 밖으로 내보내지 않기 위해서다.
운영자가 `z4s api ...`를 쓰려면 port-forward로 잠깐 연결한다.

```bash
kubectl -n zeta4s port-forward svc/zeta4s-api 18088:8088 &
z4s api connect k3s --url http://127.0.0.1:18088
z4s api status --api k3s
```

port-forward는 kubectl 인증을 그대로 쓰므로 cluster 밖에 포트를 여는 것보다 안전하다.
상시 노출이 필요하면 운영 정책에 따라 별도 Ingress를 두되, `/internal` 경로가 그쪽으로
새지 않게 해야 한다. 이 manifest는 그런 Ingress를 포함하지 않는다.

## scheduler 배선

이 manifest는 `zeta4s-api`만 배포한다. Airflow와 Prefect는 각자 공식 배포 방식(Helm chart
등)을 쓰고, zeta4s는 거기에 붙는 **환경 배선만** 정의한다.

```bash
kubectl apply -f 50-scheduler-config.yaml

kubectl -n zeta4s patch statefulset airflow-scheduler \
  --patch-file patches/airflow-scheduler.yaml
kubectl -n zeta4s patch deployment prefect-worker \
  --patch-file patches/prefect-worker.yaml
```

workload 이름과 container 이름은 배포 방식마다 다르므로 patch 파일의 `name`을 그 배포에
맞게 바꾼다. Helm을 쓴다면 같은 내용을 values의 extraEnv / extraVolumeMounts로 옮긴다.

배선이 주는 것은 세 가지다.

| 항목 | 이유 |
|---|---|
| `zeta4s-airflow-dags` PVC를 `/opt/airflow/dags`에 mount | Airflow가 생성된 DAG를 읽는 유일한 공유 지점 |
| `ZETA4S_API_INTERNAL_URL`, `ZETA4S_RUNTIME_INTERNAL_TOKEN` | generated DAG source가 `os.environ`으로 읽는 코드 계약 |
| `zeta4s.io/runtime: scheduler` label | `zeta4s-api`의 ingress 정책이 발신자를 식별한다 |

**이 배선이 없으면 `z4s api deploy`가 DAG discovery 단계에서 timeout된다.** Airflow가
generated DAG를 볼 수 없기 때문이다.

scheduler를 다른 namespace에 두면 그 namespace에 `zeta4s.io/scheduler: "true"` label을
붙인다. `zeta4s-api`의 ingress가 그 namespace를 발신자로 인정한다.

**scheduler Pod의 egress는 zeta4s가 정하지 않는다.** `40-networkpolicy.yaml`의 default
deny는 zeta4s 소유 Pod만 덮는다. Airflow가 자기 metastore로, Prefect worker가
`prefect-server`로 나가는 경로는 그 배포가 아는 것이고 zeta4s는 모른다. NetworkPolicy는
Pod를 selector로 고르는 순간 그 방향을 화이트리스트로 바꾸므로, zeta4s가 scheduler에
egress 정책을 씌우면 열어 준 두 대상 밖이 전부 닫힌다.

namespace 전체에 default deny를 두는 운영 정책이라면 그 정책과 함께 engine이 필요로 하는
egress를 열고, 거기에 `zeta4s-api:8088`을 포함한다. zeta4s가 요구하는 것은 그 한 줄뿐이다.

```yaml
# 운영자가 자기 default deny 와 함께 두는 정책의 zeta4s 관련 부분
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: zeta4s-api
      ports:
        - protocol: TCP
          port: 8088
```

scheduler Pod는 master keyring과 외부 자격증명 Secret을 받지 않는다. manifest 검사가
이를 강제하며, patch 파일도 같은 검사를 받는다.

## master key 회전

keyring 파일은 운영자 소유 입력이다. API는 읽기만 한다. Kubernetes Secret volume은
Pod에서 쓸 수 없으므로 이 분리가 배포 형태와 일치한다.

```bash
# 1. 현재 keyring 을 꺼내 새 세대를 추가한다
kubectl -n zeta4s get secret zeta4s-secret-master-keyring -o jsonpath='{.data.master\.json}' | base64 -d > master.json
#    (편집: keys 에 새 세대를 넣고 active_key_id 를 그것으로 바꾼다)

# 2. Secret 을 교체하고 Pod 를 재기동한다
kubectl -n zeta4s create secret generic zeta4s-secret-master-keyring \
  --from-file=master.json=master.json --dry-run=client -o yaml | kubectl apply -f -
kubectl -n zeta4s rollout restart deployment/zeta4s-api

# 3. 저장된 ciphertext 를 새 세대로 옮긴다
z4s api secret keyring rotate --api <alias>

# 4. remaining_by_key_id 가 비었으면 keyring 에서 옛 세대를 지우고 2 를 반복한다
```

4단계 전에 옛 세대를 지우면 그 세대로 암호화된 secret을 영영 복호화할 수 없다. `rotate`는
남은 세대가 있으면 0이 아닌 코드로 끝난다.
