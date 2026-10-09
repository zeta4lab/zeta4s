#!/usr/bin/env bash
set -euo pipefail

# manifest 는 배포 계약이다. 경계가 깨지면 배포된 뒤에야 드러나므로 파일 단계에서 막는다.
# cluster 가 필요한 검증은 여기서 하지 않는다 — 이 검사는 PR CI 에서 Docker 없이 돈다.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_DIR="${ROOT}/deploy/k3s"

fail=0
violation() {
  echo "k3s manifest violation: $1" >&2
  fail=1
}

if [ ! -d "$MANIFEST_DIR" ]; then
  echo "k3s manifest directory is missing: $MANIFEST_DIR" >&2
  exit 1
fi

python3 - "$MANIFEST_DIR" <<'PY' || fail=1
import sys
from pathlib import Path

import yaml

manifest_dir = Path(sys.argv[1])
docs = []
# patch 파일도 keyring 을 붙일 수 있다. 하위 디렉터리까지 본다.
for path in sorted([*manifest_dir.rglob("*.yaml"), *manifest_dir.rglob("*.yml")]):
    for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
        if doc:
            docs.append((path.name, doc))

violations = []


def kind(doc, name):
    return doc.get("kind") == name


# 1. master keyring 은 zeta4s-api 만 mount 한다.
#
# Deployment 만 보면 StatefulSet/DaemonSet/Job/CronJob/Pod 로 우회된다.
# Airflow 는 보통 StatefulSet 이다.
WORKLOAD_KINDS = ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job", "CronJob", "Pod")


def pod_specs(doc):
    """workload 종류와 무관하게 (이름, podSpec) 을 돌려준다.

    patch 파일은 kind 가 없다. 그래도 spec.template.spec 을 담고 있으면 podSpec 을
    바꾸는 문서이므로 같은 경계를 적용한다 — patch 로도 keyring 을 붙일 수 있다.
    """
    name = (doc.get("metadata") or {}).get("name", "?")
    spec = doc.get("spec") or {}
    kind_name = doc.get("kind")

    if kind_name is None:
        template = (spec.get("template") or {}).get("spec")
        return [(name, template)] if template else []

    if kind_name not in WORKLOAD_KINDS:
        return []
    if kind_name == "Pod":
        return [(name, spec)]
    if kind_name == "CronJob":
        spec = ((spec.get("jobTemplate") or {}).get("spec")) or {}
    template = (spec.get("template") or {}).get("spec")
    return [(name, template)] if template else []


def all_containers(spec):
    """initContainers 와 ephemeralContainers 도 keyring 을 집을 수 있다."""
    return [
        *(spec.get("containers") or []),
        *(spec.get("initContainers") or []),
        *(spec.get("ephemeralContainers") or []),
    ]


def secret_names_in_volume(volume):
    """secret 뿐 아니라 projected 로도 같은 Secret 을 붙일 수 있다."""
    names = []
    secret = volume.get("secret") or {}
    if secret.get("secretName"):
        names.append(secret["secretName"])
    for item in ((volume.get("projected") or {}).get("sources") or []):
        projected = item.get("secret") or {}
        if projected.get("name"):
            names.append(projected["name"])
    return names


keyring_secret = "zeta4s-secret-master-keyring"
for source, doc in docs:
    for app, spec in pod_specs(doc):
        if spec is None:
            continue
        mounts_keyring = any(keyring_secret in secret_names_in_volume(v) for v in spec.get("volumes", []))
        if mounts_keyring and app != "zeta4s-api":
            violations.append(f"{source}: {app} mounts the master keyring secret")
        for container in all_containers(spec):
            for item in container.get("envFrom", []):
                if (item.get("secretRef") or {}).get("name") == keyring_secret:
                    violations.append(f"{source}: {app}/{container.get('name')} loads the keyring through envFrom")
            for env in container.get("env", []):
                ref = ((env.get("valueFrom") or {}).get("secretKeyRef") or {}).get("name")
                if ref == keyring_secret:
                    violations.append(f"{source}: {app} exposes the master keyring through env {env.get('name')}")

for source, doc in docs:
    if not kind(doc, "Deployment"):
        continue
    app = doc["metadata"]["name"]
    spec = doc["spec"]["template"]["spec"]
    mounts_keyring = any(
        (volume.get("secret") or {}).get("secretName") == keyring_secret for volume in spec.get("volumes", [])
    )
    if mounts_keyring and app != "zeta4s-api":
        violations.append(f"{source}: {app} mounts the master keyring secret")
    if app == "zeta4s-api" and not mounts_keyring:
        violations.append(f"{source}: zeta4s-api does not mount the master keyring secret")

    # 2. keyring 은 read-only 0400 이어야 한다.
    for volume in spec.get("volumes", []):
        secret = volume.get("secret") or {}
        if secret.get("secretName") != keyring_secret:
            continue
        if secret.get("defaultMode") != 0o400:
            violations.append(f"{source}: master keyring volume is not mode 0400")
        for container in spec.get("containers", []):
            for mount in container.get("volumeMounts", []):
                if mount.get("name") == volume["name"] and not mount.get("readOnly"):
                    violations.append(f"{source}: master keyring mount is not readOnly")

    # 3. master key 를 환경변수로 싣지 않는다. env 는 describe pod 와 crash dump 에 남는다.
    for container in spec.get("containers", []):
        for env in container.get("env", []):
            ref = ((env.get("valueFrom") or {}).get("secretKeyRef") or {}).get("name")
            if ref == keyring_secret:
                violations.append(f"{source}: master keyring is exposed through env {env.get('name')}")

    # 4. Pod Security
    if app == "zeta4s-api":
        pod_security = spec.get("securityContext") or {}
        if not pod_security.get("runAsNonRoot"):
            violations.append(f"{source}: zeta4s-api does not set runAsNonRoot")
        for container in spec.get("containers", []):
            container_security = container.get("securityContext") or {}
            if container_security.get("allowPrivilegeEscalation") is not False:
                violations.append(f"{source}: {container['name']} allows privilege escalation")
            if container_security.get("readOnlyRootFilesystem") is not True:
                violations.append(f"{source}: {container['name']} does not set readOnlyRootFilesystem")

        # 5. ZETA4S_API_TOKEN 이 비면 public API 가 무인증이 된다. 평문 기본값을 두지 않는다.
        token_env = [
            env
            for container in spec.get("containers", [])
            for env in container.get("env", [])
            if env.get("name") == "ZETA4S_API_TOKEN"
        ]
        for env in token_env:
            if env.get("value") is not None:
                violations.append(f"{source}: ZETA4S_API_TOKEN must come from a secret, not a literal value")
        env_from = [
            (item.get("secretRef") or {}).get("name")
            for container in spec.get("containers", [])
            for item in container.get("envFrom", [])
        ]
        if not token_env and "zeta4s-external-credentials" not in env_from:
            violations.append(f"{source}: ZETA4S_API_TOKEN is not wired from a secret")

        # 6. scheduler 실행 rowset 과 bootstrap 은 Iceberg catalog 를 요구한다. 접속 정보가
        #    없으면 배포는 뜨지만 bootstrap 과 첫 rowset step 에서야 실패한다.
        api_env = {
            env.get("name"): env for container in spec.get("containers", []) for env in container.get("env", [])
        }
        for required in ("ZETA4S_ICEBERG_CATALOG_URI", "ZETA4S_ICEBERG_WAREHOUSE"):
            env = api_env.get(required)
            if env is None:
                violations.append(f"{source}: zeta4s-api does not receive {required}")
            elif not str(env.get("value") or "").strip() and not env.get("valueFrom"):
                violations.append(f"{source}: zeta4s-api sets {required} to an empty value")

        # catalog token 은 자격증명이다. env literal 로 싣지 않는다.
        catalog_token = api_env.get("ZETA4S_ICEBERG_CATALOG_TOKEN")
        if catalog_token is not None:
            if catalog_token.get("value") is not None:
                violations.append(
                    f"{source}: ZETA4S_ICEBERG_CATALOG_TOKEN must come from a secret, not a literal value"
                )
            elif not (catalog_token.get("valueFrom") or {}).get("secretKeyRef"):
                violations.append(f"{source}: ZETA4S_ICEBERG_CATALOG_TOKEN is not wired from a secret")
        elif "zeta4s-external-credentials" not in env_from:
            violations.append(f"{source}: ZETA4S_ICEBERG_CATALOG_TOKEN is not wired from a secret")

# 7. internal execution endpoint 를 cluster 밖으로 내보내지 않는다.
for source, doc in docs:
    # k3s 는 Traefik 을 기본 탑재하므로 IngressRoute 가 그 환경의 관용적 노출 수단이다.
    if doc.get("kind") in ("Ingress", "IngressRoute", "HTTPRoute", "TCPRoute"):
        violations.append(f"{source}: {doc['kind']} must not expose the internal execution endpoint")
    if kind(doc, "Service") and doc["spec"].get("type") not in (None, "ClusterIP"):
        violations.append(f"{source}: service {doc['metadata']['name']} is not ClusterIP")

# 8. Role 은 keyring Secret 하나의 get 만 갖는다. list 는 다른 Secret 이름을 드러낸다.
for source, doc in docs:
    if doc.get("kind") not in ("Role", "ClusterRole"):
        continue
    for rule in doc.get("rules", []):
        if "secrets" not in rule.get("resources", []):
            continue
        verbs = set(rule.get("verbs", []))
        if not verbs <= {"get"}:
            violations.append(f"{source}: secret role grants more than get: {sorted(verbs)}")
        if not rule.get("resourceNames"):
            violations.append(f"{source}: secret role does not pin resourceNames")

# 9. zeta4s 소유 Pod 에 default deny 가 걸려야 한다.
ZETA4S_OWNER_LABEL = ("app.kubernetes.io/part-of", "zeta4s")
policies = [(source, doc) for source, doc in docs if kind(doc, "NetworkPolicy")]


def selector_labels(doc):
    return ((doc["spec"].get("podSelector") or {}).get("matchLabels")) or {}


def selects_only_zeta4s(doc):
    labels = selector_labels(doc)
    if not labels:
        return False
    if labels.get(ZETA4S_OWNER_LABEL[0]) == ZETA4S_OWNER_LABEL[1]:
        return True
    return str(labels.get("app.kubernetes.io/name", "")).startswith("zeta4s")


if not any(
    selector_labels(doc).get(ZETA4S_OWNER_LABEL[0]) == ZETA4S_OWNER_LABEL[1]
    and set(doc["spec"].get("policyTypes") or []) == {"Ingress", "Egress"}
    for _, doc in policies
):
    violations.append("no default deny NetworkPolicy covering zeta4s-owned pods")

# 10. zeta4s 는 자기가 배포하지 않는 Pod 를 selector 로 고르지 않는다.
#
# NetworkPolicy 는 Pod 를 고르는 순간 그 방향을 화이트리스트로 바꾼다. scheduler 를
# 고르면 zeta4s 가 열어 준 대상 밖이 전부 닫히고, Airflow 는 자기 metastore 에,
# Prefect worker 는 prefect-server 에 닿지 못한 채 멈춘다. 그 egress 가 무엇이어야
# 하는지는 engine 배포가 알고 zeta4s 는 모른다.
for source, doc in policies:
    if not selects_only_zeta4s(doc):
        name = doc["metadata"]["name"]
        violations.append(f"{source}: NetworkPolicy {name} selects pods zeta4s does not deploy: {selector_labels(doc) or '{}'}")

for item in violations:
    print(f"k3s manifest violation: {item}", file=sys.stderr)

sys.exit(1 if violations else 0)
PY

if [ "$fail" -ne 0 ]; then
  echo "k3s manifest contract violated" >&2
  exit 1
fi

echo "k3s manifest contract holds: keyring is api-only, api is non-root read-only, no ingress, policies cover only zeta4s pods, api receives iceberg catalog wiring"
