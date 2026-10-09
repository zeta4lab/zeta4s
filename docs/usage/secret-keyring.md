# Master keyring 운영

`zeta4s-api`는 profile connection의 `password_ref`를 encrypted secret store에서 resolve한다.
그 저장소를 여는 키가 master keyring이다.

keyring 파일은 **운영자 소유 입력**이다. API는 읽기만 하고 절대 쓰지 않는다. 설계 근거는
[../design/secret-boundary.md](../design/secret-boundary.md)에 있다.

## 상태 확인

```bash
z4s api secret keyring status --api <alias>
```

`valid`가 거짓이면 API가 secret을 열지 못한다. 흔한 원인은 권한(world 접근 허용)과 소유자다.

## 새로 만들기

compose stack은 `z4s-state-init`이 없으면 만든다. 직접 만들려면:

```bash
python -c 'from pathlib import Path; from zeta4s.runtime.secrets import init_master_key_file; init_master_key_file(Path("master.json"))'
```

Kubernetes에서는 이 파일을 Secret으로 넣는다.

```bash
kubectl -n zeta4s create secret generic zeta4s-secret-master-keyring --from-file=master.json=master.json
shred -u master.json
```

## 회전

세 단계다. 순서를 지키지 않으면 secret을 잃는다.

### 1. 새 세대를 추가한다

현재 keyring을 꺼내 `keys`에 새 세대를 넣고 `active_key_id`를 그것으로 바꾼다. 옛 세대는
남겨둔다 — 아직 그 세대로 암호화된 ciphertext가 있다.

```json
{
  "schema_version": 1,
  "active_key_id": "<새 세대>",
  "keys": [
    {"key_id": "<옛 세대>", "key": "..."},
    {"key_id": "<새 세대>", "key": "..."}
  ]
}
```

새 키 값은 다음으로 만든다.

```bash
python -c 'from zeta4s.runtime.secrets import generate_master_key, generate_key_id; print(generate_key_id(), generate_master_key())'
```

파일을 배포한다. compose는 파일 교체, Kubernetes는 Secret 갱신 후 Pod 재기동이다.

### 2. 저장된 ciphertext를 옮긴다

```bash
z4s api secret keyring rotate --api <alias>
```

`remaining_by_key_id`가 비면 모든 active secret이 새 세대다. 중단되어도 안전하니 남은 것이
있으면 다시 실행한다.

`without_generation`에 secret 이름이 있으면 그 row는 `key_id`가 없어 재암호화할 수 없다.
이 변경 이전에 만든 데이터이며 조회도 거절된다. 해당 secret을 다시 설정한 뒤 회전을 마쳐야
옛 세대를 안전하게 지울 수 있다.

### 3. 옛 세대를 제거한다

`remaining_by_key_id`와 `without_generation`이 모두 빈 것을 확인한 뒤 keyring에서 옛 세대를
빼고 배포한다.

**확인 전에 지우면 그 세대로 암호화된 secret을 영영 복호화할 수 없다.** `rotate`는 남은
세대가 있으면 0이 아닌 코드로 끝나므로 스크립트에서 그대로 판정에 쓸 수 있다.

## 기존 스택 주의

`key_id` 없는 secret row는 거절된다. 이 변경 이전에 만든 개발 스택은 모든 row가 그 상태이므로
secret을 다시 설정하거나 volume을 초기화해야 한다.

```bash
docker compose --env-file .env down -v --remove-orphans
```

## 제약

secret 쓰기와 회전은 PostgreSQL metastore가 정본이다. ClickHouse metastore를 쓰는 server는
직렬화 구간을 제공하지 못해 fail-closed한다.
