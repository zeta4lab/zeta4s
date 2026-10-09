# Secret Boundary

zeta4s가 다루는 비밀은 두 갈래다. 성격이 다르므로 관리 주체도 다르다.

## 1. zeta4s 암호 체계의 비밀

`zeta4s-api`가 소유하고 코드가 의미를 아는 것이다.

| 비밀 | 역할 |
|---|---|
| master keyring | encrypted secret store의 복호화 키 세대 모음 |
| runtime internal token | scheduler runtime이 internal execution endpoint를 호출할 때 쓰는 인증 |

runtime database password는 여기 포함되지 않는다. profile은 평문 대신 `password_ref`를 갖고,
실행 시 `zeta4s-api`가 encrypted secret store에서 resolve한다. 복호화는 API process 안에서만
일어난다. scheduler는 credential을 받지 않는다.

`src/zeta4s/prefect/runtime.py`는 이름이 오해를 부른다. Prefect worker가 실행하는 코드가
아니라 **`zeta4s-api` 안에서 도는 코드**다. worker의 flow entrypoint는
`prefect_engine`이며 step 실행을 internal API로 위임한다. secret을 푸는 쪽은 언제나
API process다. 이 구분을 놓치면 worker에게 master keyring이 필요하다고 잘못 판단하게 된다.

## 2. 외부 서비스 자격증명

`zeta4s-api`가 소비하지만 zeta4s가 의미를 정의하지 않는 배포 입력이다. metastore 접속 정보,
Airflow REST 자격증명, public API token, iceberg catalog token이 여기 속한다. 배포 형태가
이들을 안전하게 전달할 책임을 진다.

---

## master keyring

### 세대 구조

keyring은 JSON이며 세대마다 `key_id`와 키를 갖는다. 활성 세대는 정확히 하나이고, 나머지는
회전이 끝나기 전 ciphertext를 읽기 위해 남는다.

secret ciphertext row는 자신을 암호화한 `key_id`를 기록한다. `key_id` 없는 row는 계약 위반으로
거절한다 — 어떤 키로 만들었는지 모르는 데이터를 추측해서 읽지 않는다.

AAD는 `{secret_key}:{version}:{key_id}`다. 세대 정보를 AAD에 넣어 ciphertext와 세대의 결합을
명시한다.

### 소유권 — API는 읽기만 한다

keyring 파일의 생성, 세대 추가, 옛 세대 제거는 **운영자 절차**다. `zeta4s-api`는 이 파일에
쓰지 않는다.

이 분리는 배포 형태가 강제한다. Kubernetes Secret volume은 Pod에서 쓸 수 없으므로, API가
파일을 갱신하는 설계는 그 형태에서 동작할 수 없다. 이 경계는 배포 형태에 맞춘 우회가 아니라
배포 형태가 드러내는 올바른 경계다.

### 회전

1. 운영자가 keyring에 새 활성 세대를 추가해 배포한다
2. API가 옛 세대의 active ciphertext를 새 세대로 in-place 재암호화한다
3. 남은 세대가 없음을 확인한 뒤 운영자가 옛 세대를 제거한다

재암호화는 version을 유지한다. version은 "값 변경" 이력이므로 키 변경이 그것을 소비하지 않는다.
각 row는 compare-and-set으로 갱신하므로, 재암호화 도중 사용자가 새 값을 쓰면 CAS가 실패하고
그 secret은 건너뛴다 — 새 version은 이미 활성 세대로 암호화되어 있다.

3단계 전에 옛 세대를 지우면 그 세대로 암호화된 secret을 영영 복호화할 수 없다.

### 파일 검사

검사는 symlink를 따라간 **최종 대상** 기준이다. 배포 형태마다 파일 모양이 다르기 때문이다.

| 형태 | 소유자 | mode | 구조 |
|---|---|---|---|
| compose | 실행 UID | 0400 | 일반 파일 |
| Kubernetes | root | 0440 | `master.json -> ..data/master.json` symlink |

두 형태를 함께 만족하도록 **world 접근만 막고**, 소유자는 실행 UID 또는 root를 허용한다.
group을 막으면 Pod가 자기 Secret을 못 읽고, symlink를 거절하면 Kubernetes에서 keyring을
읽을 수 없다.

## 쓰기 직렬화

`set_secret`은 현재 version을 읽어 `max + 1`을 쓴다. 동시 호출이 같은 값을 계산하면 서로를
덮으므로 secret 쓰기 경로를 advisory lock으로 직렬화한다.

ClickHouse metastore는 이 직렬화 구간을 제공하지 못하므로 secret 쓰기와 회전을 fail-closed
한다.

## 코드 위치

- secret primitives와 keyring: `src/zeta4s/runtime/secrets.py`
- repository 계약: `src/zeta4s/metastore/contracts.py`
- password_ref 해석: `src/zeta4s/api/app.py`의 runtime connection resolve
- 배포 경계: `docker-compose.yml`, `deploy/k3s/`
