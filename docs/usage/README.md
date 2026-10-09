# docs/usage

사용자가 zeta4s 를 쓰는 법이다. 사용자는 사람 또는 AI Agent 다. 파일 목록은 두지 않는다.
목록을 적으면 그 목록이 먼저 낡는다.

사용자는 workspace 안에 작성하고 `z4s` 로 실행한다.

- `profiles/` — 소스/타겟 연결 정의와 실행 환경 값
- `projects/` — zeta4s 로 실행할 job 의 집합. job 안의 step 을 실행하기 위한 입력물과
  project 단위 문서

내용은 YAML 로 설정하고 step 에 따라 SQL 이나 dbt 정의 파일이 추가된다. zeta4s 가 정한
규격대로 작성해야 step graph 를 실행할 수 있다. 여기 문서는 그 규격과 실행 방법을 다룬다.

- 구현이 어떻게 되어 있는지는 `../design/` 에 있다. 사용자는 볼 필요가 없다.
- 검증 gate 는 `../gate/`, 목표와 완성 여부는 `../roadmap/` 에 있다.
- `z4s` 사용법은 `z4s.md` 에 있다. 명령 목록과 option 은 `z4s --help` 가 정본이므로 문서에 복제하지 않는다.
