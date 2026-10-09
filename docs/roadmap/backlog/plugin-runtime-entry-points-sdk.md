# Plugin Runtime — entry_points 와 plugin SDK

상태: backlog

전제: entry-point discovery(1.0.15) 완료 + open-core 경계 확정.

## 목표

외부 Python package 가 step type 을 배포하는 완전한 플러그인 경로를 연다. `entry_points`
discovery 자체는 1.0.15 에서 착지했다(`zeta4s.step_types` group, 설치=등록). 남은 것은 외부
저자 표면과 안전장치이며, `docs/design/builtin-step-adapter-abstraction.md` 의 "이후 확장" 절이
지목한다. zeta4s 가 AI Agent 생성 계약을 실행하는 범용 Runtime Engine 으로서 빌트인과 외부
step type 을 같은 Step Graph 계약으로 다루는 최종 상태다.

- `zeta4s-plugin-sdk`: 외부 저자가 `StepTypeDescriptor` 를 구현하는 최소 표면
- adapter package compatibility range (descriptor 계약 버전)
- plugin validation CLI (`z4s plugin check` 류)
- runtime image 에 plugin dependency 를 포함하는 배포 방식
  (Kubernetes 운영 런타임과 연동)

## open-core 경계 결정 (착수 전 확정)

이 단계는 사업 판단이 선행돼야 한다. 플러그인 진입점은 **open-core 경계를 긋는 지점**이다 —
상업 adapter(고급 provider 기능 등)를 플러그인으로 분리할지, plugin SDK 자체를 어느
라이선스로 낼지가 여기서 정해진다. 현재 성숙도(엔진 코어는 완성, 프로덕션 운영 자산 미달)에서는
이르다. 프로덕션 운영 자산이 갖춰지고 경계가 확정된 뒤에 착수한다.

## 구현 기준

- discovery 는 검증·실행 이전의 명시 등록 호출로 트리거한다 (import-time side effect 없음
  원칙 유지). 이 부분은 1.0.15 에서 착지했다.
- `zeta4s-plugin-sdk` 는 외부 저자가 scheduler 를 몰라도 descriptor 를 구현할 수 있는
  최소 표면만 노출한다.
- compatibility range 밖 plugin 은 로드 자체를 거부한다.

## 완료 기준 (기계 확인 가능)

- plugin package 를 별도 wheel 로 설치했을 때 `z4s plugin check` 가 descriptor 계약
  준수를 검증한다.
- 미준수 plugin 이 로드 자체를 실패시키고 실행 경로로 새지 않는다.
- compatibility range 밖 plugin 을 명확한 계약 위반으로 거부한다.
- runtime image 배포 흐름에서 plugin dependency 포함이 재현 가능하다.

## 문서 현행화

- 별도 마일스톤 문서와 `zeta4s-plugin-sdk` 배포 계약 신설
- open-core 경계 결정을 사업 전략 문서에 반영
