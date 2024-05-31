# Operato Foundation Service

인티그레이션 플랫폼은 엔터프라이즈 아키텍쳐에서 인티그레이션 레이어를 담당하게 될 플랫폼입니다.

이 플랫폼을 구성하는 모듈들은 다음과 같습니다.

- 인티그레이션 모델러 : 모델러는 보드를 기반으로 EIP(Enterprise Integration Patterns) 개념을 구현될 예정입니다.
- 인티그레이션 엔진 : 시나리오 실행 엔진을 기반으로 구현될 예정입니다.
- 인티그레이션 커넥터 들 : 다양한 커넥터들이 파이선 모듈로 개발될 것입니다. 파이선 모듈과 엔진의 연동 구조는 별도로 준비하고 있습니다.
- 인티그레이션 관리 서비스 : 우리 프레임워크(things-factory) 베이스로 개발됩니다.

위 구성품 외에도 모든 플랫폼 구성에 꼭 필요한 마이크로서비스들이 있는데, 이것을 우리는 파운데이션 서비스라고 부르며, 현재 다음과 같이 구성되어 있습니다.

- [스케줄러 서비스](./scheduler-service/README.md) : 스케쥴링 이벤트 (PUBSUB 모드)
- [지연 스케쥴러 서비스](./pending-queue-service/README.md) : 스케쥴링 이벤트 (POLLING 모드)
- [메시지 허브 서비스](./service-broker/README.md) : 동적 서비스 엔드포인트 관리서비스
- [Locking 서비스](./locking-service/README.md) : 멀티 인스턴스 서비스의 비동기 환경에서의 동기 패턴의 Request-Response 통신 구현을 위한 공유 Waiting 서비스
