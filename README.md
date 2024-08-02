# Ddareungi-Spark CI/CD Repository

Ddareungi-Spark 리포지토리에 오신 것을 환영합니다! 이 리포지토리는 실시간 따릉이 대시보드 구축 프로젝트에서 Spark Streaming 코드를 CI/CD하기 위해 관리합니다.

## 프로젝트 개요
이 프로젝트의 주요 목표는 다음과 같습니다:

- **실시간 데이터 수집**: Kafka에서 데이터를 수집하고 Redis에 캐싱하여 빠르게 접근할 수 있도록 합니다.
- **데이터 변환 및 저장**: Spark Streaming을 사용하여 실시간으로 데이터를 처리하고, 변환된 데이터를 Amazon S3에 저장합니다.
- **자동 배포**: GitHub Actions와 AWS CodeDeploy를 활용하여 Spark Streaming과 Redis 캐싱 코드를 EC2 서버에 자동으로 배포합니다.

## 아키텍처
![Github Action & CodeDeploy CI/CD](https://1drv.ms/i/c/9ded56be8cf81c92/IQNJRKls7yGMSJpXC5w6SHexAcdcMmCo1u9rPOT7osYAtOQ?width=1024)

### 상세 구성 요소
1. **Kafka**: Ddareungi API에서 실시간 데이터를 수신하는 데이터 수집 레이어 역할을 합니다.
2. **Spark Streaming**: 실시간으로 Kafka에서 수신한 데이터를 처리합니다.
3. **Redis**: 빠르게 접근하고 효율적으로 쿼리할 수 있도록 처리된 데이터를 캐싱합니다.
4. **Amazon S3**: 장기 저장 및 추가 분석을 위해 변환된 데이터를 저장합니다.
5. **GitHub Actions**: CI/CD 파이프라인을 관리하여 코드 변경 사항이 효율적으로 테스트되고 배포되도록 합니다.
6. **AWS CodeDeploy**: EC2 서버에 일관되고 원활한 업데이트를 보장하기 위해 배포 프로세스를 자동화합니다.

## 리포지토리 구조
이 리포지토리는 Spark Streaming과 Redis 캐싱 코드를 개발, 테스트 및 배포하기 쉽게 구성되어 있습니다:

- **src/**: Spark Streaming 및 Redis 캐싱 소스 코드가 포함되어 있습니다.
- **tests/**: 코드의 정확성을 보장하기 위한 단위 및 통합 테스트가 포함되어 있습니다.
- **scripts/**: AWS CodeDeploy를 위한 배포 스크립트 및 설정 파일이 포함되어 있습니다.
- **.github/workflows/**: CI/CD 파이프라인 자동화를 위한 GitHub Actions 워크플로우가 포함되어 있습니다.
## Member
<table>
  <tr>
    <td align="center">
    <a href="https://github.com/StatisticsFox">
      <img src="https://avatars.githubusercontent.com/u/92065443?v=4" width="100px;" alt=""/>
      <br />
      <sub>
        <b>최지혁</b>
      </sub>
    </a>
    <br />
    </td>
    <td align="center">
    <a href="https://github.com/Hamseungjin">
      <img src="https://avatars.githubusercontent.com/u/109064686?v=4" width="100px;" alt=""/>
      <br />
      <sub>
        <b>함승진</b>
      </sub>
      </a>