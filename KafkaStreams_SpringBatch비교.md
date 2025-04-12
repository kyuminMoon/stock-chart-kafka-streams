# Kafka Streams와 Spring Batch 비교: 주요 개념 및 용어 정리

## 1. 핵심 개념 비교표

| Kafka Streams | Spring Batch | 개념 매핑 |
|--------------|--------------|------------|
| 스트림(Stream) | ItemReader | Kafka: 지속적인 실시간 데이터 흐름<br>Spring Batch: 한정된 배치 단위 데이터 처리 |
| 토폴로지(Topology) | Job 구성 | Kafka: 프로세서 노드 연결 그래프<br>Spring Batch: Step 연결 흐름도 |
| 프로세서(Processor) | ItemProcessor | Kafka: 스트림 내 데이터 변환 노드<br>Spring Batch: 아이템 단위 처리 로직 |
| 윈도우(Window) | Chunk 처리 | Kafka: 시간 기반 데이터 그룹화<br>Spring Batch: 아이템 개수 기반 그룹화 |
| 상태 저장소(State Store) | JobRepository | Kafka: 처리 상태 및 중간 결과 저장<br>Spring Batch: 작업 진행 상태 및 메타데이터 저장 |
| 집계(Aggregation) | Step 처리 로직 | Kafka: 스트림 데이터의 지속적 집계<br>Spring Batch: 청크 단위 집계 및 결과 계산 |

## 2. Kafka Streams 주요 개념 상세 설명

### 2.1. 스트림(Stream)
- **정의**: 지속적으로 유입되는 무한한 데이터 레코드의 흐름
- **구조**: 키-값(key-value) 쌍으로 구성된 레코드 시퀀스
- **Spring Batch와 차이점**:
    - Kafka Streams: 데이터가 계속 유입되는 열린 구조
    - Spring Batch: 시작과 끝이 명확한 폐쇄적 데이터 처리

### 2.2. 토폴로지(Topology)

- **정의**: 스트림 처리 로직을 표현하는 계산 그래프
- **구성요소**:
    - **소스 노드(Source)**: 데이터를 읽어오는 시작점 (Spring Batch의 ItemReader와 유사)
    - **프로세서 노드(Processor)**: 데이터 변환/처리 (Spring Batch의 ItemProcessor와 유사)
    - **싱크 노드(Sink)**: 처리 결과를 저장하는 종착점 (Spring Batch의 ItemWriter와 유사)
- **시각적 표현**:
```
소스(Kafka 토픽) → 프로세서(데이터 변환) → 프로세서(집계) → 싱크(다른 Kafka 토픽/외부 저장소)
```

### 2.3. 윈도우(Window) 상세 설명

- **정의**: 시간을 기준으로 데이터를 그룹화하는 메커니즘
- **Spring Batch 유사 개념**:
    - Spring Batch의 Chunk 처리 (일정 개수의 아이템 모음)와 유사하나,
    - 청크는 '데이터 개수' 기반, 윈도우는 '시간' 기반으로 근본적 차이가 있음

#### 윈도우 유형 상세 비교

| 윈도우 유형 | 특징 | 구체적 활용 사례 | 코드 예시 |
|------------|------|-----------------|----------|
| **텀블링 윈도우**<br>(고정 간격) | • 겹치지 않음<br>• 모든 레코드는 정확히 하나의 윈도우에 속함 | • 금융 차트의 분봉/시봉/일봉<br>• 시간별 판매 집계<br>• IoT 센서 주기적 요약 | `TimeWindows.ofSizeWithNoGrace(`<br>`  Duration.ofMinutes(1))` |
| **홉핑 윈도우**<br>(중첩 간격) | • 윈도우 간 중첩됨<br>• 레코드가 여러 윈도우에 속할 수 있음 | • 이동 평균 계산<br>• 트렌드 분석<br>• 점진적 집계 필요 시 | `TimeWindows.ofSizeAndAdvance(`<br>`  Duration.ofMinutes(10),`<br>`  Duration.ofMinutes(5))` |
| **슬라이딩 윈도우**<br>(이벤트 기반) | • 이벤트 수 기준 윈도우<br>• 가장 최근 N개 이벤트 유지 | • 최근 N개 거래 기반 분석<br>• 실시간 이상 감지<br>• 패턴 인식 | `KStream.windowedBy(`<br>`  SlidingWindows.withTimeDifferenceAndGrace(`<br>`  Duration.ofSeconds(5), Duration.ZERO))` |
| **세션 윈도우**<br>(활동 기반) | • 비활동 기간으로 세션 구분<br>• 유연한 윈도우 크기 | • 사용자 세션 분석<br>• 웹사이트 방문 패턴<br>• 연속 활동 그룹화 | `SessionWindows.with(`<br>`  Duration.ofMinutes(5))` |

### 2.4. 상태 저장소(State Store)와 Spring Batch 메타데이터 저장소 비교

- **Kafka Streams 상태 저장소**:
    - 실시간 처리 중 필요한 중간 상태 저장
    - 집계, 조인, 윈도우 작업에 필수적
    - 장애 발생 시 복구를 위한 기반
    - 내부 구현: RocksDB(기본), 인메모리 등

- **Spring Batch JobRepository**:
    - 작업 실행 메타데이터 관리
    - 작업 상태, 진행 상황 추적
    - 재시작 지점 관리
    - 내부 구현: 관계형 DB(기본), 인메모리 등

- **주요 차이점**:
    - Kafka: 실제 데이터의 중간 상태 저장
    - Spring Batch: 작업 진행 정보 저장

### 2.5. KStream과 KTable 개념 명확화

- **KStream (이벤트 스트림)**:
    - 모든 이벤트가 독립적으로 처리됨
    - 동일 키의 이전 레코드와 무관하게 처리
    - 이벤트 발생 로그와 유사(append-only)
    - **사용 시점**: 모든 개별 트랜잭션이 중요할 때 (예: 주식 거래 기록)

- **KTable (변경 로그 테이블)**:
    - 동일 키에 대해 최신 값만 유지
    - 업데이트 의미론(update semantics)
    - 관계형 DB 테이블과 유사한 개념
    - **사용 시점**: 최신 상태만 중요할 때 (예: 사용자 프로필, 상품 재고)

- **Spring Batch 유사 개념**:
    - KStream → 전체 트랜잭션 로그 처리
    - KTable → 마스터 데이터 업데이트 처리

- **실제 예시**:
```
// KStream - 모든 주식 거래 이벤트 (중복 키 허용)
거래ID1, {AAPL, 150.0, 10주, 10:00:01} 
거래ID2, {AAPL, 151.0, 5주, 10:00:30}
거래ID3, {AAPL, 149.5, 15주, 10:00:45}

// KTable - 주식 최신 가격 (키별 최신값만 유지)
AAPL, 150.0  // 첫 거래 후
AAPL, 151.0  // 두번째 거래 후 (업데이트)
AAPL, 149.5  // 세번째 거래 후 (업데이트)
```

### 2.6. 그레이스 기간(Grace Period) 설명 추가

- **정의**: 윈도우가 종료된 후에도 지연 도착 데이터를 처리하는 추가 시간
- **중요성**:
    - 네트워크 지연, 클럭 스큐 등으로 늦게 도착한 데이터 처리 가능
    - 더 정확한 집계 결과 보장
    - 분산 시스템에서의 데이터 완전성 향상

- **설정 방법**:
  ```kotlin
  // 그레이스 기간 없음 (프로젝트 예시)
  TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
  
  // 10초 그레이스 기간 설정 예시
  TimeWindows.ofSizeAndGrace(
    Duration.ofMinutes(1),  // 윈도우 크기
    Duration.ofSeconds(10)  // 그레이스 기간
  )
  ```

- **Spring Batch 유사 개념**:
    - 명확한 동등 개념 없음
    - ItemReader의 재시도(retry) 정책과 개념적으로 유사

### 2.7. 백프레셔(Backpressure) 개념 상세화

- **정의**: 생산자와 소비자 간 처리 속도 차이를 관리하는 메커니즘
- **중요성**:
    - 메모리 사용량 제어
    - 시스템 안정성 유지
    - 과부하 상황 방지

- **Kafka Streams 구현**:
    - 내부적으로 Kafka Consumer의 Poll 모델 사용
    - 처리 가능한 레코드만 가져와 자연스러운 백프레셔 형성
    - 버퍼 크기 조정(`max.poll.records`, `buffer.memory` 등)

- **Spring Batch 유사 개념**:
    - `throttleLimit` - 병렬 처리 시 스텝 실행 제한
    - 청크 크기 설정

## 3. 프로젝트 윈도우 처리 흐름 상세 설명

이 주식 차트 프로젝트의 윈도우 처리는 Spring Batch의 단계적 처리와 달리, 동시에 여러 시간대의 집계가 실시간으로 이루어집니다.

### 3.1. 윈도우 처리 계층 구조

```
[실시간 거래 데이터 스트림]
       ↓
[1분 텀블링 윈도우] → 분봉 생성 → Redis 저장
       ↓
[1시간 텀블링 윈도우] → 시봉 생성 → Redis 저장
       ↓
[1일 텀블링 윈도우] → 일봉 생성 → Redis 저장
       ↓
[30일 텀블링 윈도우] → 월봉 생성 → Redis 저장
```

### 3.2. 윈도우 처리 메커니즘 상세

1. **윈도우 생성 및 관리**:
    - 각 시간 프레임별 윈도우가 독립적으로 관리됨
    - 모든 윈도우는 동시에 활성화되어 병렬 처리됨
    - Spring Batch의 순차적 Step 실행과 달리 동시 처리

2. **윈도우 상태 처리 흐름**:
   ```
   윈도우 시작 → 데이터 누적(State Store) → 윈도우 종료 → 집계 계산 → 결과 전달 → Redis 저장
   ```

3. **Spring Batch와 비교한 주요 차이점**:

   | 처리 측면 | Kafka Streams | Spring Batch |
      |----------|--------------|--------------|
   | 처리 시점 | 실시간, 지속적 | 배치, 주기적 |
   | 데이터 범위 | 무한 스트림 | 유한 데이터셋 |
   | 그룹화 기준 | 시간 기반 | 레코드 수 기준 |
   | 상태 관리 | 분산 상태 저장소 | 중앙 집중식 JobRepository |
   | 장애 복구 | 자동 복구 | 수동/반자동 재시작 |
   | 처리 모델 | 이벤트 기반, 비동기 | 청크 기반, 동기 |

4. **텀블링 윈도우 선택 이유**:
    - 캔들스틱 차트의 특성과 정확히 일치(겹치지 않는 시간 간격)
    - 각 시간대(분봉, 시봉, 일봉, 월봉)가 명확히 구분되어야 함
    - 중복 없이 정확한 OHLC(시가,고가,저가,종가) 계산 필요

### 3.3. 윈도우 처리와 Redis 저장 시점

- **저장 트리거**: 윈도우가 닫힐 때만 저장 수행
  ```kotlin
  .map { key, candle -> 
      // 1분 캔들 윈도우 닫힘을 로깅
      logger.info("1분 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
      
      // 캔들스틱 변환
      val multiTimeFrameCandle = convertToMultiTimeFrame(candle, key.key(), CandleTimeFrame.MINUTE)
      
      // Redis에 저장 - 윈도우가 닫힐 때 수행됨
      saveToRedis(multiTimeFrameCandle)  // <-- 이 시점에서 Redis 저장
      
      KeyValue.pair(key.key(), multiTimeFrameCandle) 
  }
  ```

- **Spring Batch 유사 비교**:
    - Kafka Streams: 윈도우 종료 시 저장 (이벤트 기반 트리거)
    - Spring Batch: ItemWriter 호출 시 저장 (청크 단위 트리거)

## 4. 실제 애플리케이션 시나리오

### 주식 데이터 처리 예시로 보는 Kafka Streams vs Spring Batch

**시나리오**: AAPL(애플) 주식 거래 데이터 처리

**Kafka Streams 접근법** (프로젝트 구현):
1. 실시간으로 들어오는 모든 거래 데이터를 지속적으로 처리
2. 동시에 여러 시간대(분봉, 시봉, 일봉, 월봉) 캔들 생성
3. 윈도우 종료 시점마다 자동으로 Redis에 저장
4. 장애 발생 시 자동으로 복구 및 재처리

**Spring Batch 접근법** (가상 구현):
1. 정해진 스케줄에 따라 주기적으로 배치 작업 실행
2. 마지막 실행 이후 데이터만 처리
3. 각 시간대별로 별도 스텝 실행 (분봉→시봉→일봉→월봉 순차적)
4. 작업 완료 후 한 번에 결과 저장
5. 장애 발생 시 마지막 성공 지점부터 재시작

**선택 기준**:
- 실시간성 요구: Kafka Streams
- 주기적 대용량 처리: Spring Batch
- 이 프로젝트: 실시간 캔들스틱 생성 필요로 Kafka Streams 선택

---

이 설명을 통해 Kafka Streams의 주요 개념과 Spring Batch와의 비교를 이해하고, 특히 윈도우 처리와 상태 관리가 어떻게 다른지 파악할 수 있습니다. 프로젝트의 주식 차트 생성 사례는 Kafka Streams의 실시간 처리 및 시간 기반 윈도우 기능을 효과적으로 활용하는 좋은 예시입니다.