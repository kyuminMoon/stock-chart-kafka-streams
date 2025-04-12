# Kafka Streams 메소드 체이닝 상세 설명

## 트랜잭션 스트림 생성 부분

```kotlin
val transactionStream = streamsBuilder
    .stream(transactionsTopic, Consumed.with(Serdes.String(), transactionSerde))
    .peek { key, value -> logger.debug("거래 데이터 수신: $key, $value") }
```

### stream() 메소드
- **목적**: Kafka 토픽에서 데이터를 읽어 KStream 객체로 생성
- **작동 방식**:
    - `transactionsTopic`: 데이터를 읽을 Kafka 토픽 이름
    - `Consumed.with()`: 키와 값의 직렬화/역직렬화 방식 지정
    - 여기서는 키가 String 타입, 값이 StockTransaction 타입임을 명시
- **중요성**: 모든 스트림 처리의 시작점으로, 데이터 소스를 정의함

### peek() 메소드
- **목적**: 데이터 흐름 디버깅
- **작동 방식**: 스트림의 각 레코드를 조회하고 제공된 함수 실행 (스트림 내용은 변경하지 않음)
- **사용 이유**: 로깅을 통해 데이터 흐름을 추적하고 디버깅하기 위함
- **참고**: 실제 프로덕션에서는 성능 영향이 있으므로 필요한 경우에만 사용

## 1분봉 캔들스틱 생성 파이프라인

```kotlin
val minuteCandleStream = transactionStream
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .aggregate(
        { initializeCandle() },
        { key, transaction, candle -> updateCandle(key, transaction, candle) },
        Materialized.with(Serdes.String(), candleStickSerde)
    )
    .toStream()
    .map { key, candle -> ... }
    .peek { key, candle -> logger.info("생성된 1분 캔들 for $key: $candle") }
```

### groupByKey() 메소드
- **목적**: 레코드의 키(주식 심볼)를 기준으로 데이터 그룹화
- **반환 타입**: KGroupedStream
- **작동 방식**:
    - 동일한 키를 가진 레코드를 논리적으로 그룹화 (실제 데이터 수집은 아님)
    - 동일한 키의 레코드는 동일한 파티션/태스크로 라우팅됨
- **사용 이유**: 같은 주식 심볼의 모든 거래를 함께 처리하기 위함
- **참고**: 키 기반 그룹화는 Kafka의 파티션 메커니즘과 연계됨

### windowedBy() 메소드
- **목적**: 시간 기반 윈도우 적용으로 데이터를 시간 간격별로 처리
- **반환 타입**: TimeWindowedKStream
- **매개변수**:
    - `TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))`:
        - 윈도우 크기: 1분
        - 유예 기간: 없음 (늦게 도착한 데이터 무시)
- **작동 방식**:
    - 1분 간격으로 데이터를 분할하여 각 윈도우별로 독립적으로 처리
    - 각 윈도우는 고유한 시작/종료 시간 정보를 가짐
- **사용 이유**: 1분 단위 캔들스틱 집계를 위해 정확한 시간 간격으로 데이터를 묶기 위함

### aggregate() 메소드
- **목적**: 윈도우 내 데이터를 집계하여 캔들스틱 생성
- **반환 타입**: KTable (윈도우 키와 집계 결과의 맵핑)
- **매개변수**:
    1. **초기값 생성자**:
        - `{ initializeCandle() }`
        - 각 키-윈도우 조합마다 한 번씩 호출됨
        - 빈 캔들스틱 객체 생성
    2. **집계 함수**:
        - `{ key, transaction, candle -> updateCandle(key, transaction, candle) }`
        - 각 거래 데이터가 들어올 때마다 호출됨
        - 기존 캔들스틱에 새 거래 정보를 반영하여 업데이트
    3. **저장 방식**:
        - `Materialized.with(Serdes.String(), candleStickSerde)`
        - 집계 상태를 저장할 때 사용할 직렬화/역직렬화 방식 지정
- **작동 방식**:
    - 윈도우 내 첫 데이터 → 초기값 생성
    - 이후 데이터 → 집계 함수로 상태 업데이트
    - 윈도우 닫힘 → 최종 집계 결과 생성
- **사용 이유**: OHLC(시가,고가,저가,종가) 및 거래량을 계산하여 캔들스틱 생성

### toStream() 메소드
- **목적**: KTable을 KStream으로 변환
- **반환 타입**: KStream
- **작동 방식**:
    - 윈도우 기반 테이블(업데이트 중심)을 다시 이벤트 기반 스트림으로 변환
    - 윈도우가 닫힐 때 최종 집계 결과를 스트림 이벤트로 방출
- **사용 이유**:
    - 다운스트림 처리를 위해 테이블을 스트림으로 변환
    - 윈도우 완료 시점에 결과 캔들스틱을 다음 처리 단계로 전달하기 위함

### map() 메소드
- **목적**: 스트림의 각 레코드를 변환
- **작동 방식**:
    - 람다 함수를 통해 입력 레코드를 새로운 형식으로 변환
    - 여기서는 다음 작업 수행:
        1. 윈도우 종료 로깅
        2. 기본 캔들스틱을 MultiTimeFrameCandleStick으로 변환
        3. Redis에 데이터 저장
        4. 새 KeyValue 쌍 생성 (원래 키와 변환된 값)
- **중요 포인트**:
    - `key.key()`로 윈도우 키에서 원래 주식 심볼 추출
    - 윈도우 메타데이터는 제거하고 원래 키 유지
- **사용 이유**:
    - 집계 결과의 형식 변환
    - 부가 효과(Redis 저장) 수행
    - 다운스트림 프로세싱을 위한 형식 준비

### peek() 메소드 (파이프라인 마지막)
- **목적**: 최종 생성된 캔들스틱 확인 및 로깅
- **작동 방식**: 스트림을 변경하지 않고 각 요소를 관찰
- **사용 이유**: 디버깅 및 모니터링 목적으로 최종 결과 확인

## 핵심 개념 및 데이터 흐름 요약

1. **입력**: Kafka 토픽에서 주식 거래 데이터 수신
2. **그룹화**: 주식 심볼별로 데이터 그룹화
3. **윈도우 적용**: 1분 간격으로 데이터 분할
4. **집계**: 각 윈도우 내 거래 데이터로 OHLC 캔들스틱 계산
5. **변환**: 기본 캔들스틱을 다중 시간대 형식으로 변환
6. **저장**: Redis에 결과 저장 및 다음 처리 단계로 전달
7. **로깅**: 처리 과정 모니터링 및 디버깅

이 파이프라인은 실시간으로 들어오는 거래 데이터를 시간 기반으로 집계하여 차트 데이터로 변환하는 전형적인 Kafka Streams 패턴을 보여줍니다.

## 윈도우 개념 상세 설명

### 윈도우란?
- **정의**: 스트림 데이터를 시간 단위로 분할하여 그룹화하는 메커니즘
- **목적**: 무한한 스트림 데이터를 유한한 시간 조각으로 나누어 집계 처리
- **유형**: 이 코드에서는 Tumbling Window(고정 크기 윈도우, 겹치지 않음) 사용

### 윈도우 동작 방식
- **생성 시점**: 해당 시간대의 첫 데이터가 도착할 때 윈도우 생성
- **활성화 기간**: 윈도우의 시작 시간부터 지정된 기간(여기서는 1분) 동안 활성화
- **닫힘 시점**:
    - 윈도우 시작 시간 + 지정 기간(1분) 경과 시 닫힘
    - 예: 10:00:00에 시작된 윈도우는 10:01:00에 닫힘
- **유예 기간**: `ofSizeWithNoGrace`로 설정되어 있어 유예 기간 없음
    - 윈도우가 닫힌 후 도착한 데이터는 해당 윈도우에 포함되지 않음

### 내부 처리 메커니즘
- **상태 저장**: 각 윈도우의 진행 중인 집계 상태는 Kafka Streams의 상태 저장소에 유지됨
- **키-윈도우 조합**: 각 키(주식 심볼)와 윈도우 시간 조합마다 별도의 상태 유지
- **메모리 관리**: 활성 윈도우만 메모리에 유지되고, 닫힌 윈도우는 다운스트림으로 전달 후 정리됨

```kotlin
.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
```

## 레디스 저장 시점 상세 설명

### 저장 트리거 시점
- **발생 시점**: 윈도우가 닫힐 때 (1분 간격 종료 시)
- **코드 위치**: `.map()` 연산 내부에서 `saveToRedis(multiTimeFrameCandle)` 호출
- **실행 시퀀스**:
    1. 윈도우 기간(1분) 동안 데이터 집계
    2. 윈도우 종료 시 `toStream()` 호출로 KTable 결과가 스트림 이벤트로 변환
    3. `map()` 연산 내에서 저장 로직 실행

```kotlin
.map { key, candle -> 
    logger.info("1분 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
    
    // 1분봉을 MultiTimeFrameCandleStick으로 변환
    val multiTimeFrameCandle = convertToMultiTimeFrame(candle, key.key(), CandleTimeFrame.MINUTE)
    // Redis에 저장 - 윈도우가 닫힐 때 수행됨
    saveToRedis(multiTimeFrameCandle)  // <-- 이 부분에서 Redis 저장
    KeyValue.pair(key.key(), multiTimeFrameCandle) 
}
```

### 레디스 저장 프로세스
- **키 생성**: `candle:{symbol}:{timeFrame}:{YYYYMMDDHHMM}` 형식으로 키 구성
- **값 저장**: 완성된 MultiTimeFrameCandleStick 객체 저장
- **저장 방식**: RedisTemplate의 opsForValue().set() 메서드 사용
- **시간 정보**: 각 캔들의 시작 시간을 사용하여 레디스 키의 타임스탬프 생성

```kotlin
private fun saveToRedis(candle: MultiTimeFrameCandleStick) {
    val dateFormat = formatTimestampByTimeFrame(candle.startTime, candle.timeFrame)
    val key = "$CANDLE_KEY_PREFIX${candle.symbol}:${candle.timeFrame}:$dateFormat"
    redisTemplate.opsForValue().set(key, candle)
    logger.info("Redis에 ${candle.timeFrame} 캔들 저장: $key")
}
```

## 윈도우와 저장 프로세스 통합 흐름

### 1분 캔들스틱 생성 및 저장 전체 흐름
1. **데이터 수신**: Kafka 토픽에서 주식 거래 데이터 수신
2. **윈도우 할당**:
    - 각 거래 데이터는 타임스탬프에 따라 해당하는 1분 윈도우에 할당
    - 예: 10:00:30 거래는 10:00:00~10:01:00 윈도우에 할당
3. **진행 중 집계**:
    - 윈도우가 활성화된 동안 계속해서 데이터 집계
    - 첫 데이터: `initializeCandle()` 호출로 초기 캔들 생성
    - 이후 데이터: `updateCandle()` 호출로 OHLC 값 업데이트
4. **윈도우 종료 처리**:
    - 1분 경과 후 윈도우가 닫힘 (예: 10:01:00)
    - 최종 집계 결과가 스트림으로 방출됨
    - `map()` 연산에서 캔들 형식 변환 후 레디스에 저장
5. **다음 처리 단계**:
    - 저장 완료 후 캔들스틱을 다음 처리 단계(시봉, 일봉 등)로 전달

### 저장 시점의 중요성
- **실시간성**: 윈도우가 닫히는 즉시 레디스에 저장하여 최신 데이터 유지
- **완전성**: 윈도우가 닫힌 후에만 저장하므로 완전한 집계 결과 보장
- **일관성**: 레디스 키에 시간 정보가 포함되어 일관된 데이터 접근 가능
- **효율성**: 중간 업데이트 없이 최종 결과만 저장하여 레디스 부하 최소화

### 윈도우와 캔들스틱의 관계
- **개념적 일치**: 금융 캔들스틱은 본질적으로 시간 윈도우 집계와 같은 개념
- **시간 정밀도**: 윈도우 시작/종료 시간이 그대로 캔들스틱의 시작/종료 시간이 됨
- **캔들 형성**: 윈도우 내 첫 거래는 시가(open), 마지막 거래는 종가(close), 최고/최저가 및 거래량은 누적 집계

## 실제 예시 시나리오

10:00:00~10:01:00 시간대의 AAPL(애플) 주식 거래 처리:

1. 10:00:05 - AAPL 첫 거래 도착
    - 새 윈도우 생성
    - `initializeCandle()` 호출
    - 시가(open) 설정

2. 10:00:05~10:00:59 - 추가 거래 도착
    - `updateCandle()` 호출로 고가/저가 업데이트
    - 거래량 누적
    - 가장 최근 가격이 종가(close)로 계속 업데이트

3. 10:01:00 - 윈도우 종료
    - 최종 1분 캔들 완성
    - MultiTimeFrameCandleStick으로 변환
    - 레디스에 키 `candle:AAPL:MINUTE:202504121000` 로 저장
    - 다음 시간대 윈도우(10:01:00~10:02:00) 준비

이러한 과정을 통해 Kafka Streams의 윈도우 메커니즘은 연속적인 거래 데이터 스트림을 정확한 시간 간격의 캔들스틱으로 변환하며, 완성된 캔들은 즉시 레디스에 저장되어 외부 시스템에서 활용할 수 있게 됩니다.