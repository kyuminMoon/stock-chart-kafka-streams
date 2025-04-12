package com.tistory.kmmoon.stockchartkafkastreams.stream

import com.tistory.kmmoon.stockchartkafkastreams.model.CandleStick
import com.tistory.kmmoon.stockchartkafkastreams.model.CandleTimeFrame
import com.tistory.kmmoon.stockchartkafkastreams.model.MultiTimeFrameCandleStick
import com.tistory.kmmoon.stockchartkafkastreams.model.StockTransaction
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters

/**
 * CandlestickAggregator 컴포넌트
 * 
 * 이 클래스는 Kafka Streams를 사용하여 주식 거래 데이터를 수신하고 
 * 여러 시간 프레임(분봉, 시봉, 일봉, 월봉)의 캔들스틱 차트 데이터로 집계
 * 집계된 데이터는 Kafka 토픽과 Redis에 저장됩니다.
 */
@Component
class CandlestickAggregator(
    private val redisTemplate: RedisTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(CandlestickAggregator::class.java)

    // Redis에 저장할 때 사용할 키 패턴
    private val CANDLE_KEY_PREFIX = "candle:"
    
    // 설정 파일에서 주식 거래 데이터가 들어오는 토픽 이름을 가져옵니다.
    @Value("\${app.topics.stock-transactions}")
    private lateinit var transactionsTopic: String

    // 설정 파일에서 캔들스틱 데이터를 발행할 토픽 이름을 가져옵니다.
    @Value("\${app.topics.stock-candles}")
    private lateinit var candlesTopic: String

    // 주식 거래 데이터 직렬화/역직렬화를 위한 Serde
    @Autowired
    private lateinit var transactionSerde: JsonSerde<StockTransaction>

    // 캔들스틱 데이터 직렬화/역직렬화를 위한 Serde
    @Autowired
    private lateinit var candleStickSerde: JsonSerde<CandleStick>

    // 다중 시간대 캔들스틱 데이터 직렬화/역직렬화를 위한 Serde
    @Autowired
    private lateinit var multiTimeFrameCandleStickSerde: JsonSerde<MultiTimeFrameCandleStick>

    /**
     * Kafka Streams 파이프라인을 구축
     * 
     * 이 메서드는 스프링이 자동으로 호출하며, 주식 거래 데이터를 다양한 시간대의 캔들스틱으로 
     * 변환하는 스트림 처리 파이프라인을 정의
     * 
     * @param streamsBuilder Kafka Streams 파이프라인을 구축하기 위한 빌더
     */
    @Autowired
    fun buildPipeline(streamsBuilder: StreamsBuilder) {
        // 로깅 추가
        logger.info("캔들스틱 집계를 위한 Kafka Streams 파이프라인 구축 시작")
        
        // 트랜잭션 스트림 생성
        // - stream(): 지정된 토픽에서 데이터를 읽어 KStream 객체를 생성
        // - Consumed.with(): 키와 값의 직렬화/역직렬화 방식을 지정
        val transactionStream = streamsBuilder
            .stream(
                transactionsTopic,
                Consumed.with(Serdes.String(), transactionSerde)
            )
            .peek { key, value -> logger.debug("거래 데이터 수신: $key, $value") }
            // peek()는 스트림의 요소를 확인하고 로깅하는 중간 연산으로, 디버깅에 유용

        // 1분봉 생성 및 처리
        // 윈도우 처리: 1분 윈도우 동안 데이터를 모아서 윈도우가 닫힐 때 집계 수행
        // 참고: 실제 데이터는 윈도우가 닫힐 때까지 상태 저장소에 누적됨
        val minuteCandleStream = transactionStream
            .groupByKey() // 키(심볼)별로 그룹화
            // TimeWindows: 시간 기반 윈도우를 정의 여기서는 1분 간격으로 윈도우를 생성
            // ofSizeWithNoGrace: 지정된 크기의 윈도우를 생성하고, 윈도우가 닫힌 후에는 늦게 도착한 데이터를 처리하지 않음.
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            // aggregate(): 윈도우 내의 데이터를 집계
            .aggregate(
                { initializeCandle() }, // 초기 빈 캔들 생성
                // {초기 값 생성 함수, 각 요소를 집계에 추가하는 방법, 집계 결과의 직렬화/역직렬화 방식}
                { key, transaction, candle -> updateCandle(key, transaction, candle) }, // 거래 데이터로 캔들 업데이트
                Materialized.with(Serdes.String(), candleStickSerde) // 결과 저장 방식 지정
            )
            .toStream() // 집계 결과를 다시 스트림으로 변환
            .map { key, candle -> 
                logger.info("1분 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
                
                // 1분봉을 MultiTimeFrameCandleStick으로 변환
                val multiTimeFrameCandle = convertToMultiTimeFrame(candle, key.key(), CandleTimeFrame.MINUTE)
                // Redis에 저장 - 윈도우가 닫힐 때 수행됨
                saveToRedis(multiTimeFrameCandle)
                KeyValue.pair(key.key(), multiTimeFrameCandle) 
                // map()은 스트림의 각 요소를 변환 여기서는 윈도우 키에서 원래 키(심볼)를 추출하고,
                // 값을 MultiTimeFrameCandleStick 형식으로 변환
            }
            .peek { key, candle -> logger.info("생성된 1분 캔들 for $key: $candle") }
            // 디버깅을 위한 중간 로깅

        // 시봉(1시간) 생성 및 처리
        // 윈도우 처리: 1시간 윈도우 동안 데이터(분봉)를 모아서 윈도우가 닫힐 때 집계 수행
        minuteCandleStream
            .groupByKey() // 심볼별로 다시 그룹화
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))) // 1시간 윈도우 정의
            .aggregate(
                { initializeMultiTimeFrameCandle(CandleTimeFrame.HOUR) }, // 초기 시봉 캔들 생성
                { key, minuteCandle, hourCandle -> aggregateCandles(key, minuteCandle, hourCandle) }, // 분봉을 시봉으로 집계
                Materialized.with(Serdes.String(), multiTimeFrameCandleStickSerde) // 결과 저장 방식 지정
            )
            .toStream() // 집계 결과를 스트림으로 변환
            .map { key, candle -> 
                logger.info("1시간 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
                
                // 시간 정보 조정
                val adjustedCandle = adjustTimeFrame(candle, CandleTimeFrame.HOUR)
                
                // 시봉을 Redis에 저장 - 윈도우가 닫힐 때 수행됨
                saveToRedis(adjustedCandle)
                KeyValue.pair(key.key(), adjustedCandle) 
            }
            .peek { key, candle -> logger.info("생성된 시간 캔들 for $key: $candle") }

        // 일봉 생성 및 처리
        // 윈도우 처리: 1일 윈도우 동안 데이터(분봉)를 모아서 윈도우가 닫힐 때 집계 수행
        minuteCandleStream
            .groupByKey() // 심볼별로 그룹화
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(1))) // 1일 윈도우 정의
            .aggregate(
                { initializeMultiTimeFrameCandle(CandleTimeFrame.DAY) }, // 초기 일봉 캔들 생성
                { key, minuteCandle, dayCandle -> aggregateCandles(key, minuteCandle, dayCandle) }, // 분봉을 일봉으로 집계
                Materialized.with(Serdes.String(), multiTimeFrameCandleStickSerde) // 결과 저장 방식 지정
            )
            .toStream() // 집계 결과를 스트림으로 변환
            .map { key, candle -> 
                logger.info("1일 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
                
                // 시간 정보 조정
                val adjustedCandle = adjustTimeFrame(candle, CandleTimeFrame.DAY)
                
                // 일봉을 Redis에 저장 - 윈도우가 닫힐 때 수행됨
                saveToRedis(adjustedCandle)
                KeyValue.pair(key.key(), adjustedCandle) 
            }
            .peek { key, candle -> logger.info("생성된 일 캔들 for $key: $candle") }

        // 월봉 생성 및 처리
        // 윈도우 처리: 30일 윈도우 동안 데이터(분봉)를 모아서 윈도우가 닫힐 때 집계 수행
        minuteCandleStream
            .groupByKey() // 심볼별로 그룹화
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(30))) // 대략적인 월 길이의 윈도우 정의
            .aggregate(
                { initializeMultiTimeFrameCandle(CandleTimeFrame.MONTH) }, // 초기 월봉 캔들 생성
                { key, minuteCandle, monthCandle -> aggregateCandles(key, minuteCandle, monthCandle) }, // 분봉을 월봉으로 집계
                Materialized.with(Serdes.String(), multiTimeFrameCandleStickSerde) // 결과 저장 방식 지정
            )
            .toStream() // 집계 결과를 스트림으로 변환
            .map { key, candle -> 
                logger.info("1개월 캔들 윈도우 닫힘: ${key.key()}, 윈도우: ${key.window().startTime()} ~ ${key.window().endTime()}")
                
                // 시간 정보 조정
                val adjustedCandle = adjustTimeFrame(candle, CandleTimeFrame.MONTH)
                
                // 월봉을 Redis에 저장 - 윈도우가 닫힐 때 수행됨
                saveToRedis(adjustedCandle)
                KeyValue.pair(key.key(), adjustedCandle) 
            }
            .peek { key, candle -> logger.info("생성된 월 캔들 for $key: $candle") }

        // 최종 캔들 토픽으로 출력 (모든 시간대 캔들이 여기로 전달됨)
        // to(): 스트림의 결과를 지정된 토픽으로 전송
        // Produced.with(): 토픽으로 데이터를 전송할 때 사용할 직렬화/역직렬화 방식을 지정
        minuteCandleStream.to(
            candlesTopic,
            Produced.with(Serdes.String(), multiTimeFrameCandleStickSerde)
        )
        
        logger.info("캔들스틱 집계를 위한 Kafka Streams 파이프라인이 성공적으로 구축되었습니다.")
    }

    /**
     * 시간대에 맞게 시작/종료 시간을 조정
     * 
     * @param candle 조정할 캔들스틱 데이터
     * @param timeFrame 적용할 시간 프레임 (분, 시, 일, 월)
     * @return 시간이 조정된 캔들스틱 데이터
     */
    private fun adjustTimeFrame(candle: MultiTimeFrameCandleStick, timeFrame: CandleTimeFrame): MultiTimeFrameCandleStick {
        // 초기 시간값이 1970년으로 들어가는 이슈가 있어 비교용으로 생성
        val isInitialState = candle.open == 0.0 ||
                candle.volume == 0 ||
                candle.startTime == Instant.EPOCH ||
                candle.startTime == candle.endTime

        // 현재 시간 또는 기존 시간 사용
        val baseTime = if (isInitialState) {
            logger.info("$timeFrame 캔들에 대한 초기 상태가 감지됨, 현재 시간 사용")
            ZonedDateTime.now(ZoneId.systemDefault())
        } else {
            ZonedDateTime.ofInstant(candle.startTime, ZoneId.systemDefault())
        }


        val (adjustedStart, adjustedEnd) = when (timeFrame) {
            CandleTimeFrame.MINUTE -> {
                // 분봉: 현재 분의 시작과 끝
                Pair(
                    baseTime.truncatedTo(ChronoUnit.MINUTES),
                    baseTime.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1).minusNanos(1)
                )
            }
            CandleTimeFrame.HOUR -> {
                // 시봉: 현재 시간의 시작과 끝
                Pair(
                    baseTime.truncatedTo(ChronoUnit.HOURS),
                    baseTime.truncatedTo(ChronoUnit.HOURS).plusHours(1).minusNanos(1)
                )
            }
            CandleTimeFrame.DAY -> {
                // 일봉: 현재 일의 시작과 끝
                Pair(
                    baseTime.truncatedTo(ChronoUnit.DAYS),
                    baseTime.truncatedTo(ChronoUnit.DAYS).plusDays(1).minusNanos(1)
                )
            }
            CandleTimeFrame.MONTH -> {
                // 월봉: 현재 월의 시작과 끝
                val startOfMonth = baseTime.withDayOfMonth(1)
                    .withHour(0).withMinute(0).withSecond(0).withNano(0)
                
                val endOfMonth = startOfMonth.with(TemporalAdjusters.lastDayOfMonth())
                    .withHour(23).withMinute(59).withSecond(59).withNano(999999999)
                
                Pair(startOfMonth, endOfMonth)
            }
        }
        
        logger.info("${candle.symbol} ${timeFrame} 시간대 조정: ${adjustedStart} ~ ${adjustedEnd}")
        
        return candle.copy(
            startTime = adjustedStart.toInstant(),
            endTime = adjustedEnd.toInstant(),
            timeFrame = timeFrame
        )
    }

    /**
     * 캔들스틱을 Redis에 저장
     * 키 형식: candle:{symbol}:{timeFrame}:{YYYYMMDDHHMM}
     * 
     * @param candle 저장할 캔들스틱 데이터
     */
    private fun saveToRedis(candle: MultiTimeFrameCandleStick) {
        val dateFormat = formatTimestampByTimeFrame(candle.startTime, candle.timeFrame)
        val key = "$CANDLE_KEY_PREFIX${candle.symbol}:${candle.timeFrame}:$dateFormat"
        redisTemplate.opsForValue().set(key, candle)
        logger.info("Redis에 ${candle.timeFrame} 캔들 저장: $key")
    }
    
    /**
     * 시간대에 맞게 시간을 YYYYMMDDHHMM 형식으로 포맷팅
     * 
     * @param timestamp 포맷팅할 시간
     * @param timeFrame 시간 프레임 (분, 시, 일, 월)
     * @return 포맷팅된 시간 문자열
     */
    private fun formatTimestampByTimeFrame(timestamp: Instant, timeFrame: CandleTimeFrame): String {
        val dateTime = ZonedDateTime.ofInstant(timestamp, ZoneId.systemDefault())

        return when (timeFrame) {
            CandleTimeFrame.MINUTE -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"))
            CandleTimeFrame.HOUR -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
            CandleTimeFrame.DAY -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
            CandleTimeFrame.MONTH -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMM"))
        }
    }

    /**
     * 기본 캔들스틱을 특정 시간대의 MultiTimeFrameCandleStick으로 변환
     * 
     * @param candle 변환할 기본 캔들스틱
     * @param symbol 주식 심볼
     * @param timeFrame 적용할 시간 프레임
     * @return 변환된 다중 시간대 캔들스틱
     */
    private fun convertToMultiTimeFrame(
        candle: CandleStick, 
        symbol: String, 
        timeFrame: CandleTimeFrame
    ): MultiTimeFrameCandleStick {
        return MultiTimeFrameCandleStick(
            symbol = symbol,
            open = candle.open,
            high = candle.high,
            low = candle.low,
            close = candle.close,
            volume = candle.volume,
            startTime = candle.startTime,
            endTime = candle.endTime,
            timeFrame = timeFrame
        )
    }

    /**
     * 소형 시간대 캔들(예: 분봉)을 대형 시간대 캔들(예: 시봉)로 집계
     * 
     * @param symbol 주식 심볼
     * @param smallerTimeFrameCandle 작은 시간대의 캔들 (예: 분봉)
     * @param largerTimeFrameCandle 큰 시간대의 캔들 (예: 시봉)
     * @return 업데이트된 큰 시간대의 캔들
     */
    private fun aggregateCandles(
        symbol: String, 
        smallerTimeFrameCandle: MultiTimeFrameCandleStick, 
        largerTimeFrameCandle: MultiTimeFrameCandleStick
    ): MultiTimeFrameCandleStick {
        // 첫 데이터인 경우 초기화
        if (largerTimeFrameCandle.open == 0.0) {
            // 시간대에 맞는 시작/종료 시간 설정
            val adjustedTimeFrame = adjustTimeFrame(
                MultiTimeFrameCandleStick(
                    symbol = symbol,
                    open = smallerTimeFrameCandle.open,
                    high = smallerTimeFrameCandle.high, 
                    low = smallerTimeFrameCandle.low,
                    close = smallerTimeFrameCandle.close,
                    volume = smallerTimeFrameCandle.volume,
                    startTime = smallerTimeFrameCandle.startTime,
                    endTime = smallerTimeFrameCandle.endTime,
                    timeFrame = largerTimeFrameCandle.timeFrame
                ),
                largerTimeFrameCandle.timeFrame
            )
            
            return adjustedTimeFrame
        }

        // 기존 캔들 업데이트
        return largerTimeFrameCandle.copy(
            high = maxOf(largerTimeFrameCandle.high, smallerTimeFrameCandle.high),
            low = minOf(largerTimeFrameCandle.low, smallerTimeFrameCandle.low),
            close = smallerTimeFrameCandle.close, // 항상 마지막 가격이 종가
            volume = largerTimeFrameCandle.volume + smallerTimeFrameCandle.volume
        )
    }

    /**
     * 다중 시간대 캔들스틱을 초기화
     * 
     * @param timeFrame 초기화할 캔들의 시간 프레임
     * @return 초기화된 다중 시간대 캔들스틱
     */
    private fun initializeMultiTimeFrameCandle(timeFrame: CandleTimeFrame): MultiTimeFrameCandleStick {
        // 현재 시간을 기준으로 초기화하되, 실제 값은 첫 데이터가 들어올 때 설정됨
        val now = Instant.now()
        return MultiTimeFrameCandleStick(
            symbol = "",
            open = 0.0,
            high = Double.MIN_VALUE,
            low = Double.MAX_VALUE,
            close = 0.0,
            volume = 0,
            startTime = now,
            endTime = now,
            timeFrame = timeFrame
        )
    }

    /**
     * 기본 캔들스틱을 초기화
     * 
     * @return 초기화된 기본 캔들스틱
     */
    private fun initializeCandle(): CandleStick {
        // 현재 시간을 기준으로 초기화하되, 실제 값은 첫 데이터가 들어올 때 설정됨
        val now = Instant.now()
        return CandleStick(
            symbol = "",
            open = 0.0,
            high = Double.MIN_VALUE,
            low = Double.MAX_VALUE,
            close = 0.0,
            volume = 0,
            startTime = now,
            endTime = now
        )
    }

    /**
     * 주식 거래 데이터로 캔들스틱을 업데이트
     * 
     * @param symbol 주식 심볼
     * @param transaction 주식 거래 데이터
     * @param candle 업데이트할 캔들스틱
     * @return 업데이트된 캔들스틱
     */
    private fun updateCandle(symbol: String, transaction: StockTransaction, candle: CandleStick): CandleStick {
        val price = transaction.price
        val quantity = transaction.quantity
        val timestamp = transaction.timestamp

        // 첫 거래인 경우
        if (candle.open == 0.0) {
            // 시간을 분 단위로 정확하게 설정
            val startTime = timestamp.truncatedTo(ChronoUnit.MINUTES)
            val endTime = startTime.plus(1, ChronoUnit.MINUTES)
            
            logger.info("심볼 $symbol 에 대한 새 캔들 초기화 (시간: $startTime)")
            
            return CandleStick(
                symbol = symbol,
                open = price,
                high = price,
                low = price,
                close = price,
                volume = quantity,
                startTime = startTime,
                endTime = endTime
            )
        }

        // 기존 캔들 업데이트
        return CandleStick(
            symbol = symbol,
            open = candle.open,
            high = maxOf(candle.high, price),
            low = minOf(candle.low, price),
            close = price, // 항상 마지막 가격이 종가
            volume = candle.volume + quantity,
            startTime = candle.startTime,
            endTime = candle.endTime
        )
    }
}