package com.tistory.kmmoon.stockchartkafkastreams.service

import com.tistory.kmmoon.stockchartkafkastreams.model.CandleStick
import com.tistory.kmmoon.stockchartkafkastreams.model.CandleTimeFrame
import com.tistory.kmmoon.stockchartkafkastreams.model.MultiTimeFrameCandleStick
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@Service
class CandleStickService(
    private val redisTemplate: RedisTemplate<String, Any>
) {
    // Redis에 저장할 때 사용할 키 접두사
    private val CANDLE_KEY_PREFIX = "candle:"

    /**
     * 특정 심볼의 특정 시간대 캔들스틱 데이터를 가져옵니다.
     * @param symbol 주식 심볼
     * @param timeFrame 캔들스틱 시간대 (분봉, 시봉, 일봉, 월봉)
     * @param from 시작 시간
     * @param to 종료 시간
     * @return 캔들스틱 목록
     */
    fun getMultiTimeFrameCandleSticks(
        symbol: String, 
        timeFrame: CandleTimeFrame,
        from: Instant, 
        to: Instant
    ): List<MultiTimeFrameCandleStick> {
        // 모든 키를 불러와서 필터링하는 대신, 날짜 범위 내의 키만 먼저 생성해서 효율적으로 처리
        val fromDateTime = ZonedDateTime.ofInstant(from, ZoneId.systemDefault())
        val toDateTime = ZonedDateTime.ofInstant(to, ZoneId.systemDefault())
        
        // 시간대에 따라 검색 범위를 계산
        val keys = when (timeFrame) {
            CandleTimeFrame.MINUTE -> {
                // 분 단위로 순회하면서 키 생성
                generateDateTimeSequence(fromDateTime, toDateTime, ChronoUnit.MINUTES)
                    .map { dt -> 
                        "$CANDLE_KEY_PREFIX$symbol:$timeFrame:${formatDateTime(dt, timeFrame)}" 
                    }
                    .toSet()
            }
            CandleTimeFrame.HOUR -> {
                // 시간 단위로 순회하면서 키 생성
                generateDateTimeSequence(fromDateTime, toDateTime, ChronoUnit.HOURS)
                    .map { dt -> 
                        "$CANDLE_KEY_PREFIX$symbol:$timeFrame:${formatDateTime(dt, timeFrame)}" 
                    }
                    .toSet()
            }
            CandleTimeFrame.DAY -> {
                // 일 단위로 순회하면서 키 생성
                generateDateTimeSequence(fromDateTime, toDateTime, ChronoUnit.DAYS)
                    .map { dt -> 
                        "$CANDLE_KEY_PREFIX$symbol:$timeFrame:${formatDateTime(dt, timeFrame)}" 
                    }
                    .toSet()
            }
            CandleTimeFrame.MONTH -> {
                // 월 단위로 순회하면서 키 생성
                generateDateTimeSequence(fromDateTime, toDateTime, ChronoUnit.MONTHS)
                    .map { dt -> 
                        "$CANDLE_KEY_PREFIX$symbol:$timeFrame:${formatDateTime(dt, timeFrame)}" 
                    }
                    .toSet()
            }
        }
        
        // 생성된 키를 통해 Redis에서 데이터 조회
        return keys
            .mapNotNull { key ->
                val value = redisTemplate.opsForValue().get(key)
                value as? MultiTimeFrameCandleStick
            }
            .sortedBy { it.startTime }
    }
    
    /**
     * 시작 시간부터 종료 시간까지의 시퀀스를 생성합니다.
     */
    private fun generateDateTimeSequence(
        from: ZonedDateTime, 
        to: ZonedDateTime,
        step: ChronoUnit
    ): Sequence<ZonedDateTime> = sequence {
        var current = from
        while (!current.isAfter(to)) {
            yield(current)
            current = current.plus(1, step)
        }
    }
    
    /**
     * 시간대에 맞게 시간을 형식화합니다.
     */
    private fun formatDateTime(dateTime: ZonedDateTime, timeFrame: CandleTimeFrame): String {
        return when (timeFrame) {
            CandleTimeFrame.MINUTE -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"))
            CandleTimeFrame.HOUR -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
            CandleTimeFrame.DAY -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
            CandleTimeFrame.MONTH -> dateTime.format(DateTimeFormatter.ofPattern("yyyyMM"))
        }
    }

    /**
     * 기존 CandleStick 타입의 데이터를 가져옵니다. (하위 호환성 유지)
     * @param symbol 주식 심볼
     * @param from 시작 시간
     * @param to 종료 시간
     * @return 캔들스틱 목록
     */
    fun getCandleSticks(symbol: String, from: Instant, to: Instant): List<CandleStick> {
        // 새 MultiTimeFrameCandleStick 형식으로 변환하여 반환
        return getMultiTimeFrameCandleSticks(symbol, CandleTimeFrame.MINUTE, from, to)
            .map { multiCandle ->
                CandleStick(
                    symbol = multiCandle.symbol,
                    open = multiCandle.open,
                    high = multiCandle.high,
                    low = multiCandle.low,
                    close = multiCandle.close,
                    volume = multiCandle.volume,
                    startTime = multiCandle.startTime,
                    endTime = multiCandle.endTime
                )
            }
    }

    /**
     * 가장 최근 분봉 데이터를 가져옵니다.
     * @param symbol 주식 심볼
     * @param minutes 가져올 분봉 개수
     * @return 캔들스틱 목록
     */
    fun getRecentCandles(symbol: String, minutes: Int): List<CandleStick> {
        val to = Instant.now()
        val from = to.minus(minutes.toLong(), ChronoUnit.MINUTES)

        return getCandleSticks(symbol, from, to)
    }

    /**
     * 가장 최근 특정 시간대 캔들스틱 데이터를 가져옵니다.
     * @param symbol 주식 심볼
     * @param timeFrame 캔들스틱 시간대
     * @param count 가져올 캔들 개수
     * @return 캔들스틱 목록
     */
    fun getRecentMultiTimeFrameCandles(
        symbol: String, 
        timeFrame: CandleTimeFrame, 
        count: Int
    ): List<MultiTimeFrameCandleStick> {
        val to = Instant.now()
        val from = when (timeFrame) {
            CandleTimeFrame.MINUTE -> to.minus(count.toLong(), ChronoUnit.MINUTES)
            CandleTimeFrame.HOUR -> to.minus(count.toLong(), ChronoUnit.HOURS)
            CandleTimeFrame.DAY -> to.minus(count.toLong(), ChronoUnit.DAYS)
            CandleTimeFrame.MONTH -> to.minus(count.toLong(), ChronoUnit.MONTHS)
        }

        return getMultiTimeFrameCandleSticks(symbol, timeFrame, from, to)
    }

    /**
     * Redis에서 모든 캔들스틱 키를 가져옵니다.
     * @return 모든 캔들스틱 키 목록
     */
    fun getAllCandleKeys(): Set<String> {
        return redisTemplate.keys("$CANDLE_KEY_PREFIX*") ?: emptySet()
    }

    /**
     * 특정 심볼의 모든 시간대 캔들스틱 키를 가져옵니다.
     * @param symbol 주식 심볼
     * @return 해당 심볼의 모든 캔들스틱 키 목록
     */
    fun getSymbolCandleKeys(symbol: String): Set<String> {
        return redisTemplate.keys("$CANDLE_KEY_PREFIX$symbol:*") ?: emptySet()
    }

    /**
     * 특정 심볼과 시간대의 모든 캔들스틱 키를 가져옵니다.
     * @param symbol 주식 심볼
     * @param timeFrame 캔들스틱 시간대
     * @return 해당 심볼과 시간대의 모든 캔들스틱 키 목록
     */
    fun getSymbolTimeFrameCandleKeys(symbol: String, timeFrame: CandleTimeFrame): Set<String> {
        return redisTemplate.keys("$CANDLE_KEY_PREFIX$symbol:$timeFrame:*") ?: emptySet()
    }
    
    /**
     * 특정 날짜의 캔들스틱을 검색합니다.
     * @param symbol 주식 심볼
     * @param timeFrame 시간대
     * @param year 연도
     * @param month 월
     * @param day 일 (일봉, 시봉, 분봉일 경우)
     * @param hour 시간 (시봉, 분봉일 경우)
     * @param minute 분 (분봉일 경우)
     * @return 해당 시간의 캔들스틱, 없으면 null
     */
    fun getCandleByDate(
        symbol: String,
        timeFrame: CandleTimeFrame,
        year: Int,
        month: Int,
        day: Int? = null,
        hour: Int? = null,
        minute: Int? = null
    ): MultiTimeFrameCandleStick? {
        val key = buildKeyByDate(symbol, timeFrame, year, month, day, hour, minute)
        val value = redisTemplate.opsForValue().get(key)
        return value as? MultiTimeFrameCandleStick
    }

    /**
     * 날짜 정보로 Redis 키를 생성합니다.
     */
    private fun buildKeyByDate(
        symbol: String,
        timeFrame: CandleTimeFrame,
        year: Int,
        month: Int,
        day: Int? = null,
        hour: Int? = null,
        minute: Int? = null
    ): String {
        fun Int.format(digits: Int): String = this.toString().padStart(digits, '0')

        val dateFormat = when (timeFrame) {
            CandleTimeFrame.MINUTE -> { "${year.format(4)}${month.format(2)}${day?.format(2)}${hour?.format(2)}${minute?.format(2)}" }
            CandleTimeFrame.HOUR -> { "${year.format(4)}${month.format(2)}${day?.format(2)}${hour?.format(2)}" }
            CandleTimeFrame.DAY -> { "${year.format(4)}${month.format(2)}${day?.format(2)}" }
            CandleTimeFrame.MONTH -> { "${year.format(4)}${month.format(2)}" }
        }

        return "$CANDLE_KEY_PREFIX$symbol:$timeFrame:$dateFormat"
    }
}
