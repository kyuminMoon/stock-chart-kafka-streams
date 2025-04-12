package com.tistory.kmmoon.stockchartkafkastreams.model

import java.time.Instant

/**
 * 캔들스틱의 시간 프레임을 정의합니다.
 */
enum class CandleTimeFrame {
    MINUTE,  // 1분봉
    HOUR,    // 1시간봉
    DAY,     // 일봉
    MONTH    // 월봉
}

/**
 * 여러 시간 프레임을 포함하는 캔들스틱 데이터 모델
 */
data class MultiTimeFrameCandleStick(
    val symbol: String,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volume: Int,
    val startTime: Instant,
    val endTime: Instant,
    val timeFrame: CandleTimeFrame
) {
    override fun toString(): String {
        return "OHLC(${timeFrame.name}, $symbol, $open, $high, $low, $close, Vol:$volume, $startTime ~ $endTime)"
    }
}
