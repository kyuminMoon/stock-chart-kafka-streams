package com.tistory.kmmoon.stockchartkafkastreams.model

import java.time.Instant

data class CandleStick(
    val symbol: String,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volume: Int,
    val startTime: Instant,
    val endTime: Instant
)
