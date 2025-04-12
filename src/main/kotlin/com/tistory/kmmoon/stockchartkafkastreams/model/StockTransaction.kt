package com.tistory.kmmoon.stockchartkafkastreams.model

import java.time.Instant

data class StockTransaction(
    val symbol: String,
    val price: Double,
    val quantity: Int,
    val timestamp: Instant = Instant.now()
)
