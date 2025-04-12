package com.tistory.kmmoon.stockchartkafkastreams.simulator

import com.tistory.kmmoon.stockchartkafkastreams.model.StockTransaction
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.max

@Component
@EnableScheduling
class StockTransactionGenerator(
    private val kafkaTemplate: KafkaTemplate<String, StockTransaction>,
    @Value("\${app.topics.stock-transactions}") private val topic: String
) {
    private val logger = LoggerFactory.getLogger(StockTransactionGenerator::class.java)

    // 주식 심볼 목록
    private val symbols = listOf("AAPL", "GOOGL", "MSFT", "AMZN", "TSLA")

    // 각 심볼의 기본 가격 (시뮬레이션 시작점)
    private val basePrice = mapOf(
        "AAPL" to 150.0,
        "GOOGL" to 2800.0,
        "MSFT" to 300.0,
        "AMZN" to 3400.0,
        "TSLA" to 900.0
    )

    // 현재 가격을 추적하기 위한 맵
    private val currentPrices = basePrice.toMutableMap()

    @Scheduled(fixedRate = 1000) // 1초마다 실행
    fun generateTransactions() {
        // 모든 심볼에 대해 거래 데이터 생성
        symbols.forEach { symbol ->
            generateTransactionForSymbol(symbol)
        }
    }

    private fun generateTransactionForSymbol(symbol: String) {
        // 현재 가격 가져오기
        val currentPrice = currentPrices[symbol] ?: basePrice[symbol] ?: 100.0

        // 가격 변동 시뮬레이션 (최대 1% 변동)
        val priceChange = currentPrice * (ThreadLocalRandom.current().nextDouble(-0.01, 0.01))
        val newPrice = max(currentPrice + priceChange, 0.01) // 가격이 0보다 작아지지 않도록

        // 거래량 생성 (1~100 사이)
        val quantity = ThreadLocalRandom.current().nextInt(1, 101)

        // 새 가격 저장
        currentPrices[symbol] = newPrice

        // 거래 데이터 생성
        val transaction = StockTransaction(
            symbol = symbol,
            price = newPrice,
            quantity = quantity,
            timestamp = Instant.now()
        )

        // 키 인코딩 문제 해결 - 명시적으로 UTF-8 인코딩 사용
        val encodedKey = symbol.toByteArray(StandardCharsets.UTF_8).toString(StandardCharsets.UTF_8)

        // Kafka로 전송
        kafkaTemplate.send(topic, encodedKey, transaction)
            .whenComplete { result, ex ->
                if (ex == null) {
                    logger.info("Generated transaction for $symbol at ${transaction.price}: ${transaction.quantity} units")
                } else {
                    logger.error("Failed to send transaction to Kafka for $symbol", ex)
                }
            }
    }
}