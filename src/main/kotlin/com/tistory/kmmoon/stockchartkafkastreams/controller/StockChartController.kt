package com.tistory.kmmoon.stockchartkafkastreams.controller

import com.tistory.kmmoon.stockchartkafkastreams.model.CandleStick
import com.tistory.kmmoon.stockchartkafkastreams.service.CandleStickService
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.*
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

@RestController
@RequestMapping("/api/charts")
class StockChartController(
    private val candleStickService: CandleStickService
) {

    @GetMapping("/{symbol}")
    fun getStockChart(
        @PathVariable symbol: String,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) from: LocalDateTime?,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) to: LocalDateTime?,
        @RequestParam(required = false, defaultValue = "60") minutes: Int
    ): List<CandleStick> {
        // from과 to가 제공되면 해당 기간의 데이터를 가져오고, 그렇지 않으면 최근 데이터를 가져옴
        return if (from != null && to != null) {
            val fromInstant = from.toInstant(ZoneOffset.UTC)
            val toInstant = to.toInstant(ZoneOffset.UTC)
            candleStickService.getCandleSticks(symbol, fromInstant, toInstant)
        } else {
            candleStickService.getRecentCandles(symbol, minutes)
        }
    }

    @GetMapping("/symbols")
    fun getAvailableSymbols(): List<String> {
        // 실제 구현에서는 동적으로 가져오는 것이 좋겠지만, 지금은 하드코딩된 목록 반환
        return listOf("AAPL", "GOOGL", "MSFT", "AMZN", "TSLA")
    }

    @GetMapping("/recent/{symbol}")
    fun getRecentCandles(
        @PathVariable symbol: String,
        @RequestParam(required = false, defaultValue = "60") minutes: Int
    ): List<CandleStick> {
        return candleStickService.getRecentCandles(symbol, minutes)
    }
}
