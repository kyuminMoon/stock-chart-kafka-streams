package com.tistory.kmmoon.stockchartkafkastreams

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class StockChartKafkaStreamsApplication

fun main(args: Array<String>) {
    runApplication<StockChartKafkaStreamsApplication>(*args)
}
