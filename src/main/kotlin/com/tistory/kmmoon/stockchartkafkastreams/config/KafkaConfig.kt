package com.tistory.kmmoon.stockchartkafkastreams.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.tistory.kmmoon.stockchartkafkastreams.model.CandleStick
import com.tistory.kmmoon.stockchartkafkastreams.model.MultiTimeFrameCandleStick
import com.tistory.kmmoon.stockchartkafkastreams.model.StockTransaction
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.support.serializer.JsonSerde

@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaConfig {

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${app.topics.stock-transactions}")
    private lateinit var transactionsTopic: String

    @Value("\${app.topics.stock-candles}")
    private lateinit var candlesTopic: String

    @Bean
    fun kafkaObjectMapper(): ObjectMapper {
        val objectMapper = ObjectMapper()
        objectMapper.registerModule(JavaTimeModule())
        objectMapper.registerKotlinModule()
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        return objectMapper
    }

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        val props = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "stock-chart-streams",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String()::class.java,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to JsonSerde::class.java,
            // 신뢰할 수 있는 패키지 설정
            "spring.json.trusted.packages" to "com.tistory.kmmoon.stockchartkafkastreams.model",
            // 명시적인 타입 매핑 추가
            "spring.json.type.mapping" to "transaction:com.tistory.kmmoon.stockchartkafkastreams.model.StockTransaction,candle:com.tistory.kmmoon.stockchartkafkastreams.model.CandleStick,multicandle:com.tistory.kmmoon.stockchartkafkastreams.model.MultiTimeFrameCandleStick"
        )
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun transactionsTopic(): NewTopic {
        return TopicBuilder.name(transactionsTopic)
            .partitions(1)
            .replicas(1)
            .build()
    }

    @Bean
    fun candlesTopic(): NewTopic {
        return TopicBuilder.name(candlesTopic)
            .partitions(1)
            .replicas(1)
            .build()
    }

    @Bean
    fun transactionSerde(kafkaObjectMapper: ObjectMapper): JsonSerde<StockTransaction> {
        val serde = JsonSerde(StockTransaction::class.java, kafkaObjectMapper)
        val configs = mapOf<String, Any>(
            "spring.json.trusted.packages" to "com.tistory.kmmoon.stockchartkafkastreams.model"
        )
        serde.configure(configs, false)
        return serde
    }

    @Bean
    fun candleStickSerde(kafkaObjectMapper: ObjectMapper): JsonSerde<CandleStick> {
        val serde = JsonSerde(CandleStick::class.java, kafkaObjectMapper)
        val configs = mapOf<String, Any>(
            "spring.json.trusted.packages" to "com.tistory.kmmoon.stockchartkafkastreams.model"
        )
        serde.configure(configs, false)
        return serde
    }

    @Bean
    fun multiTimeFrameCandleStickSerde(kafkaObjectMapper: ObjectMapper): JsonSerde<MultiTimeFrameCandleStick> {
        val serde = JsonSerde(MultiTimeFrameCandleStick::class.java, kafkaObjectMapper)
        val configs = mapOf<String, Any>(
            "spring.json.trusted.packages" to "com.tistory.kmmoon.stockchartkafkastreams.model"
        )
        serde.configure(configs, false)
        return serde
    }
}
