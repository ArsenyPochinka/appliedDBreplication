package com.applied.replication.master.outbound.kafka

import com.applied.replication.common.ReplicationMessage
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class ReplicationKafkaProducerConfiguration {

    @Bean
    fun producerFactory(kafkaProperties: KafkaProperties): ProducerFactory<String, ReplicationMessage> {
        val props = kafkaProperties.buildProducerProperties()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java
        return DefaultKafkaProducerFactory(props)
    }

    @Bean
    fun replicationKafkaTemplate(producerFactory: ProducerFactory<String, ReplicationMessage>): KafkaTemplate<String, ReplicationMessage> {
        return KafkaTemplate(producerFactory)
    }
}
