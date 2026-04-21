package com.applied.replication.receiver.inbound.kafka

import com.applied.replication.common.ReplicationMessage
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class ReplicationKafkaConsumerConfiguration {

    @Bean
    fun replicationConsumerFactory(kafkaProperties: KafkaProperties): ConsumerFactory<String, ReplicationMessage> {
        val props = kafkaProperties.buildConsumerProperties()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonDeserializer::class.java
        props[JsonDeserializer.TRUSTED_PACKAGES] = "*"
        props[JsonDeserializer.VALUE_DEFAULT_TYPE] = ReplicationMessage::class.java.name
        return DefaultKafkaConsumerFactory(props)
    }

    @Bean
    fun kafkaListenerContainerFactory(
        replicationConsumerFactory: ConsumerFactory<String, ReplicationMessage>
    ): ConcurrentKafkaListenerContainerFactory<String, ReplicationMessage> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, ReplicationMessage>()
        factory.consumerFactory = replicationConsumerFactory
        return factory
    }
}
