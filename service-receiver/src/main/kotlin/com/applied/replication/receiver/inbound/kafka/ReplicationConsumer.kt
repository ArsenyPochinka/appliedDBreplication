package com.applied.replication.receiver.inbound.kafka

import com.applied.replication.common.ReplicationMessage
import com.applied.replication.receiver.config.ReplicationProperties
import com.applied.replication.receiver.replication.ReplicationApplier
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class ReplicationConsumer(
    private val applier: ReplicationApplier,
    private val kafkaTemplate: KafkaTemplate<String, ReplicationMessage>,
    private val replicationProperties: ReplicationProperties
) {

    @KafkaListener(topics = ["\${app.replication.topic}"])
    fun listen(message: ReplicationMessage) {
        if (message.iterationCount > replicationProperties.maxRequeueIterations) {
            return
        }
        val result = applier.apply(message)
        if (result == ReplicationApplier.ApplyResult.REQUEUE) {
            val topic = replicationProperties.topic
            val republished = message.copy(iterationCount = message.iterationCount + 1)
            kafkaTemplate.send(topic, message.commitId.toString(), republished)
        }
    }
}
