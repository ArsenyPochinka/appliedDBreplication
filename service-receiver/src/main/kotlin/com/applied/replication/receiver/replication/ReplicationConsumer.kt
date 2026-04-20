package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class ReplicationConsumer(
    private val applier: ReplicationApplier
) {

    @KafkaListener(topics = ["\${app.replication.topic}"])
    fun listen(message: ReplicationMessage) {
        applier.apply(message)
    }
}
