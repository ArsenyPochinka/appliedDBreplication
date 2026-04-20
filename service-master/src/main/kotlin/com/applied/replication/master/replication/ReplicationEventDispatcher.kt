package com.applied.replication.master.replication

import com.applied.replication.common.ReplicationMessage
import com.fasterxml.jackson.databind.JsonNode
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.util.concurrent.atomic.AtomicLong

@Component
class ReplicationEventDispatcher(
    private val kafkaTemplate: KafkaTemplate<String, ReplicationMessage>,
    @Value("\${app.replication.topic}") private val topic: String
) {
    private val eventSequence = AtomicLong(0)

    fun dispatchAfterCommit(tableName: String, operation: String, payload: JsonNode) {
        val message = ReplicationMessage(
            eventId = eventSequence.incrementAndGet(),
            tableName = tableName,
            operation = operation,
            payload = payload
        )
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            kafkaTemplate.send(topic, tableName, message)
            return
        }
        TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
            override fun afterCommit() {
                kafkaTemplate.send(topic, tableName, message)
            }
        })
    }
}
