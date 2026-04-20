package com.applied.replication.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.JsonNode

/**
 * Payload produced by master DB outbox and consumed by receiver.
 *
 * [iterationCount] is incremented only on the receiver when a message is re-published to Kafka after `REQUEUE`;
 * the master always sends `0`.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class ReplicationMessage(
    val eventId: Long,
    val commitId: Long,
    val tableName: String,
    val operation: String,
    val payload: JsonNode,
    val iterationCount: Int = 0
)
