package com.applied.replication.common

import com.fasterxml.jackson.databind.JsonNode

/**
 * Payload produced by master DB outbox and consumed by receiver.
 */
data class ReplicationMessage(
    val eventId: Long,
    val tableName: String,
    val operation: String,
    val payload: JsonNode
)
