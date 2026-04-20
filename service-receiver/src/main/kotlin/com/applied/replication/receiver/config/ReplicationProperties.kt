package com.applied.replication.receiver.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("app.replication")
data class ReplicationProperties(
    val topic: String = "db-replication-events",
    /**
     * When [com.applied.replication.common.ReplicationMessage.iterationCount] exceeds this value,
     * the consumer stops reprocessing and drops the message (no apply, no requeue).
     */
    val maxRequeueIterations: Int = 10
)
