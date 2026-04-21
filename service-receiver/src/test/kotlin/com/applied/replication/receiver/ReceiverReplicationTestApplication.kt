package com.applied.replication.receiver

import com.applied.replication.receiver.apply.ReplicationPayloadJsonSqlExpressions
import com.applied.replication.receiver.apply.ReplicationTableColumnCatalog
import com.applied.replication.receiver.apply.ReplicationUpsertExecutor
import com.applied.replication.receiver.replication.ReplicationApplier
import com.applied.replication.receiver.schema.ReplicationPrimaryKeyCache
import org.springframework.boot.SpringBootConfiguration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Import

/**
 * Minimal slice for receiver replication tests (no Kafka listener, no consumer bean).
 */
@SpringBootConfiguration
@EnableAutoConfiguration
@Import(
    ReplicationApplier::class,
    ReplicationPrimaryKeyCache::class,
    ReplicationUpsertExecutor::class,
    ReplicationTableColumnCatalog::class,
    ReplicationPayloadJsonSqlExpressions::class
)
class ReceiverReplicationTestApplication
