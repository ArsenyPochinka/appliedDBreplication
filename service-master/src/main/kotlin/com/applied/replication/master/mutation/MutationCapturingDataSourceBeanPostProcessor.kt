package com.applied.replication.master.mutation

import com.applied.replication.master.outbound.kafka.ReplicationEventDispatcher
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.stereotype.Component
import javax.sql.DataSource

/**
 * Wraps the primary [DataSource] so that JDBC [java.sql.Connection] / [java.sql.PreparedStatement] calls
 * run through [MutationReturningSqlRewriter] and publish [RETURNING *] rows to Kafka after commit.
 */
@Component
class MutationCapturingDataSourceBeanPostProcessor(
    private val mutationReturningSqlRewriter: MutationReturningSqlRewriter,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val jdbcRowReplicationPayloadMapper: JdbcRowReplicationPayloadMapper
) : BeanPostProcessor {

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        if (bean !is DataSource || bean is MutationCapturingDataSource) {
            return bean
        }
        return MutationCapturingDataSource(
            bean,
            mutationReturningSqlRewriter,
            replicationEventDispatcher,
            jdbcRowReplicationPayloadMapper
        )
    }
}
