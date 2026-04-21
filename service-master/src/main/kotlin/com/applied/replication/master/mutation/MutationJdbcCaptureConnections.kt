package com.applied.replication.master.mutation

import com.applied.replication.master.outbound.kafka.ReplicationEventDispatcher
import org.springframework.jdbc.datasource.DelegatingDataSource
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicLong
import javax.sql.DataSource

/** Bound for the lifetime of the current Spring-managed transaction (no extra DB roundtrip). */
private val replicationCommitIdResourceKey = Any()

private val replicationCommitIdSequence = AtomicLong(0)

internal class MutationCapturingDataSource(
    targetDataSource: DataSource,
    private val sqlRewriter: MutationReturningSqlRewriter,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val rowPayloadMapper: JdbcRowReplicationPayloadMapper
) : DelegatingDataSource(targetDataSource) {

    override fun getConnection() = wrapConnection(super.getConnection())

    override fun getConnection(username: String, password: String) =
        wrapConnection(super.getConnection(username, password))

    private fun wrapConnection(connection: java.sql.Connection): java.sql.Connection {
        val handler = MutationCaptureConnectionHandler(
            connection,
            sqlRewriter,
            replicationEventDispatcher,
            rowPayloadMapper
        )
        return Proxy.newProxyInstance(
            connection.javaClass.classLoader,
            arrayOf(java.sql.Connection::class.java),
            handler
        ) as java.sql.Connection
    }
}

internal class MutationCaptureConnectionHandler(
    private val target: java.sql.Connection,
    private val sqlRewriter: MutationReturningSqlRewriter,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val rowPayloadMapper: JdbcRowReplicationPayloadMapper
) : InvocationHandler {

    @Volatile
    private var connectionScopedCommitId: Long? = null

    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        val callArgs = (args?.copyOf() as Array<Any?>?) ?: emptyArray()
        val maybeSql = callArgs.firstOrNull() as? String
        val rewrittenSql = if (maybeSql != null && isPrepareMethod(method.name)) {
            sqlRewriter.enrichMutationSqlIfNeeded(maybeSql)
        } else {
            null
        }
        val rewrittenArgs = if (rewrittenSql != null) {
            callArgs.copyOf().also {
                it[0] = rewrittenSql
            }
        } else {
            callArgs
        }
        val result = try {
            method.invoke(target, *rewrittenArgs)
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
        if (result is PreparedStatement && rewrittenSql != null) {
            val mutationMeta = sqlRewriter.mutationMetaOrNull(rewrittenSql)
            if (mutationMeta != null) {
                return wrapPreparedStatement(result, mutationMeta)
            }
        }
        return result
    }

    private fun isPrepareMethod(methodName: String): Boolean {
        return methodName == "prepareStatement" || methodName == "prepareCall"
    }

    private fun wrapPreparedStatement(
        statement: PreparedStatement,
        mutationMeta: MutationReturningSqlRewriter.MutationMeta
    ): PreparedStatement {
        val commitId = resolveCommitId()
        return Proxy.newProxyInstance(
            statement.javaClass.classLoader,
            arrayOf(PreparedStatement::class.java),
            MutationCapturePreparedStatementHandler(
                statement,
                mutationMeta,
                commitId,
                replicationEventDispatcher,
                rowPayloadMapper
            )
        ) as PreparedStatement
    }

    private fun resolveCommitId(): Long {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            val existing = TransactionSynchronizationManager.getResource(replicationCommitIdResourceKey) as? Long
            if (existing != null) {
                return existing
            }
            val id = replicationCommitIdSequence.incrementAndGet()
            TransactionSynchronizationManager.bindResource(replicationCommitIdResourceKey, id)
            return id
        }
        connectionScopedCommitId?.let { return it }
        val id = replicationCommitIdSequence.incrementAndGet()
        connectionScopedCommitId = id
        return id
    }
}

internal class MutationCapturePreparedStatementHandler(
    private val target: PreparedStatement,
    private val mutationMeta: MutationReturningSqlRewriter.MutationMeta,
    private val commitId: Long,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val rowPayloadMapper: JdbcRowReplicationPayloadMapper
) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        return when (method.name) {
            "executeUpdate" -> {
                val hasResultSet = target.execute()
                if (hasResultSet) {
                    return rowPayloadMapper.publishReturningRows(target.resultSet, mutationMeta, commitId, replicationEventDispatcher)
                }
                target.updateCount
            }
            "executeLargeUpdate" -> {
                val hasResultSet = target.execute()
                if (hasResultSet) {
                    return rowPayloadMapper.publishReturningRows(
                        target.resultSet,
                        mutationMeta,
                        commitId,
                        replicationEventDispatcher
                    ).toLong()
                }
                target.largeUpdateCount
            }
            "execute" -> {
                val result = invokeTarget(method, args)
                rowPayloadMapper.publishReturningRows(target.resultSet, mutationMeta, commitId, replicationEventDispatcher)
                result
            }
            else -> invokeTarget(method, args)
        }
    }

    private fun invokeTarget(method: Method, args: Array<out Any?>?): Any? {
        return try {
            method.invoke(target, *(args ?: emptyArray()))
        } catch (ex: InvocationTargetException) {
            throw ex.targetException
        }
    }
}
