package com.applied.replication.master.jdbc

import com.applied.replication.master.replication.ReplicationEventDispatcher
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.Array as SqlArray
import java.sql.Blob
import java.sql.Clob
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLXML
import java.sql.Struct
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.jdbc.datasource.DelegatingDataSource
import org.springframework.stereotype.Component
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import javax.sql.DataSource

@Component
class ReturningSqlDataSourceInterceptor(
    private val returningMutationJdbcExecutor: ReturningMutationJdbcExecutor,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val objectMapper: ObjectMapper
) : BeanPostProcessor {

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        if (bean !is DataSource || bean is ReturningAwareDataSource) {
            return bean
        }
        return ReturningAwareDataSource(bean, returningMutationJdbcExecutor, replicationEventDispatcher, objectMapper)
    }
}

private class ReturningAwareDataSource(
    targetDataSource: DataSource,
    private val sqlHandler: ReturningMutationJdbcExecutor,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val objectMapper: ObjectMapper
) : DelegatingDataSource(targetDataSource) {

    override fun getConnection() = wrapConnection(super.getConnection())

    override fun getConnection(username: String, password: String) =
        wrapConnection(super.getConnection(username, password))

    private fun wrapConnection(connection: java.sql.Connection): java.sql.Connection {
        val handler = ConnectionInvocationHandler(connection, sqlHandler, replicationEventDispatcher, objectMapper)
        return Proxy.newProxyInstance(
            connection.javaClass.classLoader,
            arrayOf(java.sql.Connection::class.java),
            handler
        ) as java.sql.Connection
    }
}

private class ConnectionInvocationHandler(
    private val target: java.sql.Connection,
    private val sqlHandler: ReturningMutationJdbcExecutor,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val objectMapper: ObjectMapper
) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        val callArgs = (args?.copyOf() as Array<Any?>?) ?: emptyArray()
        val maybeSql = callArgs.firstOrNull() as? String
        val rewrittenSql = if (maybeSql != null && isPrepareMethod(method.name)) {
            sqlHandler.enrichMutationSqlIfNeeded(maybeSql)
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
            val mutationMeta = sqlHandler.mutationMetaOrNull(rewrittenSql)
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
        mutationMeta: ReturningMutationJdbcExecutor.MutationMeta
    ): PreparedStatement {
        return Proxy.newProxyInstance(
            statement.javaClass.classLoader,
            arrayOf(PreparedStatement::class.java),
            PreparedStatementInvocationHandler(statement, mutationMeta, replicationEventDispatcher, objectMapper)
        ) as PreparedStatement
    }
}

private class PreparedStatementInvocationHandler(
    private val target: PreparedStatement,
    private val mutationMeta: ReturningMutationJdbcExecutor.MutationMeta,
    private val replicationEventDispatcher: ReplicationEventDispatcher,
    private val objectMapper: ObjectMapper
) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        return when (method.name) {
            "executeUpdate" -> {
                val hasResultSet = target.execute()
                if (hasResultSet) {
                    return publishResultRows(target.resultSet)
                }
                target.updateCount
            }
            "executeLargeUpdate" -> {
                val hasResultSet = target.execute()
                if (hasResultSet) {
                    return publishResultRows(target.resultSet).toLong()
                }
                target.largeUpdateCount
            }
            "execute" -> {
                val result = invokeTarget(method, args)
                publishResultRows(target.resultSet)
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

    private fun publishResultRows(resultSet: ResultSet?): Int {
        if (resultSet == null) {
            return 0
        }
        var rows = 0
        resultSet.use { rs ->
            val meta = rs.metaData
            val columnCount = meta.columnCount
            while (rs.next()) {
                rows++
                val payload = objectMapper.createObjectNode()
                for (idx in 1..columnCount) {
                    val columnName = meta.getColumnLabel(idx)
                    payload.set<JsonNode>(columnName, toSafeJsonNode(normalizeJdbcValue(rs.getObject(idx))))
                }
                replicationEventDispatcher.dispatchAfterCommit(mutationMeta.tableName, mutationMeta.operation, payload)
            }
        }
        return rows
    }

    private fun toSafeJsonNode(value: Any?): JsonNode {
        return try {
            objectMapper.valueToTree(value)
        } catch (ex: IllegalArgumentException) {
            objectMapper.valueToTree(value?.toString())
        } catch (ex: JsonProcessingException) {
            objectMapper.valueToTree(value?.toString())
        }
    }

    private fun normalizeJdbcValue(value: Any?): Any? {
        if (value == null) {
            return null
        }
        return when (value) {
            is SqlArray -> {
                try {
                    val arrayValue = value.array
                    when (arrayValue) {
                        is Array<*> -> arrayValue.map { normalizeJdbcValue(it) }
                        is IntArray -> arrayValue.toList()
                        is LongArray -> arrayValue.toList()
                        is DoubleArray -> arrayValue.toList()
                        is FloatArray -> arrayValue.toList()
                        is BooleanArray -> arrayValue.toList()
                        is ShortArray -> arrayValue.toList()
                        is ByteArray -> arrayValue.toList()
                        else -> arrayValue?.toString()
                    }
                } finally {
                    kotlin.runCatching { value.free() }
                }
            }
            is Clob -> value.characterStream.use { it.readText() }
            is Blob -> value.binaryStream.use { it.readBytes().toList() }
            is SQLXML -> value.string
            is Struct -> value.attributes.map { normalizeJdbcValue(it) }
            is java.sql.Date, is java.sql.Time, is java.sql.Timestamp -> value.toString()
            else -> {
                if (value.javaClass.name == "org.postgresql.util.PGobject") {
                    val type = kotlin.runCatching {
                        value.javaClass.getMethod("getType").invoke(value) as? String
                    }.getOrNull()?.lowercase()
                    val rawValue = kotlin.runCatching {
                        value.javaClass.getMethod("getValue").invoke(value) as? String
                    }.getOrNull()
                    if (rawValue == null) {
                        return null
                    }
                    if (type == "json" || type == "jsonb") {
                        return kotlin.runCatching { objectMapper.readTree(rawValue) }.getOrElse { rawValue }
                    }
                    return rawValue
                }
                // Prevent JDBC driver internals from leaking into Jackson serialization.
                if (value.javaClass.name.startsWith("org.postgresql.")) value.toString() else value
            }
        }
    }
}
