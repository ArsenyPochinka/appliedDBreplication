package com.applied.replication.master.mutation

import com.applied.replication.master.outbound.kafka.ReplicationEventDispatcher
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.Array as SqlArray
import java.sql.Blob
import java.sql.Clob
import java.sql.ResultSet
import java.sql.SQLXML
import java.sql.Struct
import org.springframework.stereotype.Component

/**
 * Maps JDBC [ResultSet] rows from [RETURNING *] into JSON payloads and hands them to [ReplicationEventDispatcher].
 */
@Component
class JdbcRowReplicationPayloadMapper(
    private val objectMapper: ObjectMapper
) {

    fun publishReturningRows(
        resultSet: ResultSet?,
        mutationMeta: MutationReturningSqlRewriter.MutationMeta,
        commitId: Long,
        replicationEventDispatcher: ReplicationEventDispatcher
    ): Int {
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
                    val rawValue = normalizeJdbcValue(rs.getObject(idx))
                    val normalizedValue = if (mutationMeta.operation == "DELETE" && columnName.equals("version", ignoreCase = true)) {
                        ((rawValue as? Number)?.toLong() ?: 0L) + 1L
                    } else {
                        rawValue
                    }
                    payload.set<JsonNode>(columnName, toSafeJsonNode(normalizedValue))
                }
                replicationEventDispatcher.dispatchAfterCommit(commitId, mutationMeta.tableName, mutationMeta.operation, payload)
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
                if (value.javaClass.name.startsWith("org.postgresql.")) value.toString() else value
            }
        }
    }
}
