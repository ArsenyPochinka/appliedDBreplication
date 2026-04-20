package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class ReplicationApplier(
    private val jdbcTemplate: NamedParameterJdbcTemplate
) {
    private val identifierRegex = Regex("^[A-Za-z_][A-Za-z0-9_]*$")

    @Transactional
    fun apply(message: ReplicationMessage) {
        val tableName = sanitizeIdentifier(message.tableName)
        val primaryKeys = fetchPrimaryKeys(tableName)
        if (primaryKeys.isEmpty()) {
            throw IllegalStateException("Table '$tableName' has no primary key. Replication requires PK columns.")
        }
        val payloadJson = message.payload.toString()
        val params = MapSqlParameterSource().addValue("payload", payloadJson)
        when (message.operation.uppercase()) {
            "INSERT", "UPDATE" -> applyUpsert(tableName, params, primaryKeys)
            "DELETE" -> applyDelete(tableName, message.payload, primaryKeys)
            else -> throw IllegalArgumentException("Unsupported replication operation '${message.operation}'.")
        }
    }

    private fun applyUpsert(tableName: String, params: MapSqlParameterSource, primaryKeys: List<String>) {
        val columns = fetchTableColumns(tableName)
        if (columns.isEmpty()) {
            throw IllegalStateException("Table '$tableName' has no columns.")
        }
        val quotedColumns = columns.map(::quoteIdentifier)
        val quotedPrimaryKeys = primaryKeys.map(::quoteIdentifier)
        val nonPrimaryColumns = columns.filterNot { it in primaryKeys }
        val updateAssignments = if (nonPrimaryColumns.isEmpty()) {
            quotedPrimaryKeys.joinToString(", ") { "$it = EXCLUDED.$it" }
        } else {
            nonPrimaryColumns.joinToString(", ") {
                val quoted = quoteIdentifier(it)
                "$quoted = EXCLUDED.$quoted"
            }
        }

        val sql = """
            WITH src AS (
              SELECT * FROM jsonb_populate_record(NULL::${quoteIdentifier(tableName)}, CAST(:payload AS jsonb))
            )
            INSERT INTO ${quoteIdentifier(tableName)} (${quotedColumns.joinToString(", ")})
            SELECT ${quotedColumns.joinToString(", ")} FROM src
            ON CONFLICT (${quotedPrimaryKeys.joinToString(", ")}) DO UPDATE SET
              $updateAssignments
        """.trimIndent()
        jdbcTemplate.update(sql, params)
    }

    private fun applyDelete(
        tableName: String,
        payload: com.fasterxml.jackson.databind.JsonNode,
        primaryKeys: List<String>
    ) {
        primaryKeys.forEach { key ->
            val valueNode = payload.get(key)
            if (valueNode == null || valueNode.isNull) {
                throw IllegalArgumentException("DELETE payload for table '$tableName' misses PK field '$key'.")
            }
        }
        val joinCondition = primaryKeys.joinToString(" AND ") {
            val quoted = quoteIdentifier(it)
            "t.$quoted = src.$quoted"
        }
        val sql = """
            WITH src AS (
              SELECT * FROM jsonb_populate_record(NULL::${quoteIdentifier(tableName)}, CAST(:payload AS jsonb))
            )
            DELETE FROM ${quoteIdentifier(tableName)} t
            USING src
            WHERE $joinCondition
        """.trimIndent()
        val params = MapSqlParameterSource().addValue("payload", payload.toString())
        jdbcTemplate.update(sql, params)
    }

    private fun fetchTableColumns(tableName: String): List<String> {
        return jdbcTemplate.queryForList(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :tableName
            ORDER BY ordinal_position
            """.trimIndent(),
            mapOf("tableName" to tableName),
            String::class.java
        )
    }

    private fun fetchPrimaryKeys(tableName: String): List<String> {
        return jdbcTemplate.queryForList(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = current_schema()
              AND tc.table_name = :tableName
            ORDER BY kcu.ordinal_position
            """.trimIndent(),
            mapOf("tableName" to tableName),
            String::class.java
        )
    }

    private fun sanitizeIdentifier(name: String): String {
        require(identifierRegex.matches(name)) { "Unsafe SQL identifier '$name'." }
        return name
    }

    private fun quoteIdentifier(name: String): String {
        return "\"${name.replace("\"", "\"\"")}\""
    }

}
