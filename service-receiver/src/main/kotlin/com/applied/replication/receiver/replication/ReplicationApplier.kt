package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import com.applied.replication.receiver.apply.ReplicationPayloadJsonSqlExpressions
import com.applied.replication.receiver.apply.ReplicationSqlGuards
import com.applied.replication.receiver.apply.ReplicationTableColumnCatalog
import com.applied.replication.receiver.apply.ReplicationUpsertExecutor
import com.applied.replication.receiver.schema.ReplicationPrimaryKeyCache
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component

@Component
class ReplicationApplier(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val primaryKeyCache: ReplicationPrimaryKeyCache,
    private val upsertExecutor: ReplicationUpsertExecutor,
    private val columnCatalog: ReplicationTableColumnCatalog,
    private val payloadSql: ReplicationPayloadJsonSqlExpressions
) {
    enum class ApplyResult {
        APPLIED,
        SKIPPED,
        REQUEUE
    }

    fun apply(message: ReplicationMessage): ApplyResult {
        val tableName = ReplicationSqlGuards.sanitizeIdentifier(message.tableName)
        val primaryKeys = primaryKeyCache.primaryKeysForTable(tableName)
        if (primaryKeys.isEmpty()) {
            throw IllegalStateException("Table '$tableName' has no primary key. Replication requires PK columns.")
        }
        val messageVersion = message.payload.path("version").asInt(Int.MIN_VALUE)
        require(messageVersion != Int.MIN_VALUE) {
            "Replication payload for table '$tableName' must contain integer field 'version'."
        }
        val payloadJson = message.payload.toString()
        val params = MapSqlParameterSource().addValue("payload", payloadJson)
        return when (message.operation.uppercase()) {
            "INSERT", "UPDATE" -> applyUpsert(tableName, params, primaryKeys)
            "DELETE" -> applyDelete(tableName, message.payload, primaryKeys)
            else -> throw IllegalArgumentException("Unsupported replication operation '${message.operation}'.")
        }
    }

    private fun applyUpsert(
        tableName: String,
        params: MapSqlParameterSource,
        primaryKeys: List<String>
    ): ApplyResult {
        val columns = columnCatalog.columnsOrdered(tableName)
        if (columns.isEmpty()) {
            throw IllegalStateException("Table '$tableName' has no columns.")
        }
        val quotedTable = ReplicationSqlGuards.quoteIdentifier(tableName)
        val quotedColumns = columns.map { ReplicationSqlGuards.quoteIdentifier(it.name) }
        val quotedPrimaryKeys = primaryKeys.map(ReplicationSqlGuards::quoteIdentifier)
        val nonPrimaryColumns = columns.filterNot { it.name in primaryKeys }
        val updateAssignments = if (nonPrimaryColumns.isEmpty()) {
            quotedPrimaryKeys.joinToString(", ") { "$it = EXCLUDED.$it" }
        } else {
            nonPrimaryColumns.joinToString(", ") {
                val quoted = ReplicationSqlGuards.quoteIdentifier(it.name)
                "$quoted = EXCLUDED.$quoted"
            }
        }

        val srcSelectList = columns.joinToString(",\n              ") { col ->
            "${payloadSql.jsonbToSqlExpr(col)} AS ${ReplicationSqlGuards.quoteIdentifier(col.name)}"
        }

        val sql = """
            WITH src AS (
              SELECT
              $srcSelectList
            ),
            applied AS (
              INSERT INTO $quotedTable (${quotedColumns.joinToString(", ")})
              SELECT ${quotedColumns.joinToString(", ")} FROM src
              ON CONFLICT (${quotedPrimaryKeys.joinToString(", ")}) DO UPDATE SET
                $updateAssignments
              WHERE $quotedTable.version + 1 = EXCLUDED.version
              RETURNING 1
            )
            SELECT COUNT(*)::int FROM applied
        """.trimIndent()
        return when (upsertExecutor.execute(sql, params)) {
            ReplicationUpsertExecutor.UpsertOutcome.APPLIED -> ApplyResult.APPLIED
            ReplicationUpsertExecutor.UpsertOutcome.SKIPPED,
            ReplicationUpsertExecutor.UpsertOutcome.SKIPPED_DUPLICATE -> ApplyResult.SKIPPED
            ReplicationUpsertExecutor.UpsertOutcome.REQUEUE -> ApplyResult.REQUEUE
        }
    }

    private fun applyDelete(
        tableName: String,
        payload: com.fasterxml.jackson.databind.JsonNode,
        primaryKeys: List<String>
    ): ApplyResult {
        primaryKeys.forEach { key ->
            val valueNode = payload.get(key)
            if (valueNode == null || valueNode.isNull) {
                throw IllegalArgumentException("DELETE payload for table '$tableName' misses PK field '$key'.")
            }
        }
        val quotedTable = ReplicationSqlGuards.quoteIdentifier(tableName)
        val columnsByName = columnCatalog.columnsOrdered(tableName).associateBy { it.name }
        val joinCondition = primaryKeys.joinToString(" AND ") { pk ->
            if (!columnsByName.containsKey(pk)) {
                throw IllegalStateException("Primary key column '$pk' not found in table metadata.")
            }
            val quoted = ReplicationSqlGuards.quoteIdentifier(pk)
            "t.$quoted = src.$quoted"
        }
        val versionCol = columnsByName["version"]
            ?: throw IllegalStateException("Table '$tableName' must have column 'version' for replication deletes.")
        val srcSelectList = buildString {
            primaryKeys.forEach { pk ->
                val col = columnsByName[pk]
                    ?: throw IllegalStateException("Primary key column '$pk' not found in table metadata.")
                append(payloadSql.jsonbToSqlExpr(col))
                append(" AS ")
                append(ReplicationSqlGuards.quoteIdentifier(pk))
                append(",\n              ")
            }
            append(payloadSql.jsonbToSqlExpr(versionCol))
            append(" AS ")
            append(ReplicationSqlGuards.quoteIdentifier("version"))
        }

        val sql = """
            WITH src AS (
              SELECT
              $srcSelectList
            ),
            pre AS (
              SELECT t.version AS prev_version
              FROM $quotedTable t
              INNER JOIN src ON $joinCondition
            ),
            deleted AS (
              DELETE FROM $quotedTable t
              USING src
              WHERE $joinCondition
                AND t.version + 1 = src.version
              RETURNING 1
            )
            SELECT
              (SELECT COUNT(*)::int FROM deleted) AS deleted_cnt,
              (SELECT prev_version FROM pre LIMIT 1) AS prev_version
        """.trimIndent()
        val params = MapSqlParameterSource().addValue("payload", payload.toString())
        return jdbcTemplate.query(sql, params) { rs, _ ->
            val deletedCnt = rs.getInt("deleted_cnt")
            val prevVersion = rs.getObject("prev_version") as? Int
            when {
                deletedCnt > 0 -> ApplyResult.APPLIED
                prevVersion == null -> ApplyResult.SKIPPED
                else -> ApplyResult.REQUEUE
            }
        }.single()
    }
}
