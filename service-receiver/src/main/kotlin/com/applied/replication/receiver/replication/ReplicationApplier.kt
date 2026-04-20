package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component

@Component
class ReplicationApplier(
    private val jdbcTemplate: NamedParameterJdbcTemplate,
    private val primaryKeyCache: ReplicationPrimaryKeyCache,
    private val upsertExecutor: ReplicationUpsertExecutor
) {
    enum class ApplyResult {
        APPLIED,
        SKIPPED,
        REQUEUE
    }

    private val identifierRegex = Regex("^[A-Za-z_][A-Za-z0-9_]*$")

    fun apply(message: ReplicationMessage): ApplyResult {
        val tableName = sanitizeIdentifier(message.tableName)
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
        val columns = fetchTableColumns(tableName)
        if (columns.isEmpty()) {
            throw IllegalStateException("Table '$tableName' has no columns.")
        }
        val quotedTable = quoteIdentifier(tableName)
        val quotedColumns = columns.map { quoteIdentifier(it.name) }
        val quotedPrimaryKeys = primaryKeys.map(::quoteIdentifier)
        val nonPrimaryColumns = columns.filterNot { it.name in primaryKeys }
        val updateAssignments = if (nonPrimaryColumns.isEmpty()) {
            quotedPrimaryKeys.joinToString(", ") { "$it = EXCLUDED.$it" }
        } else {
            nonPrimaryColumns.joinToString(", ") {
                val quoted = quoteIdentifier(it.name)
                "$quoted = EXCLUDED.$quoted"
            }
        }

        val srcSelectList = columns.joinToString(",\n              ") { col ->
            "${jsonbToSqlExpr(col)} AS ${quoteIdentifier(col.name)}"
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
        val quotedTable = quoteIdentifier(tableName)
        val columnsByName = fetchTableColumns(tableName).associateBy { it.name }
        val joinCondition = primaryKeys.joinToString(" AND ") { pk ->
            if (!columnsByName.containsKey(pk)) {
                throw IllegalStateException("Primary key column '$pk' not found in table metadata.")
            }
            val quoted = quoteIdentifier(pk)
            "t.$quoted = src.$quoted"
        }
        val versionCol = columnsByName["version"]
            ?: throw IllegalStateException("Table '$tableName' must have column 'version' for replication deletes.")
        val srcSelectList = buildString {
            primaryKeys.forEach { pk ->
                val col = columnsByName[pk]
                    ?: throw IllegalStateException("Primary key column '$pk' not found in table metadata.")
                append(jsonbToSqlExpr(col))
                append(" AS ")
                append(quoteIdentifier(pk))
                append(",\n              ")
            }
            append(jsonbToSqlExpr(versionCol))
            append(" AS ")
            append(quoteIdentifier("version"))
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

    private fun fetchTableColumns(tableName: String): List<ColumnMeta> {
        return jdbcTemplate.query(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :tableName
            ORDER BY ordinal_position
            """.trimIndent(),
            mapOf("tableName" to tableName)
        ) { rs, _ ->
            ColumnMeta(
                name = rs.getString("column_name"),
                dataType = rs.getString("data_type"),
                udtName = rs.getString("udt_name") ?: ""
            )
        }
    }

    /**
     * Maps one JSON field from the `:payload` bind variable to a SQL scalar/array expression.
     */
    private fun jsonbToSqlExpr(column: ColumnMeta): String {
        require(identifierRegex.matches(column.name)) { "Unsafe column name '${column.name}'." }
        val p = "CAST(:payload AS jsonb)"
        val f = column.name
        return when {
            column.dataType.equals("ARRAY", ignoreCase = true) ->
                when (column.udtName) {
                    "_text" -> "ARRAY(SELECT jsonb_array_elements_text($p -> '$f'))::text[]"
                    else -> throw IllegalStateException(
                        "Unsupported Postgres array type '${column.udtName}' for column '$f'. Extend jsonbToSqlExpr()."
                    )
                }
            column.dataType.equals("jsonb", ignoreCase = true) || column.udtName.equals("jsonb", ignoreCase = true) ->
                "($p -> '$f')::jsonb"
            column.dataType.equals("json", ignoreCase = true) || column.udtName.equals("json", ignoreCase = true) ->
                "($p -> '$f')::json"
            else -> {
                val cast = pgTextJsonToScalarCast(column)
                "($p->>'$f')::$cast"
            }
        }
    }

    private fun pgTextJsonToScalarCast(column: ColumnMeta): String {
        val dt = column.dataType.lowercase()
        return when (dt) {
            "smallint", "integer", "bigint", "numeric", "decimal", "real",
            "double precision", "boolean", "date", "uuid",
            "time without time zone", "time with time zone",
            "timestamp without time zone", "timestamp with time zone" -> dt
            "character varying" -> "varchar"
            "character" -> "char"
            "text" -> "text"
            "USER-DEFINED" -> when (column.udtName) {
                "int2" -> "smallint"
                "int4" -> "integer"
                "int8" -> "bigint"
                "float4" -> "real"
                "float8" -> "double precision"
                "bool" -> "boolean"
                "numeric" -> "numeric"
                "timestamptz" -> "timestamptz"
                "timestamp" -> "timestamp"
                else -> "text"
            }
            else -> when (column.udtName) {
                "int2" -> "smallint"
                "int4" -> "integer"
                "int8" -> "bigint"
                "float4" -> "real"
                "float8" -> "double precision"
                "bool" -> "boolean"
                "numeric" -> "numeric"
                "timestamptz" -> "timestamptz"
                "timestamp" -> "timestamp"
                "date" -> "date"
                "text", "varchar", "bpchar" -> "text"
                else -> "text"
            }
        }
    }

    private data class ColumnMeta(
        val name: String,
        val dataType: String,
        val udtName: String
    )

    private fun sanitizeIdentifier(name: String): String {
        require(identifierRegex.matches(name)) { "Unsafe SQL identifier '$name'." }
        return name
    }

    private fun quoteIdentifier(name: String): String {
        return "\"${name.replace("\"", "\"\"")}\""
    }
}
