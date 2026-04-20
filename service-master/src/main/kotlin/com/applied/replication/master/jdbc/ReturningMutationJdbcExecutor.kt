package com.applied.replication.master.jdbc

import org.springframework.stereotype.Component
import java.util.regex.Pattern

@Component
class ReturningMutationJdbcExecutor {
    private val insertTablePattern = Pattern.compile("^\\s*INSERT\\s+INTO\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val updateTablePattern = Pattern.compile("^\\s*UPDATE\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val deleteTablePattern = Pattern.compile("^\\s*DELETE\\s+FROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val returningTailPattern = Pattern.compile("\\bRETURNING\\b[\\s\\S]*$", Pattern.CASE_INSENSITIVE)

    fun enrichMutationSqlIfNeeded(sql: String): String {
        val sanitizedSql = sql.trim()
        if (!isMutationSql(sanitizedSql)) {
            return sql
        }
        val noSemicolonSql = sanitizedSql.trimEnd(';')
        val withoutReturning = returningTailPattern.matcher(noSemicolonSql).replaceFirst("").trimEnd()
        return "$withoutReturning RETURNING *"
    }

    fun mutationMetaOrNull(sql: String): MutationMeta? {
        val sanitizedSql = sql.trim()
        parseTableName(insertTablePattern, sanitizedSql)?.let { return MutationMeta(it, "INSERT") }
        parseTableName(updateTablePattern, sanitizedSql)?.let { return MutationMeta(it, "UPDATE") }
        parseTableName(deleteTablePattern, sanitizedSql)?.let { return MutationMeta(it, "DELETE") }
        return null
    }

    fun isMutationSql(sql: String): Boolean {
        val sanitizedSql = sql.trim()
        return insertTablePattern.matcher(sanitizedSql).find() ||
            updateTablePattern.matcher(sanitizedSql).find() ||
            deleteTablePattern.matcher(sanitizedSql).find()
    }

    private fun parseTableName(pattern: Pattern, sql: String): String? {
        val matcher = pattern.matcher(sql)
        return if (matcher.find()) matcher.group(1).substringAfterLast(".") else null
    }

    data class MutationMeta(
        val tableName: String,
        val operation: String
    )

}
