package com.applied.replication.master.mutation

import org.springframework.stereotype.Component
import java.util.regex.Pattern

/**
 * Rewrites DML statements so that PostgreSQL returns full rows via [RETURNING *]
 * and applies the replication [version] increment rule for UPDATE when needed.
 */
@Component
class MutationReturningSqlRewriter {
    private val insertTablePattern = Pattern.compile("^\\s*INSERT\\s+INTO\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val updateTablePattern = Pattern.compile("^\\s*UPDATE\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val deleteTablePattern = Pattern.compile("^\\s*DELETE\\s+FROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE)
    private val returningTailPattern = Pattern.compile("\\bRETURNING\\b[\\s\\S]*$", Pattern.CASE_INSENSITIVE)
    private val setPattern = Pattern.compile("\\bSET\\b", Pattern.CASE_INSENSITIVE)
    private val wherePattern = Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE)
    private val versionAssignmentPattern = Pattern.compile("\\bversion\\s*=", Pattern.CASE_INSENSITIVE)

    fun enrichMutationSqlIfNeeded(sql: String): String {
        val sanitizedSql = sql.trim()
        if (!isMutationSql(sanitizedSql)) {
            return sql
        }
        val noSemicolonSql = sanitizedSql.trimEnd(';')
        val withoutReturning = returningTailPattern.matcher(noSemicolonSql).replaceFirst("").trimEnd()
        val withVersionUpdate = addVersionIncrementIfNeeded(withoutReturning)
        return "$withVersionUpdate RETURNING *"
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

    private fun addVersionIncrementIfNeeded(sql: String): String {
        val mutationMeta = mutationMetaOrNull(sql) ?: return sql
        if (mutationMeta.operation != "UPDATE") {
            return sql
        }
        if (versionAssignmentPattern.matcher(sql).find()) {
            return sql
        }

        val whereMatcher = wherePattern.matcher(sql)
        if (whereMatcher.find()) {
            val whereStart = whereMatcher.start()
            return buildString {
                append(sql.substring(0, whereStart).trimEnd())
                append(", version = COALESCE(version, 0) + 1 ")
                append(sql.substring(whereStart))
            }
        }
        if (setPattern.matcher(sql).find()) {
            return "${sql.trimEnd()}, version = COALESCE(version, 0) + 1"
        }
        return sql
    }

    data class MutationMeta(
        val tableName: String,
        val operation: String
    )
}
