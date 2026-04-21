package com.applied.replication.receiver.apply

private val identifierRegex = Regex("^[A-Za-z_][A-Za-z0-9_]*$")

object ReplicationSqlGuards {

    fun sanitizeIdentifier(name: String): String {
        require(identifierRegex.matches(name)) { "Unsafe SQL identifier '$name'." }
        return name
    }

    fun quoteIdentifier(name: String): String {
        return "\"${name.replace("\"", "\"\"")}\""
    }

    fun requireSafeColumnName(columnName: String) {
        require(identifierRegex.matches(columnName)) { "Unsafe column name '$columnName'." }
    }
}
