package com.applied.replication.receiver.schema

import jakarta.annotation.PostConstruct
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component

/**
 * One-shot load of primary key column names per table (current schema) at receiver startup.
 */
@Component
class ReplicationPrimaryKeyCache(
    private val jdbcTemplate: NamedParameterJdbcTemplate
) {

    private lateinit var pkByTable: Map<String, List<String>>

    @PostConstruct
    fun load() {
        val ordered = jdbcTemplate.query(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = current_schema()
            ORDER BY tc.table_name, kcu.ordinal_position
            """.trimIndent(),
            emptyMap<String, Any>()
        ) { rs, _ ->
            rs.getString("table_name") to rs.getString("column_name")
        }
        val acc = LinkedHashMap<String, MutableList<String>>()
        for ((table, column) in ordered) {
            acc.getOrPut(table) { mutableListOf() }.add(column)
        }
        pkByTable = acc.mapValues { it.value.toList() }
    }

    fun primaryKeysForTable(tableName: String): List<String> =
        pkByTable[tableName] ?: emptyList()
}
