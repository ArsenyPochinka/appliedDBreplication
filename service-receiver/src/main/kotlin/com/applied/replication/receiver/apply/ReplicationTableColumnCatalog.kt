package com.applied.replication.receiver.apply

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component

@Component
class ReplicationTableColumnCatalog(
    private val jdbcTemplate: NamedParameterJdbcTemplate
) {

    fun columnsOrdered(tableName: String): List<ReplicationColumnMeta> {
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
            ReplicationColumnMeta(
                name = rs.getString("column_name"),
                dataType = rs.getString("data_type"),
                udtName = rs.getString("udt_name") ?: ""
            )
        }
    }
}
