package com.applied.replication.master.jdbc

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class VersionColumnStartupValidator(
    private val jdbcTemplate: JdbcTemplate
) : ApplicationRunner {

    override fun run(args: ApplicationArguments) {
        val missing = jdbcTemplate.queryForList(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema = current_schema()
              AND t.table_type = 'BASE TABLE'
              AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns c
                WHERE c.table_schema = t.table_schema
                  AND c.table_name = t.table_name
                  AND c.column_name = 'version'
                  AND c.data_type = 'integer'
              )
            ORDER BY t.table_name
            """.trimIndent(),
            String::class.java
        )
        require(missing.isEmpty()) {
            "Each table must contain INTEGER column 'version'. Missing in: ${missing.joinToString(", ")}"
        }
    }
}
