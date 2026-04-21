package com.applied.replication.master.schema

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
class PrimaryKeyPresenceStartupValidator(
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
                FROM information_schema.table_constraints tc
                WHERE tc.table_schema = t.table_schema
                  AND tc.table_name = t.table_name
                  AND tc.constraint_type = 'PRIMARY KEY'
              )
            ORDER BY t.table_name
            """.trimIndent(),
            String::class.java
        )
        require(missing.isEmpty()) {
            "Each table must have a PRIMARY KEY. Missing in: ${missing.joinToString(", ")}"
        }
    }
}
