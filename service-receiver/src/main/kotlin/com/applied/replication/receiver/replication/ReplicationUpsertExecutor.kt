package com.applied.replication.receiver.replication

import java.sql.SQLException
import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional

/**
 * Runs upsert SQL in a short independent transaction so PostgreSQL errors (aborted xact)
 * do not poison the outer consumer flow when we map them to skip/requeue.
 */
@Component
class ReplicationUpsertExecutor(
    private val jdbcTemplate: NamedParameterJdbcTemplate
) {

    enum class UpsertOutcome {
        APPLIED,
        /** Version gate / no row updated (not an error). */
        SKIPPED,
        /** Unique violation (23505): duplicate, do not requeue. */
        SKIPPED_DUPLICATE,
        /** Any other DB error on insert/update path: requeue to Kafka. */
        REQUEUE
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    fun execute(sql: String, params: MapSqlParameterSource): UpsertOutcome {
        return try {
            val applied = jdbcTemplate.queryForObject(sql, params, Int::class.java) ?: 0
            if (applied > 0) UpsertOutcome.APPLIED else UpsertOutcome.SKIPPED
        } catch (ex: DataAccessException) {
            if (isPostgresUniqueViolation(ex)) {
                UpsertOutcome.SKIPPED_DUPLICATE
            } else {
                UpsertOutcome.REQUEUE
            }
        }
    }

    private fun isPostgresUniqueViolation(ex: Throwable): Boolean {
        if (ex is DuplicateKeyException) {
            return true
        }
        var current: Throwable? = ex
        while (current != null) {
            if (current is SQLException && "23505" == current.sqlState) {
                return true
            }
            current = current.cause
        }
        return false
    }
}
