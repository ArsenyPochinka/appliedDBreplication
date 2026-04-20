package com.applied.replication.master.jdbc

import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class ReturningMutationJdbcExecutorTest {

    private val executor = ReturningMutationJdbcExecutor()

    @Test
    fun `insert gets RETURNING star without version rewrite`() {
        val sql = "INSERT INTO t(a,b) VALUES (1,2)"
        val out = executor.enrichMutationSqlIfNeeded(sql)
        assertTrue(out.contains("RETURNING *", ignoreCase = true))
        assertFalse(out.contains("version = COALESCE", ignoreCase = true))
    }

    @Test
    fun `update injects version increment before WHERE`() {
        val sql = "UPDATE my_table SET a = 1 WHERE id = 2"
        val out = executor.enrichMutationSqlIfNeeded(sql)
        assertTrue(out.contains("RETURNING *", ignoreCase = true))
        assertTrue(out.contains("version = COALESCE(version, 0) + 1", ignoreCase = true))
        assertTrue(out.contains("WHERE id = 2", ignoreCase = true))
    }

    @Test
    fun `update does not inject version when assignment already present`() {
        val sql = "UPDATE my_table SET version = 5 WHERE id = 2"
        val out = executor.enrichMutationSqlIfNeeded(sql)
        val versionAssignCount = Regex("version\\s*=", RegexOption.IGNORE_CASE).findAll(out).count()
        assertEquals(1, versionAssignCount)
    }

    @Test
    fun `replaces existing RETURNING clause`() {
        val sql = "UPDATE t SET a=1 WHERE id=2 RETURNING id"
        val out = executor.enrichMutationSqlIfNeeded(sql)
        assertTrue(out.endsWith("RETURNING *", ignoreCase = true))
        assertFalse(out.contains("RETURNING id", ignoreCase = true))
    }

    @Test
    fun `delete gets RETURNING star`() {
        val sql = "DELETE FROM t WHERE id = 1"
        val out = executor.enrichMutationSqlIfNeeded(sql)
        assertTrue(out.contains("RETURNING *", ignoreCase = true))
    }

    @Test
    fun `non mutation sql is unchanged`() {
        val sql = "SELECT * FROM t"
        assertEquals(sql, executor.enrichMutationSqlIfNeeded(sql))
    }

    @Test
    fun `mutation meta parses table and operation`() {
        val insertMeta = executor.mutationMetaOrNull("INSERT INTO sch.tt (a) VALUES (1)")!!
        assertEquals("tt", insertMeta.tableName)
        assertEquals("INSERT", insertMeta.operation)
        assertEquals("u", executor.mutationMetaOrNull("UPDATE u SET a=1")!!.tableName)
        assertEquals("d", executor.mutationMetaOrNull("DELETE FROM d WHERE 1=1")!!.tableName)
        assertNull(executor.mutationMetaOrNull("SELECT 1"))
    }
}
