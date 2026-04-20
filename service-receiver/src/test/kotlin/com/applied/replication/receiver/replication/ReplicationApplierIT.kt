package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import com.applied.replication.receiver.ReceiverReplicationTestApplication
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

@SpringBootTest(classes = [ReceiverReplicationTestApplication::class])
@ActiveProfiles("receiver-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplicationApplierIT {

    @Autowired
    private lateinit var applier: ReplicationApplier

    @Autowired
    private lateinit var jdbc: NamedParameterJdbcTemplate

    @Autowired
    private lateinit var primaryKeyCache: ReplicationPrimaryKeyCache

    private val objectMapper = ObjectMapper()

    @BeforeEach
    fun cleanTables() {
        jdbc.update("TRUNCATE recv_applier_child, recv_applier_t1 CASCADE", emptyMap<String, Any>())
    }

    @Nested
    inner class PrimaryKeyCacheTests {
        @Test
        fun `cache exposes PK columns for test tables in key order`() {
            assertEquals(listOf("id"), primaryKeyCache.primaryKeysForTable("recv_applier_t1"))
            assertEquals(listOf("id"), primaryKeyCache.primaryKeysForTable("recv_applier_child"))
        }

        @Test
        fun `unknown table returns empty PK list`() {
            assertTrue(primaryKeyCache.primaryKeysForTable("recv_does_not_exist").isEmpty())
        }
    }

    @Nested
    inner class PayloadValidationTests {
        @Test
        fun `apply fails when version field is missing`() {
            val payload = objectMapper.createObjectNode().put("id", 1L).put("email", "a@b.c").put("name", "n")
            val ex = assertFailsWith<IllegalArgumentException> {
                applier.apply(msg("INSERT", "recv_applier_t1", payload))
            }
            assertTrue(ex.message!!.contains("version"))
        }

        @Test
        fun `apply fails when table has no PK in cache`() {
            val payload = rowT1(1L, "x@y.z", "n", 0)
            val ex = assertFailsWith<IllegalStateException> {
                applier.apply(msg("INSERT", "recv_does_not_exist", payload))
            }
            assertTrue(ex.message!!.contains("no primary key"))
        }

        @Test
        fun `unsupported operation throws`() {
            assertFailsWith<IllegalArgumentException> {
                applier.apply(msg("MERGE", "recv_applier_t1", rowT1(1L, "m@example.com", "n", 0)))
            }
        }
    }

    @Nested
    inner class UpsertTests {
        @Test
        fun `insert applies new row`() {
            val r = applier.apply(msg("INSERT", "recv_applier_t1", rowT1(1L, "u1@example.com", "n1", 0)))
            assertEquals(ReplicationApplier.ApplyResult.APPLIED, r)
            assertEquals(1, countT1())
            assertEquals(0, jdbc.queryForObject("SELECT version FROM recv_applier_t1 WHERE id = 1", emptyMap<String, Any>(), Int::class.java))
        }

        @Test
        fun `duplicate unique email yields skipped without error`() {
            assertEquals(ReplicationApplier.ApplyResult.APPLIED, applier.apply(msg("INSERT", "recv_applier_t1", rowT1(1L, "dup@example.com", "a", 0))))
            val r = applier.apply(msg("INSERT", "recv_applier_t1", rowT1(2L, "dup@example.com", "b", 0)))
            assertEquals(ReplicationApplier.ApplyResult.SKIPPED, r)
            assertEquals(1, countT1())
            assertEquals(1L, jdbc.queryForObject("SELECT id FROM recv_applier_t1 WHERE email = 'dup@example.com'", emptyMap<String, Any>(), Long::class.java))
        }

        @Test
        fun `foreign key violation yields requeue`() {
            val payload = objectMapper.createObjectNode()
                .put("id", 10L)
                .put("parent_id", 999L)
                .put("title", "orphan")
                .put("version", 0)
            val r = applier.apply(msg("INSERT", "recv_applier_child", payload))
            assertEquals(ReplicationApplier.ApplyResult.REQUEUE, r)
            assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM recv_applier_child WHERE id = 10", emptyMap<String, Any>(), Int::class.java))
        }

        @Test
        fun `not null violation yields requeue`() {
            val payload = objectMapper.createObjectNode()
                .put("id", 3L)
                .put("email", "nn@example.com")
                .putNull("name")
                .put("version", 0)
            val r = applier.apply(msg("INSERT", "recv_applier_t1", payload))
            assertEquals(ReplicationApplier.ApplyResult.REQUEUE, r)
            assertEquals(0, countT1ById(3L))
        }

        @Test
        fun `stale version on conflict path yields skipped`() {
            jdbc.update(
                "INSERT INTO recv_applier_t1 (id, email, name, version) VALUES (5, 'v@example.com', 'x', 4)",
                emptyMap<String, Any>()
            )
            val r = applier.apply(msg("UPDATE", "recv_applier_t1", rowT1(5L, "v@example.com", "y", 4)))
            assertEquals(ReplicationApplier.ApplyResult.SKIPPED, r)
            assertEquals("x", jdbc.queryForObject("SELECT name FROM recv_applier_t1 WHERE id = 5", emptyMap<String, Any>(), String::class.java))
        }

        @Test
        fun `next version applies on conflict`() {
            jdbc.update(
                "INSERT INTO recv_applier_t1 (id, email, name, version) VALUES (6, 'w@example.com', 'x', 2)",
                emptyMap<String, Any>()
            )
            val r = applier.apply(msg("UPDATE", "recv_applier_t1", rowT1(6L, "w@example.com", "z", 3)))
            assertEquals(ReplicationApplier.ApplyResult.APPLIED, r)
            assertEquals("z", jdbc.queryForObject("SELECT name FROM recv_applier_t1 WHERE id = 6", emptyMap<String, Any>(), String::class.java))
            assertEquals(3, jdbc.queryForObject("SELECT version FROM recv_applier_t1 WHERE id = 6", emptyMap<String, Any>(), Int::class.java))
        }

        @Test
        fun `update when row missing behaves as insert`() {
            val r = applier.apply(msg("UPDATE", "recv_applier_t1", rowT1(7L, "new@example.com", "fresh", 0)))
            assertEquals(ReplicationApplier.ApplyResult.APPLIED, r)
            assertEquals(1, countT1ById(7L))
        }

        @Test
        fun `gap in version sequence yields skipped`() {
            jdbc.update(
                "INSERT INTO recv_applier_t1 (id, email, name, version) VALUES (8, 'g@example.com', 'x', 1)",
                emptyMap<String, Any>()
            )
            val r = applier.apply(msg("UPDATE", "recv_applier_t1", rowT1(8L, "g@example.com", "y", 5)))
            assertEquals(ReplicationApplier.ApplyResult.SKIPPED, r)
            assertEquals(1, jdbc.queryForObject("SELECT version FROM recv_applier_t1 WHERE id = 8", emptyMap<String, Any>(), Int::class.java))
        }

        @Test
        fun `child insert applies when parent exists`() {
            assertEquals(
                ReplicationApplier.ApplyResult.APPLIED,
                applier.apply(msg("INSERT", "recv_applier_t1", rowT1(100L, "parent@example.com", "p", 0)))
            )
            val childPayload = objectMapper.createObjectNode()
                .put("id", 200L)
                .put("parent_id", 100L)
                .put("title", "child-row")
                .put("version", 0)
            assertEquals(
                ReplicationApplier.ApplyResult.APPLIED,
                applier.apply(msg("INSERT", "recv_applier_child", childPayload))
            )
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM recv_applier_child WHERE id = 200", emptyMap<String, Any>(), Int::class.java))
        }
    }

    @Nested
    inner class DeleteTests {
        @Test
        fun `delete applies when version matches next`() {
            jdbc.update(
                "INSERT INTO recv_applier_t1 (id, email, name, version) VALUES (20, 'd@example.com', 'x', 2)",
                emptyMap<String, Any>()
            )
            val payload = objectMapper.createObjectNode()
                .put("id", 20L)
                .put("email", "d@example.com")
                .put("name", "x")
                .put("version", 3)
            val r = applier.apply(msg("DELETE", "recv_applier_t1", payload))
            assertEquals(ReplicationApplier.ApplyResult.APPLIED, r)
            assertEquals(0, countT1ById(20L))
        }

        @Test
        fun `delete skipped when row missing`() {
            val payload = objectMapper.createObjectNode()
                .put("id", 21L)
                .put("email", "missing@example.com")
                .put("name", "x")
                .put("version", 1)
            val r = applier.apply(msg("DELETE", "recv_applier_t1", payload))
            assertEquals(ReplicationApplier.ApplyResult.SKIPPED, r)
        }

        @Test
        fun `delete requeues when version is not next`() {
            jdbc.update(
                "INSERT INTO recv_applier_t1 (id, email, name, version) VALUES (22, 'rq@example.com', 'x', 1)",
                emptyMap<String, Any>()
            )
            val payload = objectMapper.createObjectNode()
                .put("id", 22L)
                .put("email", "rq@example.com")
                .put("name", "x")
                .put("version", 5)
            val r = applier.apply(msg("DELETE", "recv_applier_t1", payload))
            assertEquals(ReplicationApplier.ApplyResult.REQUEUE, r)
            assertEquals(1, countT1ById(22L))
        }

        @Test
        fun `delete fails when PK field missing in payload`() {
            val payload = objectMapper.createObjectNode().put("version", 2)
            assertFailsWith<IllegalArgumentException> {
                applier.apply(msg("DELETE", "recv_applier_t1", payload))
            }
        }
    }

    private fun msg(operation: String, table: String, payload: JsonNode) =
        ReplicationMessage(eventId = 1L, commitId = 1L, tableName = table, operation = operation, payload = payload)

    private fun rowT1(id: Long, email: String, name: String, version: Int): JsonNode =
        objectMapper.createObjectNode()
            .put("id", id)
            .put("email", email)
            .put("name", name)
            .put("version", version)

    private fun countT1(): Int =
        jdbc.queryForObject("SELECT COUNT(*) FROM recv_applier_t1", emptyMap<String, Any>(), Int::class.java)!!

    private fun countT1ById(id: Long): Int =
        jdbc.queryForObject(
            "SELECT COUNT(*) FROM recv_applier_t1 WHERE id = :id",
            mapOf("id" to id),
            Int::class.java
        )!!

    companion object {
        private val jdbcUrl = System.getProperty("test.receiver.jdbc-url")
            ?: System.getenv("TEST_RECEIVER_JDBC_URL")
            ?: System.getenv("TEST_SLAVE_JDBC_URL")
            ?: "jdbc:postgresql://localhost:5434/slave"

        private val jdbcUser = System.getProperty("test.receiver.username")
            ?: System.getenv("TEST_RECEIVER_DB_USERNAME")
            ?: System.getenv("TEST_SLAVE_DB_USERNAME")
            ?: "slave"
        private val jdbcPassword = System.getProperty("test.receiver.password")
            ?: System.getenv("TEST_RECEIVER_DB_PASSWORD")
            ?: System.getenv("TEST_SLAVE_DB_PASSWORD")
            ?: "slave"

        @JvmStatic
        @DynamicPropertySource
        fun datasourceProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { jdbcUrl }
            registry.add("spring.datasource.username") { jdbcUser }
            registry.add("spring.datasource.password") { jdbcPassword }
        }
    }
}
