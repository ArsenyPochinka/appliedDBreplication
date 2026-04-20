package com.applied.replication.master.integration

import com.applied.replication.common.ReplicationMessage
import com.applied.replication.master.MasterApplication
import com.applied.replication.receiver.ReceiverApplication
import com.applied.replication.receiver.replication.ReplicationApplier
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.transaction.TestTransaction
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.sql.Connection
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import javax.sql.DataSource

@SpringBootTest(classes = [MasterApplication::class])
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndReplicationIT {

    @Autowired
    private lateinit var namedJdbc: NamedParameterJdbcTemplate

    @Autowired
    private lateinit var plainJdbc: JdbcTemplate

    @Autowired
    private lateinit var dataSource: DataSource

    @Autowired
    private lateinit var transactionTemplate: TransactionTemplate

    @Autowired
    private lateinit var entityManager: EntityManager

    private lateinit var masterJdbc: JdbcTemplate
    private lateinit var slaveJdbc: JdbcTemplate
    private lateinit var receiverContext: ConfigurableApplicationContext
    private lateinit var receiverApplier: ReplicationApplier
    private val objectMapper = ObjectMapper()

    @BeforeAll
    fun beforeAll() {
        migrate(masterJdbcUrl, masterDbUsername, masterDbPassword, "db/changelog/master-test.yaml")
        migrate(slaveJdbcUrl, slaveDbUsername, slaveDbPassword, "db/changelog/slave-test.yaml")

        masterJdbc = JdbcTemplate(DriverManagerDataSource(masterJdbcUrl, masterDbUsername, masterDbPassword))
        slaveJdbc = JdbcTemplate(DriverManagerDataSource(slaveJdbcUrl, slaveDbUsername, slaveDbPassword))

        receiverContext = SpringApplicationBuilder(ReceiverApplication::class.java)
            .properties(
                mapOf(
                    "server.port" to "0",
                    "spring.main.web-application-type" to "none",
                    "spring.liquibase.enabled" to "false",
                    "spring.autoconfigure.exclude" to "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration",
                    "spring.datasource.url" to slaveJdbcUrl,
                    "spring.datasource.username" to slaveDbUsername,
                    "spring.datasource.password" to slaveDbPassword,
                    "spring.kafka.bootstrap-servers" to kafkaBootstrapServers,
                    "spring.kafka.consumer.bootstrap-servers" to kafkaBootstrapServers,
                    "spring.kafka.consumer.group-id" to "service-receiver-test",
                    "spring.kafka.consumer.auto-offset-reset" to "earliest",
                    "app.replication.topic" to "db-replication-events",
                    "app.replication.max-requeue-iterations" to "10"
                )
            )
            .run()
        receiverApplier = receiverContext.getBean(ReplicationApplier::class.java)
    }

    @AfterAll
    fun afterAll() {
        if (this::receiverContext.isInitialized) {
            receiverContext.close()
        }
    }

    @BeforeEach
    fun beforeEach() {
        cleanupDb(masterJdbc)
        cleanupDb(slaveJdbc)
    }

    @Nested
    inner class InsertTests {
        @ParameterizedTest
        @EnumSource(MasterClient::class)
        fun `insert replicated for each db access strategy`(client: MasterClient) {
            val table1Col2 = "ins_t1_${client.name.lowercase()}_${System.nanoTime()}"
            val table2Col5 = "ins_t2_${client.name.lowercase()}_${System.nanoTime()}@example.com"
            transactionTemplate.executeWithoutResult {
                val table1Id = insertTable1(client, table1Col2)
                insertTable2(
                    client,
                    table1Id,
                    table2Col5,
                    "ins_${client.name.lowercase()}"
                )
            }
            awaitReplicationConsistency()
        }
    }

    @Nested
    inner class UpdateTests {
        @ParameterizedTest
        @EnumSource(MasterClient::class)
        fun `update replicated for each db access strategy`(client: MasterClient) {
            val table1Id = seedTable1("seed_update_t1_${client.name.lowercase()}")
            val table2Id = seedTable2(table1Id, "seed_update_t2_${client.name.lowercase()}@example.com")

            transactionTemplate.executeWithoutResult {
                updateTable1(client, table1Id, "upd_t1_${client.name.lowercase()}_${System.nanoTime()}")
                updateTable2(client, table2Id, "upd_${client.name.lowercase()}")
            }
            awaitReplicationConsistency()
            kotlin.test.assertEquals(1, masterJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, table1Id))
            kotlin.test.assertEquals(1, slaveJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, table1Id))
        }
    }

    @Nested
    inner class VersioningAndOrderingTests {
        @Test
        fun `insert keeps version zero on both sides`() {
            val id = insertTable1(MasterClient.JDBC_TEMPLATE, "ver_ins_${System.nanoTime()}")
            awaitReplicationConsistency()
            kotlin.test.assertEquals(0, masterJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, id))
            kotlin.test.assertEquals(0, slaveJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, id))
        }

        @Test
        fun `receiver skips stale message and applies only next version`() {
            val id = nextTable1Id()
            slaveJdbc.update(
                "INSERT INTO table_1(col_1, col_2, col_3, version) VALUES (?, ?, now(), ?)",
                id, "slave_version_$id", 2
            )
            val stalePayload = objectMapper.readTree("""{"col_1":$id,"col_2":"stale","col_3":"2026-01-01T00:00:00","version":2}""")
            val staleResult = receiverApplier.apply(
                ReplicationMessage(1, 111, "table_1", "UPDATE", stalePayload)
            )
            kotlin.test.assertEquals(ReplicationApplier.ApplyResult.SKIPPED, staleResult)

            val nextPayload = objectMapper.readTree("""{"col_1":$id,"col_2":"fresh","col_3":"2026-01-01T00:00:00","version":3}""")
            val nextResult = receiverApplier.apply(
                ReplicationMessage(2, 112, "table_1", "UPDATE", nextPayload)
            )
            kotlin.test.assertEquals(ReplicationApplier.ApplyResult.APPLIED, nextResult)
            kotlin.test.assertEquals(3, slaveJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, id))
        }

        @Test
        fun `receiver applies update as insert when row does not exist`() {
            val id = nextTable1Id()
            val payload = objectMapper.readTree("""{"col_1":$id,"col_2":"missing_inserted","col_3":"2026-01-01T00:00:00","version":0}""")
            val result = receiverApplier.apply(
                ReplicationMessage(3, 113, "table_1", "UPDATE", payload)
            )
            kotlin.test.assertEquals(ReplicationApplier.ApplyResult.APPLIED, result)
            kotlin.test.assertEquals(0, slaveJdbc.queryForObject("SELECT version FROM table_1 WHERE col_1 = ?", Int::class.java, id))
        }

        @Test
        fun `receiver marks delete for requeue when version is not next`() {
            val id = nextTable1Id()
            slaveJdbc.update(
                "INSERT INTO table_1(col_1, col_2, col_3, version) VALUES (?, ?, now(), ?)",
                id, "requeue_delete_$id", 2
            )
            val payload = objectMapper.readTree("""{"col_1":$id,"col_2":"requeue_delete_$id","col_3":"2026-01-01T00:00:00","version":6}""")
            val result = receiverApplier.apply(
                ReplicationMessage(4, 114, "table_1", "DELETE", payload)
            )
            kotlin.test.assertEquals(ReplicationApplier.ApplyResult.REQUEUE, result)
        }
    }

    @Nested
    inner class DeleteTests {
        @ParameterizedTest
        @EnumSource(MasterClient::class)
        fun `delete replicated for each db access strategy`(client: MasterClient) {
            val table1Id = seedTable1("seed_delete_t1_${client.name.lowercase()}")
            val table2Id = seedTable2(table1Id, "seed_delete_t2_${client.name.lowercase()}@example.com")

            transactionTemplate.executeWithoutResult {
                deleteTable2(client, table2Id)
                deleteTable1(client, table1Id)
            }
            awaitReplicationConsistency()
        }
    }

    @Nested
    inner class SingleTransactionMultiInsertTests {
        @Test
        fun `replicates multiple inserts across two tables in one transaction`() {
            transactionTemplate.executeWithoutResult {
                val table1IdA = insertTable1(MasterClient.NAMED_JDBC, "tx_multi_t1_a_${System.nanoTime()}")
                val table1IdB = insertTable1(MasterClient.JDBC_TEMPLATE, "tx_multi_t1_b_${System.nanoTime()}")
                insertTable2(MasterClient.RAW_JDBC, table1IdA, "tx_multi_t2_a_${System.nanoTime()}@example.com", "tx_a")
                insertTable2(MasterClient.JPA_NATIVE, table1IdB, "tx_multi_t2_b_${System.nanoTime()}@example.com", "tx_b")
            }
            awaitReplicationConsistency()
        }

        @Test
        fun `replicates multi-row inserts in one transaction`() {
            transactionTemplate.executeWithoutResult {
                val t1Id1 = nextTable1Id()
                val t1Id2 = nextTable1Id()
                plainJdbc.update(
                    """
                    INSERT INTO table_1(col_1, col_2, col_3)
                    VALUES (?, ?, now()), (?, ?, now())
                    """.trimIndent(),
                    t1Id1, "tx_bulk_t1_a_${System.nanoTime()}",
                    t1Id2, "tx_bulk_t1_b_${System.nanoTime()}"
                )

                val t2Id1 = nextTable2Id()
                val t2Id2 = nextTable2Id()
                plainJdbc.update(
                    """
                    INSERT INTO table_2(
                        col_1, col_2, col_3, col_4, col_5, col_6, col_7,
                        col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15
                    )
                    VALUES
                        (?, ?, 'bulk3_a', 'bulk4_a', ?, 500.10, true, DATE '1999-01-01', TIME '09:00:00', now(), '{"bulk":"a"}'::jsonb, ARRAY['a','b'], 11.1, now(), now()),
                        (?, ?, 'bulk3_b', 'bulk4_b', ?, 600.20, false, DATE '1998-02-02', TIME '10:30:00', now(), '{"bulk":"b"}'::jsonb, ARRAY['c','d'], 22.2, now(), now())
                    """.trimIndent(),
                    t2Id1, t1Id1, "tx_bulk_t2_a_${System.nanoTime()}@example.com",
                    t2Id2, t1Id2, "tx_bulk_t2_b_${System.nanoTime()}@example.com"
                )
            }
            awaitReplicationConsistency()
        }
    }

    @Nested
    open inner class TransactionalAnnotationTests {
        @Test
        @Transactional
        open fun `replicates inserts and updates across multiple rows and tables in one annotated transaction`() {
            val t1Id1 = nextTable1Id()
            val t1Id2 = nextTable1Id()
            val t2Id1 = nextTable2Id()
            val t2Id2 = nextTable2Id()
            val uniq = System.nanoTime()

            plainJdbc.update(
                """
                INSERT INTO table_1(col_1, col_2, col_3)
                VALUES (?, ?, now()), (?, ?, now())
                """.trimIndent(),
                t1Id1, "tx_ann_t1_a_$uniq",
                t1Id2, "tx_ann_t1_b_$uniq"
            )

            namedJdbc.update(
                """
                INSERT INTO table_2(
                    col_1, col_2, col_3, col_4, col_5, col_6, col_7,
                    col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15
                )
                VALUES
                    (:id1, :fk1, 'ann3_a', 'ann4_a', :email1, 700.10, true, DATE '1997-07-07', TIME '07:00:00', now(), CAST(:meta1 AS jsonb), ARRAY['ann','a'], 33.3, now(), now()),
                    (:id2, :fk2, 'ann3_b', 'ann4_b', :email2, 800.20, false, DATE '1996-06-06', TIME '06:30:00', now(), CAST(:meta2 AS jsonb), ARRAY['ann','b'], 44.4, now(), now())
                """.trimIndent(),
                mapOf(
                    "id1" to t2Id1,
                    "id2" to t2Id2,
                    "fk1" to t1Id1,
                    "fk2" to t1Id2,
                    "email1" to "tx_ann_t2_a_$uniq@example.com",
                    "email2" to "tx_ann_t2_b_$uniq@example.com",
                    "meta1" to """{"tx":"annotation-a"}""",
                    "meta2" to """{"tx":"annotation-b"}"""
                )
            )

            plainJdbc.update(
                """
                UPDATE table_1
                SET col_2 = CONCAT(col_2, '_upd')
                WHERE col_1 IN (?, ?)
                """.trimIndent(),
                t1Id1, t1Id2
            )

            namedJdbc.update(
                """
                UPDATE table_2
                SET col_3 = :c3, col_4 = :c4, col_6 = :c6, col_15 = now()
                WHERE col_1 = :id
                """.trimIndent(),
                mapOf("id" to t2Id1, "c3" to "ann3_a_upd", "c4" to "ann4_a_upd", "c6" to BigDecimal("999.99"))
            )

            rawExecute(
                """
                UPDATE table_2
                SET col_3 = ?, col_4 = ?, col_6 = 555.55, col_15 = now()
                WHERE col_1 = ?
                """.trimIndent()
            ) {
                it.setString(1, "ann3_b_upd")
                it.setString(2, "ann4_b_upd")
                it.setLong(3, t2Id2)
            }

            TestTransaction.flagForCommit()
            TestTransaction.end()

            awaitReplicationConsistency()
        }
    }

    private fun insertTable1(client: MasterClient, col2: String): Long {
        return when (client) {
            MasterClient.NAMED_JDBC -> {
                val id = nextTable1Id()
                namedJdbc.update(
                    "INSERT INTO table_1(col_1, col_2, col_3) VALUES (:id, :col2, now())",
                    mapOf("id" to id, "col2" to col2)
                )
                id
            }
            MasterClient.JDBC_TEMPLATE -> {
                val id = nextTable1Id()
                plainJdbc.update(
                    "INSERT INTO table_1(col_1, col_2, col_3) VALUES (?, ?, now())",
                    id,
                    col2
                )
                id
            }
            MasterClient.RAW_JDBC -> {
                val id = nextTable1Id()
                rawExecute("INSERT INTO table_1(col_1, col_2, col_3) VALUES (?, ?, now())") {
                    it.setLong(1, id)
                    it.setString(2, col2)
                }
                id
            }
            MasterClient.JPA_NATIVE -> {
                val id = nextTable1Id()
                entityManager.createNativeQuery(
                    "INSERT INTO table_1(col_1, col_2, col_3) VALUES (?1, ?2, now())"
                ).setParameter(1, id)
                    .setParameter(2, col2)
                    .executeUpdate()
                id
            }
            MasterClient.JPA_ENTITY -> {
                val id = nextTable1Id()
                entityManager.persist(
                    Table1Entity(
                        col1 = id,
                        col2 = col2,
                        col3 = LocalDateTime.now()
                    )
                )
                id
            }
        }
    }

    private fun insertTable2(client: MasterClient, table1Id: Long, col5: String, token: String) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update(
                """
                INSERT INTO table_2(col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
                VALUES (:col2, :col3, :col4, :col5, :col6, :col7, :col8, :col9, :col10, CAST(:col11 AS jsonb), :col12, :col13, now(), now())
                """.trimIndent(),
                mapOf(
                    "col2" to table1Id,
                    "col3" to "i3_$token",
                    "col4" to "i4_$token",
                    "col5" to col5,
                    "col6" to BigDecimal("123.45"),
                    "col7" to true,
                    "col8" to LocalDate.of(1993, 3, 3),
                    "col9" to LocalTime.of(10, 0),
                    "col10" to LocalDateTime.now(),
                    "col11" to """{"source":"named"}""",
                    "col12" to arrayOf("x", "y"),
                    "col13" to 10.1
                )
            )
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update(
                """
                INSERT INTO table_2(col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
                VALUES (?, ?, ?, ?, 123.45, true, DATE '1993-03-03', TIME '10:00:00', now(), '{"source":"jdbc"}'::jsonb, ARRAY['x','y'], 10.1, now(), now())
                """.trimIndent(),
                table1Id,
                "i3_$token",
                "i4_$token",
                col5
            )
            MasterClient.RAW_JDBC -> rawExecute(
                """
                INSERT INTO table_2(col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
                VALUES (?, ?, ?, ?, 123.45, true, DATE '1993-03-03', TIME '10:00:00', now(), '{"source":"raw"}'::jsonb, ARRAY['x','y'], 10.1, now(), now())
                """.trimIndent()
            ) {
                it.setLong(1, table1Id)
                it.setString(2, "i3_$token")
                it.setString(3, "i4_$token")
                it.setString(4, col5)
            }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery(
                """
                INSERT INTO table_2(col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
                VALUES (?1, ?2, ?3, ?4, 123.45, true, DATE '1993-03-03', TIME '10:00:00', now(), '{"source":"jpa-native"}'::jsonb, ARRAY['x','y'], 10.1, now(), now())
                """.trimIndent()
            ).setParameter(1, table1Id)
                .setParameter(2, "i3_$token")
                .setParameter(3, "i4_$token")
                .setParameter(4, col5)
                .executeUpdate()
            MasterClient.JPA_ENTITY -> entityManager.persist(
                Table2Entity(
                    col1 = nextTable2Id(),
                    col2 = table1Id,
                    col3 = "i3_$token",
                    col4 = "i4_$token",
                    col5 = col5,
                    col6 = BigDecimal("123.45"),
                    col7 = true,
                    col8 = LocalDate.of(1993, 3, 3),
                    col9 = LocalTime.of(10, 0),
                    col10 = LocalDateTime.now(),
                    col13 = 10.1,
                    col14 = LocalDateTime.now(),
                    col15 = LocalDateTime.now()
                )
            )
        }
    }

    private fun updateTable1(client: MasterClient, id: Long, newCol2: String) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update(
                "UPDATE table_1 SET col_2=:col2 WHERE col_1=:id",
                mapOf("col2" to newCol2, "id" to id)
            )
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update(
                "UPDATE table_1 SET col_2=? WHERE col_1=?",
                newCol2,
                id
            )
            MasterClient.RAW_JDBC -> rawExecute("UPDATE table_1 SET col_2=? WHERE col_1=?") {
                it.setString(1, newCol2)
                it.setLong(2, id)
            }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery(
                "UPDATE table_1 SET col_2 = ?1 WHERE col_1 = ?2"
            ).setParameter(1, newCol2).setParameter(2, id).executeUpdate()
            MasterClient.JPA_ENTITY -> {
                val row = entityManager.find(Table1Entity::class.java, id) ?: return
                row.col2 = newCol2
            }
        }
    }

    private fun updateTable2(client: MasterClient, id: Long, token: String) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update(
                """
                UPDATE table_2
                SET col_3=:col3, col_4=:col4, col_6=:col6, col_7=:col7, col_13=:col13, col_15=now()
                WHERE col_1=:id
                """.trimIndent(),
                mapOf(
                    "id" to id,
                    "col3" to "u3_$token",
                    "col4" to "u4_$token",
                    "col6" to BigDecimal("777.77"),
                    "col7" to false,
                    "col13" to 99.9
                )
            )
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update(
                """
                UPDATE table_2
                SET col_3=?, col_4=?, col_6=777.77, col_7=false, col_13=99.9, col_15=now()
                WHERE col_1=?
                """.trimIndent(),
                "u3_$token",
                "u4_$token",
                id
            )
            MasterClient.RAW_JDBC -> rawExecute(
                """
                UPDATE table_2
                SET col_3=?, col_4=?, col_6=777.77, col_7=false, col_13=99.9, col_15=now()
                WHERE col_1=?
                """.trimIndent()
            ) {
                it.setString(1, "u3_$token")
                it.setString(2, "u4_$token")
                it.setLong(3, id)
            }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery(
                """
                UPDATE table_2
                SET col_3=?1, col_4=?2, col_6=777.77, col_7=false, col_13=99.9, col_15=now()
                WHERE col_1=?3
                """.trimIndent()
            ).setParameter(1, "u3_$token")
                .setParameter(2, "u4_$token")
                .setParameter(3, id)
                .executeUpdate()
            MasterClient.JPA_ENTITY -> {
                val row = entityManager.find(Table2Entity::class.java, id) ?: return
                row.col3 = "u3_$token"
                row.col4 = "u4_$token"
                row.col6 = BigDecimal("777.77")
                row.col7 = false
                row.col13 = 99.9
                row.col15 = LocalDateTime.now()
            }
        }
    }

    private fun deleteTable1(client: MasterClient, id: Long) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update("DELETE FROM table_1 WHERE col_1=:id", mapOf("id" to id))
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update("DELETE FROM table_1 WHERE col_1=?", id)
            MasterClient.RAW_JDBC -> rawExecute("DELETE FROM table_1 WHERE col_1=?") { it.setLong(1, id) }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery("DELETE FROM table_1 WHERE col_1 = ?1")
                .setParameter(1, id)
                .executeUpdate()
            MasterClient.JPA_ENTITY -> {
                val row = entityManager.find(Table1Entity::class.java, id) ?: return
                entityManager.remove(row)
            }
        }
    }

    private fun deleteTable2(client: MasterClient, id: Long) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update("DELETE FROM table_2 WHERE col_1=:id", mapOf("id" to id))
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update("DELETE FROM table_2 WHERE col_1=?", id)
            MasterClient.RAW_JDBC -> rawExecute("DELETE FROM table_2 WHERE col_1=?") { it.setLong(1, id) }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery("DELETE FROM table_2 WHERE col_1 = ?1")
                .setParameter(1, id)
                .executeUpdate()
            MasterClient.JPA_ENTITY -> {
                val row = entityManager.find(Table2Entity::class.java, id) ?: return
                entityManager.remove(row)
            }
        }
    }

    private fun seedTable1(col2: String): Long {
        val id = nextTable1Id()
        masterJdbc.update(
            "INSERT INTO table_1(col_1, col_2, col_3) VALUES (?, ?, now())",
            id,
            col2
        )
        return id
    }

    private fun seedTable2(table1Id: Long, col5: String): Long {
        val id = nextTable2Id()
        masterJdbc.update(
            """
            INSERT INTO table_2(col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
            VALUES (?, ?, 'seed3', 'seed4', ?, 111.11, true, DATE '2000-01-01', TIME '08:00:00', now(), '{"seed":true}'::jsonb, ARRAY['seed'], 1.1, now(), now())
            """.trimIndent(),
            id,
            table1Id,
            col5
        )
        return id
    }

    private fun findTable1IdByCol2(col2: String): Long {
        return masterJdbc.queryForObject(
            "SELECT col_1 FROM table_1 WHERE col_2 = ?",
            Long::class.java,
            col2
        )!!
    }

    private fun nextTable1Id(): Long {
        return (entityManager.createNativeQuery("SELECT nextval('table_1_col_1_seq')").singleResult as Number).toLong()
    }

    private fun nextTable2Id(): Long {
        return (entityManager.createNativeQuery("SELECT nextval('table_2_col_1_seq')").singleResult as Number).toLong()
    }

    private fun cleanupDb(jdbc: JdbcTemplate) {
        jdbc.execute("TRUNCATE TABLE table_2, table_1 RESTART IDENTITY CASCADE")
    }

    private fun rawExecute(sql: String, binder: (java.sql.PreparedStatement) -> Unit) {
        val connection: Connection = DataSourceUtils.getConnection(dataSource)
        try {
            connection.prepareStatement(sql).use { statement ->
                binder(statement)
                statement.executeUpdate()
            }
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource)
        }
    }


    private fun awaitReplicationConsistency() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted {
                kotlin.test.assertEquals(
                    normalizedRows(masterJdbc, "table_1"),
                    normalizedRows(slaveJdbc, "table_1")
                )
                kotlin.test.assertEquals(
                    normalizedRows(masterJdbc, "table_2"),
                    normalizedRows(slaveJdbc, "table_2")
                )
            }
    }

    private fun tableRows(jdbc: JdbcTemplate, table: String): List<Map<String, Any?>> {
        return jdbc.queryForList("SELECT * FROM $table ORDER BY col_1")
    }

    private fun normalizedRows(jdbc: JdbcTemplate, table: String): List<Map<String, Any?>> {
        return tableRows(jdbc, table).map { row ->
            row.toSortedMap().mapValues { (_, value) -> normalizeForAssertion(value) }
        }
    }

    private fun normalizeForAssertion(value: Any?): Any? {
        if (value == null) {
            return null
        }
        return when (value) {
            is java.sql.Timestamp,
            is java.sql.Date,
            is java.sql.Time -> value.toString()
            is java.sql.Array -> {
                try {
                    val arr = value.array
                    when (arr) {
                        is Array<*> -> arr.map { normalizeForAssertion(it) }
                        else -> arr?.toString()
                    }
                } finally {
                    kotlin.runCatching { value.free() }
                }
            }
            else -> {
                if (value.javaClass.name == "org.postgresql.util.PGobject") {
                    val raw = kotlin.runCatching {
                        value.javaClass.getMethod("getValue").invoke(value) as? String
                    }.getOrNull()
                    raw ?: value.toString()
                } else {
                    value
                }
            }
        }
    }

    private fun migrate(jdbcUrl: String, username: String, password: String, changelog: String) {
        DriverManagerDataSource(jdbcUrl, username, password).connection.use { connection ->
            val database = DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(JdbcConnection(connection))
            Liquibase(changelog, ClassLoaderResourceAccessor(), database)
                .update(Contexts(), LabelExpression())
        }
    }

    companion object {
        private val masterJdbcUrl = System.getProperty("test.master.jdbc-url")
            ?: System.getenv("TEST_MASTER_JDBC_URL")
            ?: "jdbc:postgresql://localhost:5433/master"
        private val masterDbUsername = System.getProperty("test.master.username")
            ?: System.getenv("TEST_MASTER_DB_USERNAME")
            ?: "master"
        private val masterDbPassword = System.getProperty("test.master.password")
            ?: System.getenv("TEST_MASTER_DB_PASSWORD")
            ?: "master"
        private val slaveJdbcUrl = System.getProperty("test.slave.jdbc-url")
            ?: System.getenv("TEST_SLAVE_JDBC_URL")
            ?: "jdbc:postgresql://localhost:5434/slave"
        private val slaveDbUsername = System.getProperty("test.slave.username")
            ?: System.getenv("TEST_SLAVE_DB_USERNAME")
            ?: "slave"
        private val slaveDbPassword = System.getProperty("test.slave.password")
            ?: System.getenv("TEST_SLAVE_DB_PASSWORD")
            ?: "slave"
        private val kafkaBootstrapServers = System.getProperty("test.kafka.bootstrap-servers")
            ?: System.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
            ?: "localhost:29092"

        @DynamicPropertySource
        @JvmStatic
        fun registerProps(registry: DynamicPropertyRegistry) {
            registry.add("server.port") { "0" }
            registry.add("spring.main.web-application-type") { "none" }
            registry.add("spring.liquibase.enabled") { false }
            registry.add("spring.datasource.url") { masterJdbcUrl }
            registry.add("spring.datasource.username") { masterDbUsername }
            registry.add("spring.datasource.password") { masterDbPassword }
            registry.add("spring.kafka.bootstrap-servers") { kafkaBootstrapServers }
            registry.add("app.replication.topic") { "db-replication-events" }
        }
    }
}

enum class MasterClient {
    NAMED_JDBC,
    JDBC_TEMPLATE,
    RAW_JDBC,
    JPA_NATIVE,
    JPA_ENTITY
}

@Entity
@Table(name = "table_1")
private class Table1Entity(
    @Id
    @Column(name = "col_1")
    var col1: Long = 0,
    @Column(name = "col_2")
    var col2: String = "",
    @Column(name = "col_3")
    var col3: LocalDateTime = LocalDateTime.now(),
    @Column(name = "version")
    var version: Int = 0
)

@Entity
@Table(name = "table_2")
private class Table2Entity(
    @Id
    @Column(name = "col_1")
    var col1: Long = 0,
    @Column(name = "col_2")
    var col2: Long? = null,
    @Column(name = "col_3")
    var col3: String = "",
    @Column(name = "col_4")
    var col4: String = "",
    @Column(name = "col_5")
    var col5: String = "",
    @Column(name = "col_6")
    var col6: BigDecimal = BigDecimal.ZERO,
    @Column(name = "col_7")
    var col7: Boolean = true,
    @Column(name = "col_8")
    var col8: LocalDate? = null,
    @Column(name = "col_9")
    var col9: LocalTime? = null,
    @Column(name = "col_10")
    var col10: LocalDateTime? = null,
    @Column(name = "col_13")
    var col13: Double? = null,
    @Column(name = "col_14")
    var col14: LocalDateTime = LocalDateTime.now(),
    @Column(name = "col_15")
    var col15: LocalDateTime = LocalDateTime.now(),
    @Column(name = "version")
    var version: Int = 0
)
