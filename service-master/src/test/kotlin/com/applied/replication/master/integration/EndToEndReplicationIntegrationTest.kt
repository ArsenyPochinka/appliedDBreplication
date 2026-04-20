package com.applied.replication.master.integration

import com.applied.replication.master.MasterApplication
import com.applied.replication.receiver.ReceiverApplication
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.sql.Connection
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import javax.sql.DataSource

@Testcontainers
@SpringBootTest(classes = [MasterApplication::class])
class EndToEndReplicationIntegrationTest {

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

    @BeforeAll
    fun beforeAll() {
        migrate(masterDb.jdbcUrl, masterDb.username, masterDb.password, "db/changelog/master-test.yaml")
        migrate(slaveDb.jdbcUrl, slaveDb.username, slaveDb.password, "db/changelog/slave-test.yaml")

        masterJdbc = JdbcTemplate(DriverManagerDataSource(masterDb.jdbcUrl, masterDb.username, masterDb.password))
        slaveJdbc = JdbcTemplate(DriverManagerDataSource(slaveDb.jdbcUrl, slaveDb.username, slaveDb.password))

        receiverContext = SpringApplicationBuilder(ReceiverApplication::class.java)
            .properties(
                mapOf(
                    "server.port" to "0",
                    "spring.main.web-application-type" to "none",
                    "spring.datasource.url" to slaveDb.jdbcUrl,
                    "spring.datasource.username" to slaveDb.username,
                    "spring.datasource.password" to slaveDb.password,
                    "spring.kafka.bootstrap-servers" to kafka.bootstrapServers,
                    "app.replication.topic" to "db-replication-events"
                )
            )
            .run()
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
                insertTable1(client, table1Col2)
                val table1Id = findTable1IdByCol2(table1Col2)
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

    private fun insertTable1(client: MasterClient, col2: String) {
        when (client) {
            MasterClient.NAMED_JDBC -> namedJdbc.update(
                "INSERT INTO table_1(col_2, col_3) VALUES (:col2, now())",
                mapOf("col2" to col2)
            )
            MasterClient.JDBC_TEMPLATE -> plainJdbc.update(
                "INSERT INTO table_1(col_2, col_3) VALUES (?, now())",
                col2
            )
            MasterClient.RAW_JDBC -> rawExecute("INSERT INTO table_1(col_2, col_3) VALUES (?, now())") {
                it.setString(1, col2)
            }
            MasterClient.JPA_NATIVE -> entityManager.createNativeQuery(
                "INSERT INTO table_1(col_2, col_3) VALUES (?1, now())"
            ).setParameter(1, col2).executeUpdate()
            MasterClient.JPA_ENTITY -> entityManager.persist(
                Table1Entity(
                    col1 = nextTable1Id(),
                    col2 = col2,
                    col3 = LocalDateTime.now()
                )
            )
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
        return masterJdbc.queryForObject(
            "INSERT INTO table_1(col_2, col_3) VALUES (?, now()) RETURNING col_1",
            Long::class.java,
            col2
        )!!
    }

    private fun seedTable2(table1Id: Long, col5: String): Long {
        return masterJdbc.queryForObject(
            """
            INSERT INTO table_2(col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15)
            VALUES (?, 'seed3', 'seed4', ?, 111.11, true, DATE '2000-01-01', TIME '08:00:00', now(), '{"seed":true}'::jsonb, ARRAY['seed'], 1.1, now(), now())
            RETURNING col_1
            """.trimIndent(),
            Long::class.java,
            table1Id,
            col5
        )!!
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
        dataSource.connection.use { connection: Connection ->
            connection.prepareStatement(sql).use { statement ->
                binder(statement)
                statement.executeUpdate()
            }
        }
    }

    private fun awaitReplicationConsistency() {
        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted {
                kotlin.test.assertEquals(tableRows(masterJdbc, "table_1"), tableRows(slaveJdbc, "table_1"))
                kotlin.test.assertEquals(tableRows(masterJdbc, "table_2"), tableRows(slaveJdbc, "table_2"))
            }
    }

    private fun tableRows(jdbc: JdbcTemplate, table: String): List<Map<String, Any?>> {
        return jdbc.queryForList("SELECT * FROM $table ORDER BY col_1")
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
        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:3.7.1"))

        @Container
        @JvmStatic
        val masterDb = PostgreSQLContainer<Nothing>("postgres:16")
            .apply {
                withDatabaseName("master")
                withUsername("master")
                withPassword("master")
            }

        @Container
        @JvmStatic
        val slaveDb = PostgreSQLContainer<Nothing>("postgres:16")
            .apply {
                withDatabaseName("slave")
                withUsername("slave")
                withPassword("slave")
            }

        @DynamicPropertySource
        @JvmStatic
        fun registerProps(registry: DynamicPropertyRegistry) {
            registry.add("server.port") { "0" }
            registry.add("spring.main.web-application-type") { "none" }
            registry.add("spring.datasource.url", masterDb::getJdbcUrl)
            registry.add("spring.datasource.username", masterDb::getUsername)
            registry.add("spring.datasource.password", masterDb::getPassword)
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
            registry.add("app.replication.topic") { "db-replication-events" }
        }
    }
}

private enum class MasterClient {
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
    var col3: LocalDateTime = LocalDateTime.now()
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
    var col15: LocalDateTime = LocalDateTime.now()
)
