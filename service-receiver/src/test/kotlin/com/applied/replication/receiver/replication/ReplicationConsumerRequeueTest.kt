package com.applied.replication.receiver.replication

import com.applied.replication.common.ReplicationMessage
import com.applied.replication.receiver.config.ReplicationProperties
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.test.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.kafka.core.KafkaTemplate

@ExtendWith(MockitoExtension::class)
class ReplicationConsumerRequeueTest {

    @Mock
    private lateinit var applier: ReplicationApplier

    @Mock
    private lateinit var kafkaTemplate: KafkaTemplate<String, ReplicationMessage>

    private lateinit var consumer: ReplicationConsumer

    private val objectMapper = ObjectMapper()

    private val defaultProps = ReplicationProperties(topic = "db-replication-events", maxRequeueIterations = 10)

    @BeforeEach
    fun setup() {
        consumer = ReplicationConsumer(applier, kafkaTemplate, defaultProps)
    }

    @Test
    fun `requeues message when applier returns REQUEUE and increments iterationCount`() {
        val payload = objectMapper.createObjectNode().put("id", 1L).put("version", 2)
        val message = ReplicationMessage(10L, 99L, "recv_applier_t1", "DELETE", payload, iterationCount = 2)
        `when`(applier.apply(any(ReplicationMessage::class.java)))
            .thenReturn(ReplicationApplier.ApplyResult.REQUEUE)

        consumer.listen(message)

        val captor = ArgumentCaptor.forClass(ReplicationMessage::class.java)
        verify(kafkaTemplate).send(eq("db-replication-events"), eq("99"), captor.capture())
        assertEquals(3, captor.value.iterationCount)
        assertEquals(message.eventId, captor.value.eventId)
    }

    @Test
    fun `does not send to kafka when applier returns SKIPPED`() {
        val payload = objectMapper.createObjectNode().put("id", 1L).put("version", 2)
        val message = ReplicationMessage(11L, 100L, "recv_applier_t1", "DELETE", payload)
        `when`(applier.apply(any(ReplicationMessage::class.java)))
            .thenReturn(ReplicationApplier.ApplyResult.SKIPPED)

        consumer.listen(message)

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any(ReplicationMessage::class.java))
    }

    @Test
    fun `does not send to kafka when applier returns APPLIED`() {
        val payload = objectMapper.createObjectNode().put("id", 1L).put("version", 2)
        val message = ReplicationMessage(12L, 101L, "recv_applier_t1", "INSERT", payload)
        `when`(applier.apply(any(ReplicationMessage::class.java)))
            .thenReturn(ReplicationApplier.ApplyResult.APPLIED)

        consumer.listen(message)

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any(ReplicationMessage::class.java))
    }

    @Test
    fun `skips apply when iterationCount exceeds configured max`() {
        val strict = ReplicationConsumer(
            applier,
            kafkaTemplate,
            ReplicationProperties(topic = "db-replication-events", maxRequeueIterations = 5)
        )
        val payload = objectMapper.createObjectNode().put("id", 1L).put("version", 2)
        val message = ReplicationMessage(13L, 102L, "recv_applier_t1", "DELETE", payload, iterationCount = 6)

        strict.listen(message)

        verify(applier, never()).apply(any(ReplicationMessage::class.java))
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any(ReplicationMessage::class.java))
    }

    @Test
    fun `still applies when iterationCount equals max`() {
        val strict = ReplicationConsumer(
            applier,
            kafkaTemplate,
            ReplicationProperties(topic = "db-replication-events", maxRequeueIterations = 5)
        )
        val payload = objectMapper.createObjectNode().put("id", 1L).put("version", 2)
        val message = ReplicationMessage(14L, 103L, "recv_applier_t1", "DELETE", payload, iterationCount = 5)
        `when`(applier.apply(any(ReplicationMessage::class.java)))
            .thenReturn(ReplicationApplier.ApplyResult.APPLIED)

        strict.listen(message)

        verify(applier).apply(any(ReplicationMessage::class.java))
    }
}
