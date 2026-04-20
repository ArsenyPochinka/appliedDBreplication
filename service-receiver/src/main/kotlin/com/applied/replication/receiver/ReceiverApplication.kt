package com.applied.replication.receiver

import com.applied.replication.receiver.config.ReplicationProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(ReplicationProperties::class)
class ReceiverApplication

fun main(args: Array<String>) {
    runApplication<ReceiverApplication>(*args)
}
