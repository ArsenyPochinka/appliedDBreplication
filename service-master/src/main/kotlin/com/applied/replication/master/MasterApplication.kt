package com.applied.replication.master

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MasterApplication

fun main(args: Array<String>) {
    runApplication<MasterApplication>(*args)
}
