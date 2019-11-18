package com.oracle.training

import com.oracle.training.kafka.SimpleKafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

import java.util.Properties

@SpringBootApplication
@RestController
open class StartApplication : CommandLineRunner {


    private/*
         * Defining Kafka consumer properties.
         */ val consumerProperties: Properties
        get() {
            val consumerProperties = Properties()
            consumerProperties["bootstrap.servers"] = "192.168.1.3:9092"
            consumerProperties["group.id"] = "thetechcheck"
            consumerProperties["zookeeper.session.timeout.ms"] = "6000"
            consumerProperties["zookeeper.sync.time.ms"] = "2000"
            consumerProperties["auto.commit.enable"] = "false"
            consumerProperties["auto.commit.interval.ms"] = "1000"
            consumerProperties["consumer.timeout.ms"] = "-1"
            consumerProperties["max.poll.records"] = "1"
            consumerProperties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            consumerProperties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            return consumerProperties
        }

    @RequestMapping("/priceChange")
    fun home(): String {
        return "Welcome to price change history"
    }

    @Throws(Exception::class)
    override fun run(vararg args: String) {

        logger.info("StartApplication...")

        consumer()

    }

    fun consumer() {
        /*
         * Creating a thread to listen to the kafka topic
         */
        val kafkaConsumerThread = Thread {
            logger.info("Starting Kafka consumer thread.")

            val simpleKafkaConsumer = SimpleKafkaConsumer("productInfo1", consumerProperties)

            simpleKafkaConsumer.runSingleWorker()
        }

        /*
         * Starting the first thread.
         */
        kafkaConsumerThread.start()
    }

    companion object {

        private val logger = LoggerFactory.getLogger(StartApplication::class.java)


        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(StartApplication::class.java, *args)
        }
    }

}
