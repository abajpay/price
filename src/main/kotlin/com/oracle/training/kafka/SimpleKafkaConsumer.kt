package com.oracle.training.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.configurationprocessor.json.JSONException
import org.springframework.boot.configurationprocessor.json.JSONObject

import java.util.Arrays
import java.util.HashMap
import java.util.Properties

//@Component
class SimpleKafkaConsumer(theTechCheckTopicName: String, consumerProperties: Properties) {

    private val kafkaConsumer: KafkaConsumer<String, String>

    init {

        kafkaConsumer = KafkaConsumer(consumerProperties)
        kafkaConsumer.subscribe(Arrays.asList(theTechCheckTopicName))
    }

    /**
     * This function will start a single worker thread per topic.
     * After creating the consumer object, we subscribed to a list of Kafka topics, in the constructor.
     * For this example, the list consists of only one topic. But you can give it a try with multiple topics.
     */
    fun runSingleWorker() {

        /*
         * We will start an infinite while loop, inside which we'll be listening to
         * new messages in each topic that we've subscribed to.
         */
        while (true) {

            val records = kafkaConsumer.poll(100)

            for (record in records) {

                /*
            Whenever there's a new message in the Kafka topic, we'll get the message in this loop, as the record object.
             */

                /*
            Getting the message as a string from the record object.
             */
                val message = record.value()

                /*
            Logging the received message to the console.
             */
                logger.info("Received message: $message")

                /*
            If you remember, we sent 10 messages to this topic as plain strings. 10 other messages were serialized JSON objects. Now we'll deserialize them here. But since we can't make out which message is a serialized JSON object and which isn't, we'll try to deserialize all of them. So, obviously, we'll get an exception for the first 10 messages we receive. We'll just log the errors and not worry about them.
             */
                try {
                    val receivedJsonObject = JSONObject(message)

                    /*
                To make sure we successfully deserialized the message to a JSON object, we'll log the index of JSON object.
                 */
                    logger.info("Index of deserialized JSON object: " + receivedJsonObject.getInt("index"))
                } catch (e: JSONException) {
                    logger.error(e.message)
                }

                /*
            Once we finish processing a Kafka message, we have to commit the offset so that we don't end up consuming the same message endlessly. By default, the consumer object takes care of this. But to demonstrate how it can be done, we have turned this default behaviour off, instead, we're going to manually commit the offsets.
            The code for this is below. It's pretty much self explanatory.
             */
                run {
                    val commitMessage = HashMap<TopicPartition, OffsetAndMetadata>()

                    commitMessage[TopicPartition(record.topic(), record.partition())] = OffsetAndMetadata(record.offset() + 1)

                    kafkaConsumer.commitSync(commitMessage)

                    logger.info("Offset committed to Kafka.")
                }
            }
        }
    }

    companion object {

        private val logger = LoggerFactory.getLogger(SimpleKafkaConsumer::class.java)
    }
}
