package com.github.simplekafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

public class Producer {

    private final KafkaProducer<String, String> kafkaProducer;
    private final Logger logger;

    public Producer(KafkaProducer<String, String> kafkaProducer, Logger logger) {
        this.kafkaProducer = kafkaProducer;
        this.logger = logger;
    }

    public void send(String topic, String key, String value) {
        this.logger.info("key: " + key);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                this.logger.info("\nReceived Metadata " +
                        "\nTopic = " + recordMetadata.topic() +
                        "\nPartition = " + recordMetadata.partition() +
                        "\nOffset = " + recordMetadata.offset() +
                        "\nTimeStamp = " + recordMetadata.timestamp()
                );
            } else {
                this.logger.error("Error while Producing ", e);
            }
        });
    }
}
