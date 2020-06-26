package com.github.simplekafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;

class Consumer {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Logger logger;

    public Consumer(KafkaConsumer<String, String> kafkaConsumer, Logger logger) {
        this.kafkaConsumer = kafkaConsumer;
        this.logger = logger;
    }

    public void subScribe(Collection<String> topics) {
        this.kafkaConsumer.subscribe(topics);
    }

    public void poll(long mills) {
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(mills));
            consumerRecords.forEach(record -> {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            });
        }
    }

}
