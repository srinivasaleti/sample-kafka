package com.github.simplekafka.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        Properties properties = Config.consumerProperties("my_first_application", "earliest", "127.0.0.1:9092");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        Consumer consumer = new Consumer(kafkaConsumer, logger);
        consumer.subScribe(Collections.singleton("hello_world"));

        consumer.poll(100);
    }

}
