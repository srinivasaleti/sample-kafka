package com.github.simplekafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(Producer.class);
        Properties properties = Config.producerProperties("127.0.0.1:9092");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        Producer producer = new Producer(kafkaProducer, logger);

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            producer.send("hello_world", key, "New Message: " + i);
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
