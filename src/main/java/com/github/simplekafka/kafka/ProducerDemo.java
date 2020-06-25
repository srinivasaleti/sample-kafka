package com.github.simplekafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);


        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic","Hey First One");

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            System.out.println("Hello World");
        });

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            System.out.println("Hello World");
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
