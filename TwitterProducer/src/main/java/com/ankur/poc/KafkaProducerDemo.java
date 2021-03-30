package com.ankur.poc;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Produces a single record to a topic.
 *
 * */
public class KafkaProducerDemo {
    public static void main(String[] args) {

        // create properties
        final Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create record
        final ProducerRecord<String, String> record = new ProducerRecord("first_topic", "first topic value");

        // create producer
        final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);

        // publish
        producer.send(record);
        System.out.println("Producing record...");
        producer.flush();
        producer.close();

    }
}
