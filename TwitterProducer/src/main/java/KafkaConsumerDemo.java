import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 *
 * Consumes records from topic.
 * */
public class KafkaConsumerDemo {

    public static final String MY_JAVA_CONSUMER = "my_java_consumer";
    public static final String KAFKA_SERVER = "localhost:9092";
    public static final String FIRST_TOPIC = "first_topic";

    public static void main(String[] args) {

        // create properties
        final Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, MY_JAVA_CONSUMER);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer  = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(singleton(FIRST_TOPIC));

        // poll data
        while(true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> System.out.println("Topic: " + record.topic() + "\n Partition: " + record.partition() +
                    "\n Key: " + record.key() + "\n Value: " + record.value()));
        }
    }
}
