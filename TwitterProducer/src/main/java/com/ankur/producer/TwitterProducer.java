package com.ankur.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TwitterProducer {

    final static Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());
    public static final String CONSUMER_KEY = System.getenv("CONSUMER_KEY");
    public static final String CONSUMER_SECRET = System.getenv("CONSUMER_SECRET");
    public static final String TOKEN = System.getenv("TOKEN");
    public static final String SECRET = System.getenv("SECRET");
    public static final String SEARCH_TERM = "kafka";
    public static final String TOPIC = "tweets";
    public static final String KAFKA_SERVER = "localhost:9092";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);
        final Client hosebirdClient = this.getTwitterClient(msgQueue);
        hosebirdClient.connect();

        final org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer = this.getKafkaProducer();
        this.processData(msgQueue, hosebirdClient, kafkaProducer);

    }

    private org.apache.kafka.clients.producer.KafkaProducer getKafkaProducer() {
        final Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new org.apache.kafka.clients.producer.KafkaProducer(properties);
    }

    private Client getTwitterClient(final BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        /** Optional: set up some followings and track terms */
        final List<String> terms = asList(SEARCH_TERM);
        hosebirdEndpoint.trackTerms(terms);

        final Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);
        return new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();
    }

    private static void processData(
            final BlockingQueue<String> msgQueue,
            final Client hosebirdClient,
            final org.apache.kafka.clients.producer.KafkaProducer kafkaProducer
    ) {
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                LOGGER.info(e.getMessage());
                hosebirdClient.stop();
            }
            if (msg != null) {
                LOGGER.info(msg);
                kafkaProducer.send(
                        (new org.apache.kafka.clients.producer.ProducerRecord(TOPIC, null, msg)),
                        (final org.apache.kafka.clients.producer.RecordMetadata recordMetadata, final Exception e) -> {
                            if (e != null) {
                                LOGGER.error("Something bad happened: {0}", e);
                            }
                        });
            }
        }
    }
}
