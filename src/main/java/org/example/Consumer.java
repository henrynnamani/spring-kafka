package org.example;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getName());

    public static void main(String[] args) {
        log.info("Kafka consumer start");

        String groupId = "first_application";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("group.id", groupId);
    }
}
