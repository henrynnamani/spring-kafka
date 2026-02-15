package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am working!!!");

        Properties properties = new Properties();

        // environment
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id" + i;
            String value = "hello universe, " + i;


            ProducerRecord<String, String> record =  new ProducerRecord<>(topic, key, value);

            // sending data to topic
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Received new metadata \nKey: {}\nPartition: {}\nOffset: {}", key, recordMetadata.partition(), recordMetadata.offset());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }



        producer.flush();

        producer.close();
    }
}