package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
       log.info("Hello World");

        // create Producer Properties
        Properties properties = new Properties();

        // localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // not recommend in production
        // properties.setProperty("batch.size", "400");

        // use this instead
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());



        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record

    for (int j = 0; j < 10; j++) {
        for (int i = 0; i < 30; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world " + i);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every times a record successfully sent a record or an exception is thrown
                    if (e == null) {
                        log.info("Successfully sent message \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: ", recordMetadata.timestamp());
                    }else {
                        log.error("Error while sending message", e);
                    }
                }
            });
        }

        Thread.sleep(500);
    }

        // flush and close the producer
        // flush: tell producer to send all data and block until done --> synchronize
        producer.flush();

        // close: also call producer.flush before doing this
        producer.close();
    }
}
