package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.consumer.KafkaEventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerTest {

    private static final Properties PROPERTIES = PropertiesUtils.load("kafka-consumer.properties");
    private static final KafkaConsumer<String, Transaction> CONSUMER = new KafkaConsumer<>(PROPERTIES);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);

    @Test
    public void testConsumer() {
        EXECUTOR_SERVICE.submit(new KafkaEventConsumer<>(EXECUTOR_SERVICE, CONSUMER, PROPERTIES));
        while (true) {

        }
    }
}
