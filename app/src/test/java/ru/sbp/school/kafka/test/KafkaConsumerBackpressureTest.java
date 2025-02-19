package ru.sbp.school.kafka.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.app.TransactionConsumerBackpressure;
import ru.sbp.school.kafka.app.TransactionProducerBackpressure;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerBackpressureTest {

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(4);
    private static final Properties PRODUCER_PROPERTIES = PropertiesUtils.load("kafka-producer.properties");
    private static final Properties CONSUMER_PROPERTIES = PropertiesUtils.load("kafka-consumer.properties");
    private static final Properties PRODUCER_BACKPRESSURE_PROPERTIES = PropertiesUtils.load("kafka-producer-backpressure.properties");
    private static final Properties CONSUMER_BACKPRESSURE_PROPERTIES = PropertiesUtils.load("kafka-consumer-backpressure.properties");

    private static TransactionProducerBackpressure producer;
    private static TransactionConsumerBackpressure consumer;


    @BeforeAll
    public static void before() {
        producer = new TransactionProducerBackpressure(EXECUTOR_SERVICE, PRODUCER_PROPERTIES, CONSUMER_BACKPRESSURE_PROPERTIES);
        consumer = new TransactionConsumerBackpressure(EXECUTOR_SERVICE, CONSUMER_PROPERTIES, PRODUCER_BACKPRESSURE_PROPERTIES);
        consumer.listen();
    }

    @Test
    public void testProducer() {
        Transaction transaction1 = Transaction.create(new BigDecimal("100.00"), "47199900011");
        producer.send(transaction1);
        Transaction transaction2 = transaction1.update(new BigDecimal("200.00"), "47199900011");
        producer.send(transaction2);
        Transaction transaction3 = transaction2.delete(new BigDecimal("200.00"), "47199900011");
        producer.send(transaction3);
        while (true) {

        }
    }
}
