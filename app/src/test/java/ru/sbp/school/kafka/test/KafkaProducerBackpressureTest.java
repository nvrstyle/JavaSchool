package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.app.TransactionProducerBackpressure;
import ru.sbp.school.kafka.consumer.KafkaEventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaProducerBackpressureTest {

    private static final Properties PRODUCER_PROPERTIES = PropertiesUtils.load("kafka-producer.properties");
    private static TransactionProducerBackpressure producer;
    private static final Properties CONSUMER_BACKPRESSURE_PROPERTIES = PropertiesUtils.load("kafka-consumer-backpressure.properties");
    private static final KafkaConsumer<String, Ack> CONSUMER_BACKPRESSURE = new KafkaConsumer<>(CONSUMER_BACKPRESSURE_PROPERTIES);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(4);


    @BeforeAll
    public static void before() {
        KafkaEventConsumer<Ack> kafkaConsumer = new KafkaEventConsumer<>(EXECUTOR_SERVICE, CONSUMER_BACKPRESSURE, CONSUMER_BACKPRESSURE_PROPERTIES);
        producer = new TransactionProducerBackpressure(EXECUTOR_SERVICE, kafkaConsumer, new KafkaProducer<>(PRODUCER_PROPERTIES), PRODUCER_PROPERTIES);
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
