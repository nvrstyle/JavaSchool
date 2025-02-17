package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.app.TransactionProducer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.Properties;

public class KafkaProducerTest {

    private static final Properties PROPERTIES = PropertiesUtils.load("kafka-producer.properties");
    private static TransactionProducer producer;


    @BeforeAll
    public static void before() {
        producer = new TransactionProducer(new KafkaProducer<>(PROPERTIES), PROPERTIES);
    }

    @Test
    public void testProducer() {
        Transaction transaction1 = Transaction.create(new BigDecimal("100.00"), "47199900011");
        producer.send(transaction1);
        Transaction transaction2 = transaction1.update(new BigDecimal("200.00"), "47199900011");
        producer.send(transaction2);
        Transaction transaction3 = transaction2.delete(new BigDecimal("200.00"), "47199900011");
        producer.send(transaction3);
    }
}
