package ru.sbp.school.kafka.app;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducer;

import java.util.Properties;

public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private final KafkaEventProducer<Transaction> kafkaEventProducer;

    public TransactionProducer(Producer<String, Transaction> producer, Properties properties) {
        this.kafkaEventProducer = new KafkaEventProducer<>(producer, properties);
    }

    public void send(Transaction transaction) {
        try {
            kafkaEventProducer.send(transaction);
        } catch (Exception e) {
            log.error("Ошибка публикации транзакции {} в Kafka: {}", transaction.getUuid(), e.getMessage(), e);
            throw e;
        }
    }
}
