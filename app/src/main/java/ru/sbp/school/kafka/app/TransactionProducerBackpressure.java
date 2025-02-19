package ru.sbp.school.kafka.app;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.api.EventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducerBackpressure;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class TransactionProducerBackpressure {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerBackpressure.class);
    private final KafkaEventProducerBackpressure<Transaction> kafkaProducer;

    public TransactionProducerBackpressure(ExecutorService executorService, EventConsumer<Ack> consumer, Producer<String, Transaction> producer, Properties properties) {
        this.kafkaProducer = new KafkaEventProducerBackpressure<>(executorService, consumer, producer, properties);
    }

    public void send(Transaction transaction) {
        try {
            kafkaProducer.send(transaction);
        } catch (Exception e) {
            log.error("Ошибка публикации транзакции {} c гарантией доставки в Kafka: {}", transaction.getUuid(), e.getMessage(), e);
            throw e;
        }
    }
}
