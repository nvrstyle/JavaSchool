package ru.sbp.school.kafka.app;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.api.EventConsumer;
import ru.sbp.school.kafka.consumer.KafkaEventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducerBackpressure;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class TransactionProducerBackpressure {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerBackpressure.class);
    private final KafkaEventProducerBackpressure<Transaction> kafkaProducer;

    public TransactionProducerBackpressure(ExecutorService executorService, Properties producerProperties, Properties consumerBackpressureProperties) {
        KafkaEventConsumer<Ack> kafkaConsumer = new KafkaEventConsumer<>(executorService, new KafkaConsumer<>(consumerBackpressureProperties), consumerBackpressureProperties);
        Producer<String, Transaction> producer = new KafkaProducer<>(producerProperties);
        this.kafkaProducer = new KafkaEventProducerBackpressure<>(executorService, kafkaConsumer, producer, producerProperties);
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
