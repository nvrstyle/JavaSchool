package ru.sbp.school.kafka.app;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.consumer.KafkaEventConsumerBackpressure;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class TransactionConsumerBackpressure {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumerBackpressure.class);

    private static boolean IS_EXECUTE = Boolean.FALSE;
    private final ExecutorService executorService;
    private KafkaEventConsumerBackpressure<Transaction> kafkaConsumer;

    public TransactionConsumerBackpressure(ExecutorService executorService, Properties consumerProperties, Properties producerBackpressureProperties) {
        this.executorService = executorService;
        KafkaEventProducer<Ack> producerBackpressure = new KafkaEventProducer<>(new KafkaProducer<>(producerBackpressureProperties), producerBackpressureProperties);
        KafkaConsumer<String, Transaction> consumer = new KafkaConsumer<>(consumerProperties);
        this.kafkaConsumer = new KafkaEventConsumerBackpressure<>(executorService, producerBackpressure, consumer, consumerProperties);
    }

    public void listen() {
        if (!IS_EXECUTE) {
            executorService.execute(kafkaConsumer);
            IS_EXECUTE = Boolean.TRUE;
            log.info("Запущен кафка консьюмер топика транзакций с гарантией доставки");
        }
    }
}
