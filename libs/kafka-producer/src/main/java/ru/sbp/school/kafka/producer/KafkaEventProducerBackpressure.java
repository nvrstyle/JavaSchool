package ru.sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.*;
import ru.sbp.school.kafka.producer.commit.AckCommiter;
import ru.sbp.school.kafka.producer.exception.KafkaProducerException;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

public class KafkaEventProducerBackpressure<T extends Partitionable & Identifiable> implements EventProducer<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducerBackpressure.class);

    private final KafkaEventProducer<T> kafkaProducer;
    private final AckCommiter ackCommiter;
    private final Long commitTimeout;

    public KafkaEventProducerBackpressure(ExecutorService executorService, EventConsumer<Ack> consumer, Producer<String, T> producer, Properties properties) {
        this.commitTimeout = Long.valueOf(properties.getProperty("backpressure.timeout.ms"));
        this.kafkaProducer = new KafkaEventProducer<>(producer, properties);
        this.ackCommiter = new AckCommiter(Executors.newSingleThreadScheduledExecutor(), commitTimeout, (event) -> send((T) event));
        consumer.addHandler(ackCommiter::commit);
        executorService.execute(consumer);
    }

    @Override
    public void send(UUID uuid, PartitionKey key, T event) {
        try {
            kafkaProducer.send(uuid, key, event);
            ackCommiter.waitCommit(event);
        } catch (Exception e) {
            log.error("Ошибка отправки с гарантией доставки события в кафка: {}", e.getMessage(), e);
            throw new KafkaProducerException(e);
        }
    }
}
