package ru.sbp.school.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.EventProducer;
import ru.sbp.school.kafka.api.Identifiable;
import ru.sbp.school.kafka.api.PartitionKey;
import ru.sbp.school.kafka.api.Partitionable;
import ru.sbp.school.kafka.producer.exception.KafkaProducerException;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class KafkaEventProducer<T extends Partitionable & Identifiable> implements EventProducer<T> {

    private static final String SUCCESS_SEND_LOG = "Отправлено событие с uuid {} в топик {}, партиция: {}, оффсет: {}";
    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final Producer<String, T> producer;
    private final String topic;

    public KafkaEventProducer(Producer<String, T> producer, Properties properties) {
        this.producer = producer;
        this.topic = properties.getProperty("topic");
    }

    public void send(T object) {
       send(object.getUuid(), object.partitionKey(), object);
    }

    public void send(UUID uuid, PartitionKey key, T object) {
        log.debug("Попытка отправки события (uuid = {}) с ключом {} в топик {}", uuid, key, topic);
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key.getKey(), object));
            RecordMetadata recordMetadata = future.get();
            log.info(SUCCESS_SEND_LOG, uuid, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        } catch (Throwable e) {
            log.error("Ошибка отправки события с uuid = {} в топик {} с ключом {}", uuid , topic, key, e);
            throw new KafkaProducerException(e);
        } finally {
            producer.flush();
        }
    }
}
