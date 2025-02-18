package ru.sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.EventConsumer;
import ru.sbp.school.kafka.api.EventHandler;
import ru.sbp.school.kafka.api.Identifiable;
import ru.sbp.school.kafka.consumer.exception.KafkaConsumerException;
import ru.sbp.school.kafka.consumer.exception.KafkaEventHandleException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class KafkaEventConsumer<T extends Identifiable> implements Runnable, EventConsumer<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);
    private static final String ERROR_PROCESS_LOG = "Ошибка обаботки kafka события из топика {} партиция {} оффсет {}: {}";

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();
    private final Set<EventHandler<T>> handlers = new HashSet<>();
    private final Consumer<String, T> consumer;
    private final ExecutorService executorService;
    private final String topic;
    private final int batchSize;

    public KafkaEventConsumer(ExecutorService executorService, Consumer<String, T> consumer, Properties properties) {
        this.consumer = consumer;
        this.executorService = executorService;
        this.topic = properties.getProperty("topic");
        this.batchSize = Integer.parseInt(properties.getProperty("batch.size"));
        this.consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, T> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, T> record : consumerRecords) {
                    processRecord(record);
                    handlers.forEach(handler -> executorService.execute(() -> tryHandle(handler, record.value())));
                }
            }
        } catch (Exception e) {
            log.error("В работе Kafka консьюмера произошла ошибка: {}", e.getMessage(), e);
            throw new KafkaConsumerException(e);
        } finally {
            consumer.commitSync(currentOffsets, null);
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, T> record) {
        try {
            currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "metadata"));
            if (currentOffsets.size() % batchSize == 0) {
                consumer.commitAsync(currentOffsets, null);
                log.info("Событие uuid = {} offset = {} прочитано из топика {} (партиция {})", record.value().getUuid(), record.offset(), record.topic(), record.key());
            }
        } catch (Exception e) {
            log.error(ERROR_PROCESS_LOG, topic, record.partition(), record.offset(), e.getMessage(), e);
            throw new SerializationException(e);
        }
    }

    private void tryHandle(EventHandler<T> handler, T event) {
        try {
            handler.handle(event);
        } catch (Exception e) {
            log.error("Ошибка обработки kafka события uuid = {} : {}", event.getUuid(), e.getMessage(), e);
            throw new KafkaEventHandleException(e);
        }
    }

    @Override
    public void addHandler(EventHandler<T> handler) {
        handlers.add(handler);
    }
}
