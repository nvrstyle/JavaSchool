package ru.sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ru.sbp.school.kafka.api.*;
import ru.sbp.school.kafka.consumer.commit.EventCommiter;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class KafkaEventConsumerBackpressure<T extends Identifiable> implements EventConsumer<T> {

    private final KafkaEventConsumer<T> kafkaConsumer;
    private final EventCommiter eventCommiter;
    private final Long commitTimeout;
    private final EventProducer<Ack> ackProducer;
    private final java.util.function.Consumer<Ack> commitCallBack;

    public KafkaEventConsumerBackpressure(ExecutorService executorService, EventProducer<Ack> producer, Consumer<String, T> consumer, Properties properties) {
        this.commitTimeout = Long.valueOf(properties.getProperty("backpressure.timeout.ms"));
        this.kafkaConsumer = new KafkaEventConsumer<>(executorService, consumer, properties);
        this.ackProducer = producer;
        this.commitCallBack = (ack) -> ackProducer.send(ack);;
        this.eventCommiter = new EventCommiter(commitTimeout, commitCallBack);
        kafkaConsumer.addHandler(eventCommiter::waitCommit);
        executorService.execute(kafkaConsumer);
    }

    @Override
    public void addHandler(EventHandler<T> handler) {
        kafkaConsumer.addHandler(handler);
    }

    @Override
    public void listen() {
        kafkaConsumer.listen();
    }

    @Override
    public void stop() {
        kafkaConsumer.stop();
    }

    @Override
    public void run() {
        kafkaConsumer.run();
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return kafkaConsumer.getCurrentOffsets();
    }

    public void commit() {
        eventCommiter.commit(commitCallBack);
    }

    public void scheduleCommit(ScheduledExecutorService executorService) {
        eventCommiter.scheduleCommit(executorService);
    }

    public void scheduleCommit() {
        scheduleCommit(Executors.newSingleThreadScheduledExecutor());
    }
}
