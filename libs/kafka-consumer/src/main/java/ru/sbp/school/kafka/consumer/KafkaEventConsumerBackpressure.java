package ru.sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import ru.sbp.school.kafka.api.*;
import ru.sbp.school.kafka.consumer.commit.EventCommiter;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaEventConsumerBackpressure<T extends Identifiable> implements EventConsumer<T> {

    private final KafkaEventConsumer<T> kafkaConsumer;
    private final EventCommiter eventCommiter;
    private final Long commitTimeout;

    public KafkaEventConsumerBackpressure(ExecutorService executorService, EventProducer<Ack> producer, Consumer<String, T> consumer, Properties properties) {
        this.commitTimeout = Long.valueOf(properties.getProperty("backpressure.timeout.ms"));
        this.kafkaConsumer = new KafkaEventConsumer<>(executorService, consumer, properties);
        this.eventCommiter = new EventCommiter(Executors.newSingleThreadScheduledExecutor(), commitTimeout, (ack) -> producer.send(ack));
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
}
