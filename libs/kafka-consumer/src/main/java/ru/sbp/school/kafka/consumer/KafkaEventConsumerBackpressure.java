package ru.sbp.school.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import ru.sbp.school.kafka.api.*;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class KafkaEventConsumerBackpressure<T extends Identifiable> implements EventConsumer<T> {

    private final KafkaEventConsumer<T> kafkaConsumer;

    public KafkaEventConsumerBackpressure(ExecutorService executorService, EventProducer<Ack> producer, Consumer<String, T> consumer, Properties properties) {
        this.kafkaConsumer = new KafkaEventConsumer<>(executorService, consumer, properties);
        kafkaConsumer.addHandler((event) -> producer.send(Ack.acknowledge(event)));
        executorService.execute(kafkaConsumer);
    }

    @Override
    public void addHandler(EventHandler<T> handler) {
        kafkaConsumer.addHandler(handler);
    }

    @Override
    public void run() {
        kafkaConsumer.run();
    }
}
