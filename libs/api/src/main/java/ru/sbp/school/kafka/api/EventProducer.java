package ru.sbp.school.kafka.api;

import java.util.UUID;

public interface EventProducer<E extends Partitionable & Identifiable> {

    void send(UUID uuid, PartitionKey key, E event);

    default void send(E event) {
        send(event.getUuid(), event.partitionKey(), event);
    }

}
