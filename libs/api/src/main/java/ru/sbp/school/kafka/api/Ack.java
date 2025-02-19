package ru.sbp.school.kafka.api;

import java.time.LocalDateTime;
import java.util.UUID;

public class Ack implements Identifiable, Partitionable  {

    private UUID uuid;
    private LocalDateTime timeStamp;

    public Ack() {
    }

    public Ack(UUID uuid) {
        this.uuid = uuid;
        this.timeStamp = LocalDateTime.now();
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public LocalDateTime getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(LocalDateTime timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public PartitionKey partitionKey() {
        return new PartitionKey(String.valueOf(timeStamp.getHour()));
    }

    public boolean isOverdue(Long timeout) {
        return LocalDateTime.now().getSecond() - timeStamp.getSecond() > timeout;
    }

    public static Ack acknowledge(Identifiable event) {
        return new Ack(event.getUuid());
    }
}
