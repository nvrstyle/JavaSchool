package ru.sbp.school.kafka.api;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Ack implements Identifiable, Partitionable  {

    private UUID uuid;
    private LocalDateTime timeStamp;

    private List<UUID> events = new ArrayList<>();

    public Ack() {
    }

    public Ack(List<UUID> events) {
        this.uuid = UUID.randomUUID();
        this.timeStamp = LocalDateTime.now();
        this.events = events;
    }

    public List<UUID> getEvents() {
        return events;
    }

    public void setEvents(List<UUID> events) {
        this.events = events;
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

    public static Ack acknowledge(List<Identifiable> events) {
        return new Ack(events.stream()
                .map(Identifiable::getUuid)
                .collect(Collectors.toList()));
    }

    public String flatMapEvents() {
        return getEvents().stream()
                .map(UUID::toString)
                .reduce((l,r) -> l + ", " + r)
                .orElse("");
    }
}
