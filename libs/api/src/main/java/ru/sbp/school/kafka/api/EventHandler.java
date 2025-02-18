package ru.sbp.school.kafka.api;

public interface EventHandler<E> {

    void handle(E event);
}
