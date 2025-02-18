package ru.sbp.school.kafka.api;

public interface EventConsumer<T> extends Runnable {

    void addHandler(EventHandler<T> consumer);
}
