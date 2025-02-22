package ru.sbp.school.kafka.model;

import ru.sbp.school.kafka.api.PartitionKey;
import ru.sbp.school.kafka.api.Partitionable;

public enum OperationType implements Partitionable {
    CREATE("Создание"),
    UPDATE("Редактирование"),
    DELETE("Удаление");

    private final String description;

    OperationType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public PartitionKey partitionKey() {
        return new PartitionKey(name());
    }
}
