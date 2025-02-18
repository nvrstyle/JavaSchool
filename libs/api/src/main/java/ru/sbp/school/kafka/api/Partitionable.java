package ru.sbp.school.kafka.api;

public interface Partitionable {

    PartitionKey partitionKey();

    String toString();
}
