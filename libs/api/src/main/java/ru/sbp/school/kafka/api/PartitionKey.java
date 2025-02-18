package ru.sbp.school.kafka.api;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class PartitionKey {

    private final String key;

    public PartitionKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public byte[] getKeyBytes() {
        return getKey().getBytes(StandardCharsets.UTF_8);
    }

    public int partitionForKey(int size) {
        return hashCode() & (size - 1);
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        PartitionKey that = (PartitionKey) object;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public String toString() {
        return key;
    }
}
