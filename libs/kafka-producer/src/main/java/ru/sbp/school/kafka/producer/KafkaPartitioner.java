package ru.sbp.school.kafka.producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import ru.sbp.school.kafka.api.PartitionKey;
import ru.sbp.school.kafka.api.Partitionable;

import java.util.List;
import java.util.Map;

public class KafkaPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!(value instanceof Partitionable)) {
            throw new IllegalArgumentException("Ключ партиционирования должен иметь тип %s".formatted(PartitionKey.class.getName()));
        }
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        return ((Partitionable) value).partitionKey().partitionForKey(partitions.size());
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
