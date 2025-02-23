package ru.sbp.school.kafka.producer.test;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.producer.KafkaEventProducer;
import ru.sbp.school.kafka.producer.KafkaPartitioner;
import ru.sbp.school.kafka.producer.test.model.OperationType;
import ru.sbp.school.kafka.producer.test.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaPartitionerTest {

    private static final String TOPIC_NAME = "TEST";
    private static Cluster cluster;

    @Test
    public void partitionerTest() {
        //given
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new PartitionInfo(TOPIC_NAME, 0, null, null, null));
        partitions.add(new PartitionInfo(TOPIC_NAME, 1, null, null, null));
        cluster = new Cluster("kafkab", new ArrayList<>(), partitions, emptySet(), emptySet());
        int expectedPartition = 0;
        UUID rqUid = UUID.fromString("6d12f8e3-ecba-46aa-8109-0ca2ad212e72");
        LocalDateTime date = LocalDate.of(2024, 01, 21).atStartOfDay();
        Transaction transaction1 = new Transaction(rqUid, OperationType.CREATE, new BigDecimal("100.00"), "47199900011", date);
        //when
        int partition = new KafkaPartitioner().partition(TOPIC_NAME, transaction1.partitionKey(), null, transaction1, null, cluster);
        //then
        assertEquals(expectedPartition, partition);
    }
}
