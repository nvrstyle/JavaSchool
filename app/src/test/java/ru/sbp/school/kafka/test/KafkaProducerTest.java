package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducer;
import ru.sbp.school.kafka.producer.KafkaPartitioner;
import ru.sbp.school.kafka.producer.serialize.JsonSerializer;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class KafkaProducerTest {

    private static final Properties PROPERTIES = PropertiesUtils.load("kafka-producer.properties");
    private static final String TOPIC_NAME = PROPERTIES.getProperty("topic");
    private static KafkaEventProducer<Transaction> producer;
    private static MockProducer<String, Transaction> mockProducer;
    private static Cluster cluster;

    @BeforeAll
    public static void before() {
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new PartitionInfo(TOPIC_NAME, 0, null, null, null));
        partitions.add(new PartitionInfo(TOPIC_NAME, 1, null, null, null));

        cluster = new Cluster("kafkab", new ArrayList<>(), partitions, emptySet(), emptySet());
        mockProducer = new MockProducer<>(cluster, true, new KafkaPartitioner(), new StringSerializer(), new JsonSerializer<>());
        producer = new KafkaEventProducer<>(mockProducer, PROPERTIES);
    }

    @Test
    public void testProducer() {
        //given
        List<Transaction> transactions = new ArrayList<>();
        transactions.add(Transaction.create(new BigDecimal("100.00"), "47199900011"));
        //when
        transactions.forEach(producer::send);
        //then
        assertTrue(mockProducer.history().size() == 1);
    }
}
