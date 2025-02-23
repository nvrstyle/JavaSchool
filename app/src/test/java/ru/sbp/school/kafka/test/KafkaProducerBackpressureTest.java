package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.consumer.KafkaEventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducerBackpressure;
import ru.sbp.school.kafka.producer.KafkaPartitioner;
import ru.sbp.school.kafka.producer.serialize.JsonSerializer;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaProducerBackpressureTest {

    private static final ExecutorService EXECUTOR_SERVICE = Mockito.mock(ExecutorService.class);
    private static final Properties PRODUCER_PROPERTIES = PropertiesUtils.load("kafka-producer.properties");
    private static final Properties CONSUMER_BACKPRESSURE_PROPERTIES = PropertiesUtils.load("kafka-consumer-backpressure.properties");
    private static MockProducer<String, Transaction> mockProducer;
    private static Cluster cluster;
    private static KafkaEventProducerBackpressure<Transaction> producer;
    private static final Transaction transaction = Transaction.create(new BigDecimal("100.00"), "47199900011");
    private static final Ack ack = Ack.acknowledge(List.of(transaction));

    private static final String TOPIC = PRODUCER_PROPERTIES.getProperty("topic");
    private static final String ACK_TOPIC = CONSUMER_BACKPRESSURE_PROPERTIES.getProperty("topic");
    private static final MockConsumer<String, Ack> CONSUMER = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private static KafkaEventConsumer<Ack> ackConsumer = new KafkaEventConsumer<>(EXECUTOR_SERVICE, CONSUMER, CONSUMER_BACKPRESSURE_PROPERTIES);


    @BeforeAll
    public static void before() {
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new PartitionInfo(TOPIC, 0, null, null, null));
        partitions.add(new PartitionInfo(TOPIC, 1, null, null, null));

        cluster = new Cluster("kafkab", new ArrayList<>(), partitions, emptySet(), emptySet());
        mockProducer = new MockProducer<>(cluster, true, new KafkaPartitioner(), new StringSerializer(), new JsonSerializer<>());
        producer = new KafkaEventProducerBackpressure<>(EXECUTOR_SERVICE, ackConsumer, mockProducer, PRODUCER_PROPERTIES);
        producer.send(transaction);
        CONSUMER.schedulePollTask(() -> {
            CONSUMER.rebalance(Collections.singletonList(new TopicPartition(ACK_TOPIC, 0)));
            CONSUMER.addRecord(new ConsumerRecord<>(ACK_TOPIC, 0, 0L, "key", ack));
        });
        CONSUMER.schedulePollTask(CONSUMER::wakeup);
    }

    @Test
    public void testProducer() {
        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(ACK_TOPIC, 0);
        startingOffsets.put(tp, 0L);
        CONSUMER.updateBeginningOffsets(startingOffsets);
        //when
        ackConsumer.listen();
        Map<TopicPartition, OffsetAndMetadata> map = ackConsumer.getCurrentOffsets();
        long currentOffset = map.get(tp).offset();
        //then
        Assertions.assertEquals(1, currentOffset);
        Assertions.assertTrue(CONSUMER.closed());
        assertTrue(mockProducer.history().size() == 1);
        //Проверка получения из обратного потока получения события-подтверждения
        List<Ack> acks = producer.getAcks();
        assertEquals(1, acks.size());
        assertEquals(acks.get(0).getEvents().get(0), transaction.getUuid());
    }
}
