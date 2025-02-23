package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.consumer.KafkaEventConsumerBackpressure;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.producer.KafkaEventProducer;
import ru.sbp.school.kafka.producer.KafkaPartitioner;
import ru.sbp.school.kafka.producer.serialize.JsonSerializer;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaConsumerBackpressureTest {

    private static final ExecutorService EXECUTOR_SERVICE = Mockito.mock(ExecutorService.class);
    private static final Properties CONSUMER_PROPERTIES = PropertiesUtils.load("kafka-consumer.properties");
    private static final Properties PRODUCER_BACKPRESSURE_PROPERTIES = PropertiesUtils.load("kafka-producer-backpressure.properties");
    private static MockProducer<String, Ack> mockAckProducer;
    private static Cluster cluster;
    private static KafkaEventProducer<Ack> producer;
    private static final Transaction transaction = Transaction.create(new BigDecimal("100.00"), "47199900011");

    private static final String TOPIC = CONSUMER_PROPERTIES.getProperty("topic");
    private static final String ACK_TOPIC = PRODUCER_BACKPRESSURE_PROPERTIES.getProperty("topic");
    private static final MockConsumer<String, Transaction> CONSUMER = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private static KafkaEventConsumerBackpressure<Transaction> kafkaEventConsumerBackpressure;

    @BeforeEach
    public void beforeAll() {
        List<PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new PartitionInfo(ACK_TOPIC, 0, null, null, null));
        partitions.add(new PartitionInfo(ACK_TOPIC, 1, null, null, null));

        cluster = new Cluster("kafkab", new ArrayList<>(), partitions, emptySet(), emptySet());
        mockAckProducer = new MockProducer<>(cluster, true, new KafkaPartitioner(), new StringSerializer(), new JsonSerializer<>());
        producer = new KafkaEventProducer<>(mockAckProducer, PRODUCER_BACKPRESSURE_PROPERTIES);
        CONSUMER.schedulePollTask(() -> {
            CONSUMER.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
            CONSUMER.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key", transaction));
        });
        CONSUMER.schedulePollTask(CONSUMER::wakeup);
    }

    @Test
    public void testConsumer() throws InterruptedException {
        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        startingOffsets.put(tp, 0L);
        CONSUMER.updateBeginningOffsets(startingOffsets);
        //when
        kafkaEventConsumerBackpressure = new KafkaEventConsumerBackpressure<>(EXECUTOR_SERVICE, producer, CONSUMER, CONSUMER_PROPERTIES);
        kafkaEventConsumerBackpressure.listen();
        kafkaEventConsumerBackpressure.commit();
        Map<TopicPartition, OffsetAndMetadata> map = kafkaEventConsumerBackpressure.getCurrentOffsets();
        long currentOffset = map.get(tp).offset();
        //then
        Assertions.assertEquals(1, currentOffset);
        Assertions.assertTrue(CONSUMER.closed());
        //Проверка отправки события в обратный поток после чтения транзакции
        assertTrue(mockAckProducer.history().size() == 1);
        Ack ack = mockAckProducer.history().get(0).value();
        assertEquals(ack.getEvents().get(0), transaction.getUuid());
    }
}
