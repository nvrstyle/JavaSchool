package ru.sbp.school.kafka.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.sbp.school.kafka.consumer.KafkaEventConsumer;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class KafkaConsumerTest {

    private static final Properties PROPERTIES = PropertiesUtils.load("kafka-consumer.properties");
    private static final String TOPIC = PROPERTIES.getProperty("topic");
    private static final MockConsumer<String, Transaction> CONSUMER = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2);
    private static KafkaEventConsumer<Transaction> kafkaEventConsumer;

    @BeforeEach
    public void beforeAll() {
        CONSUMER.schedulePollTask(() -> {
            CONSUMER.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
            CONSUMER.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "key", new Transaction()));
        });
        CONSUMER.schedulePollTask(CONSUMER::wakeup);
    }

    @Test
    public void testConsumer() {
        HashMap<TopicPartition, Long> startingOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        startingOffsets.put(tp, 0L);
        CONSUMER.updateBeginningOffsets(startingOffsets);
        //when
        kafkaEventConsumer = new KafkaEventConsumer<>(EXECUTOR_SERVICE, CONSUMER, PROPERTIES);
        kafkaEventConsumer.listen();
        Map<TopicPartition, OffsetAndMetadata> map = kafkaEventConsumer.getCurrentOffsets();
        long currentOffset = map.get(tp).offset();
        //then
        Assertions.assertEquals(1, currentOffset);
        Assertions.assertTrue(CONSUMER.closed());
    }
}
