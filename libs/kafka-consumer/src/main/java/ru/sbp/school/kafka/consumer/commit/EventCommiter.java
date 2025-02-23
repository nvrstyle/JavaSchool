package ru.sbp.school.kafka.consumer.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.api.Identifiable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Сервис подтверждения доставки события
 */
public class EventCommiter {


    private static final Logger log = LoggerFactory.getLogger(EventCommiter.class);

    private final Map<UUID, Identifiable> waitCommitMap = new ConcurrentHashMap<>();
    private final Long commitTimeOut;
    private final Consumer<Ack> backOffCallBack;

    public EventCommiter(Long commitTimeOut, Consumer<Ack> backOffCallBack) {
        this.commitTimeOut = commitTimeOut;
        this.backOffCallBack = backOffCallBack;
    }

    public void waitCommit(Identifiable event) {
        waitCommitMap.putIfAbsent(event.getUuid(), event);
        log.info("Событие uuid = {} добавлено в очередь ожидания подтверждения доставки", event.getUuid());
    }

    public void commit(Consumer<Ack> backOffCallBack) {
        Ack acknowledge = Ack.acknowledge(waitCommitMap.values().stream().toList());
        waitCommitMap.clear();
        backOffCallBack.accept(acknowledge);
    }

    public void scheduleCommit(ScheduledExecutorService scheduledService) {
        scheduledService.schedule(() -> commit(backOffCallBack), commitTimeOut, TimeUnit.MILLISECONDS);
    }
}
