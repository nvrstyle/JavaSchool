package ru.sbp.school.kafka.producer.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.api.Ack;
import ru.sbp.school.kafka.api.Identifiable;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Сервис подтверждения доставки события
 */
public class AckCommiter {


    private static final Logger log = LoggerFactory.getLogger(AckCommiter.class);

    private final Map<UUID, Ack> commitMap = new ConcurrentHashMap<>();
    private final Map<UUID, Identifiable> waitCommitMap = new ConcurrentHashMap<>();
    private final Long commitTimeOut;

    public AckCommiter(ScheduledExecutorService scheduledService, Long commitTimeOut, Consumer<Identifiable> backOffCallBack) {
        this.commitTimeOut = commitTimeOut;
        scheduledService.schedule(() -> checkCommit(backOffCallBack), commitTimeOut, TimeUnit.MILLISECONDS);
    }

    public void commit(Ack ack) {
        commitMap.put(ack.getUuid(), ack);
        waitCommitMap.remove(ack.getUuid());
        log.info("Событие uuid = {} успешно доставлено в {}", ack.getUuid(), ack.getTimeStamp());
    }

    public void waitCommit(Identifiable event) {
        waitCommitMap.putIfAbsent(event.getUuid(), event);
        log.info("Событие uuid = {} добавлено в очередь ожидания подтверждения доставки", event.getUuid());
    }

    public boolean isCommited(Identifiable event) {
        return commitMap.get(event.getUuid()) != null;
    }

    private void checkCommit(Consumer<Identifiable> backOffCallBack) {
        waitCommitMap.values().stream()
                .filter(event -> !isCommited(event) || isOverdueAsk(event))
                .forEach(backOffCallBack);
    }

    private boolean isOverdueAsk(Identifiable event) {
        return Optional.ofNullable(commitMap.get(event.getUuid()))
                .map(ask -> ask.isOverdue(commitTimeOut))
                .orElse(Boolean.TRUE);
    }
}
