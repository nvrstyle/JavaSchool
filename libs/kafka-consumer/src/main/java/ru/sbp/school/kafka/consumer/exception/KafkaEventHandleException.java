package ru.sbp.school.kafka.consumer.exception;

public class KafkaEventHandleException extends RuntimeException {
    public KafkaEventHandleException() {
    }

    public KafkaEventHandleException(String message) {
        super(message);
    }

    public KafkaEventHandleException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaEventHandleException(Throwable cause) {
        super(cause);
    }

    public KafkaEventHandleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
