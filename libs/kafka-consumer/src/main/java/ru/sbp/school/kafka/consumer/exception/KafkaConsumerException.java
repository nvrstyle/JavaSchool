package ru.sbp.school.kafka.consumer.exception;

public class KafkaConsumerException extends RuntimeException {

    public KafkaConsumerException() {
    }

    public KafkaConsumerException(String message) {
        super(message);
    }

    public KafkaConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumerException(Throwable cause) {
        super(cause);
    }

    public KafkaConsumerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
