package ru.sbp.school.kafka.producer.exception;

public class KafkaProducerException extends RuntimeException {

    public KafkaProducerException() {
    }

    public KafkaProducerException(String message) {
        super(message);
    }

    public KafkaProducerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaProducerException(Throwable cause) {
        super(cause);
    }

    public KafkaProducerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
