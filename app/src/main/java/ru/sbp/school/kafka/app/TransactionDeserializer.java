package ru.sbp.school.kafka.app;


import ru.sbp.school.kafka.consumer.deserialize.JsonDeserializer;
import ru.sbp.school.kafka.model.Transaction;

public class TransactionDeserializer extends JsonDeserializer<Transaction> {

    @Override
    public Class<Transaction> getType() {
        return Transaction.class;
    }
}
