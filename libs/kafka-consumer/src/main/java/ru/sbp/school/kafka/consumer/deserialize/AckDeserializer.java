package ru.sbp.school.kafka.consumer.deserialize;

import ru.sbp.school.kafka.api.Ack;

public class AckDeserializer extends JsonDeserializer<Ack> {
    @Override
    public Class<Ack> getType() {
        return Ack.class;
    }
}
