package ru.sbp.school.kafka.consumer.deserialize;

import org.apache.kafka.common.serialization.Deserializer;
import ru.sbp.school.kafka.utils.json.JsonUtils;

public abstract class JsonDeserializer<T> implements Deserializer<T> {

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalStateException("Событие Kafka не содержит данных");
        }
        JsonUtils.validate(data, getType());
        return JsonUtils.unmarshall(data, getType());
    }

    public abstract Class<T> getType();
}
