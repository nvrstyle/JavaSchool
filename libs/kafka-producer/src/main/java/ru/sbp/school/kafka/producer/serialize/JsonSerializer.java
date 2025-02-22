package ru.sbp.school.kafka.producer.serialize;

import org.apache.kafka.common.serialization.Serializer;
import ru.sbp.school.kafka.utils.json.JsonUtils;

import java.nio.charset.StandardCharsets;

public class JsonSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T object) {
        if (object == null) {
            return new byte[0];
        }
        String json = JsonUtils.marshal(object);
        JsonUtils.validate(json, object.getClass());
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
