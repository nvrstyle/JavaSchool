package ru.sbp.school.kafka.utils.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class JsonUtils {

    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
    }

    public static <T> String marshal(T object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Ошибка сериализации класса {}: {}", object.getClass().getName(),  e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    public static <T> void validate(String json, Class<T> clazz) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        JsonSchema jsonSchema = factory.getSchema(clazz.getResourceAsStream("/schema/" + clazz.getSimpleName() + ".json"));
        JsonNode jsonNode = getJsonNode(json);
        Set<ValidationMessage> messages = jsonSchema.validate(jsonNode);
        if (!messages.isEmpty()) {
            throw new IllegalStateException("Ошибка валидации JSON по схеме : " + String.join(";", messages.stream()
                            .map(ValidationMessage::getMessage)
                            .collect(Collectors.toSet())
                    )
            );
        }
    }

    private static JsonNode getJsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            log.error("Ошибка обработки JSON объекта ({}) : {}", json, e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }
}
