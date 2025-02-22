package ru.sbp.school.kafka.connect.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTaskContext;
import ru.sbp.school.kafka.model.Transaction;

import java.util.Collections;
import java.util.Map;

public class ConnectUtils {

    public static final String OFFSET_POSITION = "position";

    public static Long lastOffset(SourceTaskContext context, String key, String value) {
        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(key, value));
        if (offset == null) {
            return 0L;
        }
        Object lastOffset = offset.get(OFFSET_POSITION);
        if (lastOffset == null) {
            return 0L;
        }
        if ((lastOffset instanceof Long)) {
            return (Long) lastOffset;
        }
        throw new ConnectException("Позиция оффсета некорректного типа: %s".formatted(lastOffset.getClass().getName()));
    }

    public static Schema buildSchema() {
        return SchemaBuilder.struct()
                .field("uuid", Schema.STRING_SCHEMA)
                .field("operationType", Schema.STRING_SCHEMA)
                .field("amount", Schema.STRING_SCHEMA)
                .field("account", Schema.STRING_SCHEMA)
                .field("date", Schema.STRING_SCHEMA)
                .build();
    }

    public static Struct mapOnSchema(Transaction transaction, Schema schema) {
        Struct struct = new Struct(schema);
        struct.put("uuid", transaction.getUuid());
        struct.put("operationType", transaction.getOperationType().toString());
        struct.put("amount", transaction.getAmount().toString());
        struct.put("account", transaction.getAccount());
        struct.put("date", transaction.getDate());
        return struct;
    }


}
