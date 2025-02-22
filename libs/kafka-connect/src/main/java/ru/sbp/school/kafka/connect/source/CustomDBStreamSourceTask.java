package ru.sbp.school.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import ru.sbp.school.kafka.connect.storage.TransactionStorage;
import ru.sbp.school.kafka.model.Transaction;
import ru.sbp.school.kafka.connect.util.ConnectUtils;
import ru.sbp.school.kafka.connect.storage.DataSource;

import java.util.Collections;

import java.util.*;
import java.util.stream.Collectors;

import static ru.sbp.school.kafka.connect.source.CustomDBStreamSourceConnector.*;
import static ru.sbp.school.kafka.connect.util.ConnectUtils.OFFSET_POSITION;

/**
 * DBStreamSourceTask reads from db.
 */
public class CustomDBStreamSourceTask extends SourceTask {

    public static final String DATABASE_NAME = "db_name";
    private static final Schema TRANSACTION_SCHEMA = ConnectUtils.buildSchema();

    private String tableName;
    private DataSource dataSource;
    private String topic;
    private TransactionStorage storage;

    @Override
    public String version() {
        return new CustomDBStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        dataSource = new DataSource(loadProps(props, DB_URI), loadProps(props, DB_USERNAME), loadProps(props, DB_PASSWORD));
        dataSource.connect();
        storage = new TransactionStorage(dataSource);
        tableName = config.getString(DB_TABLE);
        topic = config.getString(TOPIC);
    }

    @Override
    public List<SourceRecord> poll() {
        long lastOffset = ConnectUtils.lastOffset(context, DATABASE_NAME, tableName);
        return storage.getAll(lastOffset).stream()
                .map(transaction -> mapTransaction(transaction, lastOffset))
                .collect(Collectors.toList());
    }

    private SourceRecord mapTransaction(Transaction transaction, long lastOffset) {
        Map<String, Long> offset = Collections.singletonMap(OFFSET_POSITION, lastOffset);
        return new SourceRecord(
                Collections.singletonMap(DATABASE_NAME, tableName),
                offset,
                topic,
                null,
                null,
                null,
                TRANSACTION_SCHEMA,
                ConnectUtils.mapOnSchema(transaction, TRANSACTION_SCHEMA),
                System.currentTimeMillis()
        );
    }

    @Override
    public void stop() {
        dataSource.disconnect();
    }
}
