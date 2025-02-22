package ru.sbp.school.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.utils.resource.PropertiesUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * Very simple source connector that works with db
 */
public class CustomDBStreamSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(CustomDBStreamSourceConnector.class);

    private static final Properties PROPERTIES = PropertiesUtils.load("kafka-connect.properties");

    public static final String TOPIC = PROPERTIES.getProperty("kafka.topic");
    public static final String DB_URI = PROPERTIES.getProperty("h2.db.uri");
    public static final String DB_USERNAME = PROPERTIES.getProperty("h2.db.username");
    public static final String DB_PASSWORD = PROPERTIES.getProperty("h2.db.password");
    public static final String DB_TABLE = PROPERTIES.getProperty("h2.db.table");
    public static final String BATCH_SIZE = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 1000;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC, Type.STRING, NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH, "The topic to publish data to")
            .define(DB_URI, Type.STRING, null, Importance.HIGH, "Source DB URL")
            .define(DB_USERNAME, Type.STRING, null, Importance.HIGH, "Source DB USERNAME")
            .define(DB_PASSWORD, Type.STRING, null, Importance.HIGH, "Source DB PASSWORD")
            .define(DB_TABLE, Type.STRING, null, Importance.HIGH, "Source DB table name")
            .define(BATCH_SIZE, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
                    "The maximum number of records the source task can read from the file each time it is polled");

    private Map<String, String> props;

    public static String loadProps(Map<String, String> props, String propsName) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        return config.getString(propsName);
    }


    @Override
    public void start(Map<String, String> map) {
        this.props = map;
        log.info("Запущен коннектор чтения потока данных из БД {}", new AbstractConfig(CONFIG_DEF, props).getString(DB_URI));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomDBStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
