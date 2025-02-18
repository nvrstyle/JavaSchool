package ru.sbp.school.kafka.utils.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtils.class);

    /**
     * метод загружает конфигурацию Kafka из файла в объект
     * @param path - имя файла конфигурации
     * @return configuration - экземпляр класса Properties
     */
    public static Properties load(String path) {
        Properties configuration = new Properties();
        try(InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream(path)) {
            configuration.load(is);
        } catch (Exception e) {
            log.error("Ошибка чтения файла конфигурации: {}", e.getMessage(), e);
            throw new IllegalStateException(e);
        }
        return configuration;
    }
}
