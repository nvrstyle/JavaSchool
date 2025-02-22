package ru.sbp.school.kafka.connect.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class DataSource {

    private static final Logger log = LoggerFactory.getLogger(DataSource.class);

    private final String uri;
    private final String user;
    private final String password;
    private Connection connection;

    public DataSource(String uri, String user, String password) {
        this.uri = uri;
        this.user = user;
        this.password = password;
    }

    public void connect() {
        try {
            connection = DriverManager.getConnection(uri, user,password);
            log.info("Уставнолено подключение в базе данных");
        } catch (SQLException ex) {
            log.error("Ошибка при подключении к базе данных в кафка-коннекторе", ex);
            throw new IllegalStateException(ex);
        }
    }

    public void disconnect() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error("Ошибка закрытия соединения с базой данных {}: {}", uri, e.getMessage(), e);
            throw new IllegalStateException("Ошибка закрытия соединения с базой данных", e);
        }
    }

    public PreparedStatement prepareStatement(String sql) {
        if (connection == null) {
            throw new IllegalStateException("Отсутствует подключение к базе данных");
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            return preparedStatement;
        } catch (SQLException e) {
            log.error("Возникла ошибка при выполнении SQL запроса {}: {}", sql, e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

}
