package ru.sbp.school.kafka.connect.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbp.school.kafka.model.OperationType;
import ru.sbp.school.kafka.model.Transaction;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransactionStorage {

    private static final Logger log = LoggerFactory.getLogger(TransactionStorage.class);

    private static final String SQL_SELECT_QUERY = "SELECT * FROM TRANSACTION offset ?";
    private final DataSource dataSource;

    public TransactionStorage(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<Transaction> getAll(long offset) {
        List<Transaction> transactions = new ArrayList<>();
        try (PreparedStatement statement = dataSource.prepareStatement(SQL_SELECT_QUERY)) {
            statement.setLong(1, offset);
            if (!statement.execute()) {
                return new ArrayList<>();
            }
            statement.getResultSet();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                Transaction transaction = mapResult(resultSet);
                transactions.add(transaction);
            }
            return transactions;
        } catch (SQLException e) {
            log.error("Ошибка при выгрузке данных из таблицы TRANSACTION: {}", e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private Transaction mapResult(ResultSet resultSet) throws SQLException {
        Transaction transaction = new Transaction();
        transaction.setUuid(UUID.fromString(resultSet.getString("uuid")));
        transaction.setAmount(resultSet.getBigDecimal("amount"));
        transaction.setOperationType(OperationType.valueOf(resultSet.getString("operationType")));
        transaction.setAccount(resultSet.getString("account"));
        transaction.setDate(LocalDateTime.parse(resultSet.getTimestamp("date").toString()));
        return transaction;
    }
}
