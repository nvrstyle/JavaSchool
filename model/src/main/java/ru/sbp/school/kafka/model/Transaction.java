package ru.sbp.school.kafka.model;


import ru.sbp.school.kafka.api.Identifiable;
import ru.sbp.school.kafka.api.PartitionKey;
import ru.sbp.school.kafka.api.Partitionable;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

import static ru.sbp.school.kafka.model.OperationType.*;

public class Transaction implements Partitionable, Identifiable {

    private UUID uuid;
    private OperationType operationType;
    private BigDecimal amount;
    private String account;
    private LocalDateTime date;

    public Transaction() {
    }

    public Transaction(OperationType operationType, BigDecimal amount, String account, LocalDateTime date) {
        this.uuid = UUID.randomUUID();
        this.operationType = operationType;
        this.amount = amount;
        this.account = account;
        this.date = date;
    }

    public static Transaction create(BigDecimal amount, String account) {
        return new Transaction(CREATE, amount, account, LocalDateTime.now());
    }

    public Transaction update(BigDecimal amount, String account) {
        return new Transaction(UPDATE, amount, account, LocalDateTime.now());
    }

    public Transaction delete(BigDecimal amount, String account) {
        return new Transaction(DELETE, amount, account, LocalDateTime.now());
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return operationType == that.operationType && Objects.equals(amount, that.amount) && Objects.equals(account, that.account) && Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationType, amount, account, date);
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "uuid=" + uuid +
                ", operationType=" + operationType +
                ", amount=" + amount +
                ", account='" + account + '\'' +
                ", date=" + date +
                '}';
    }

    @Override
    public PartitionKey partitionKey() {
        return operationType.partitionKey();
    }

}
