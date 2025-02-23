package ru.sbp.school.kafka.producer.test;

import org.junit.jupiter.api.Test;
import ru.sbp.school.kafka.producer.test.model.OperationType;
import ru.sbp.school.kafka.producer.test.model.Transaction;
import ru.sbp.school.kafka.utils.json.JsonUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonSerializerTest {

    @Test
    public void serializeTest() {
        //given
        UUID rqUid = UUID.fromString("6d12f8e3-ecba-46aa-8109-0ca2ad212e72");
        LocalDateTime date = LocalDate.of(2024, 01, 21).atStartOfDay();
        String expectedMarshal = "{\"uuid\":\"6d12f8e3-ecba-46aa-8109-0ca2ad212e72\",\"operationType\":\"CREATE\",\"amount\":100.00,\"account\":\"47199900011\",\"date\":[2024,1,21,0,0]}";
        Transaction transaction1 = new Transaction(rqUid, OperationType.CREATE, new BigDecimal("100.00"), "47199900011", date);
        //when
        String marshal = JsonUtils.marshal(transaction1);
        //then
        assertEquals(expectedMarshal, marshal);
    }
}
