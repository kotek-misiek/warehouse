package org.app.centralservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.warehouse.common.Message;

public class MessageDeserializer implements Deserializer<Message> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, Message.class);
        } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize JSON to Message: " + new String(data), e);
        }
    }
}
