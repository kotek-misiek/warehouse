package org.app.warehouseservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.lang.reflect.Field;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class MessageSerializerTest {
    private final MessageSerializer serializer = new MessageSerializer();

    @Test
    void testSerializeValidMessage() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "s1", 25, new Timestamp(System.currentTimeMillis()));
        var bytes = serializer.serialize("test-topic", message);

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        var json = new String(bytes);
        assertTrue(json.contains("wh-1"));
        assertTrue(json.contains("s1"));
        assertTrue(json.contains("25"));
    }

    @Test
    void testSerializeHumidityMessage() {
        var message = new Message("wh-2", SensorType.HUMIDITY, "h1", 80, new Timestamp(System.currentTimeMillis()));
        var bytes = serializer.serialize("test-topic", message);

        assertNotNull(bytes);
        var json = new String(bytes);
        assertTrue(json.contains("wh-2"));
        assertTrue(json.contains("h1"));
        assertTrue(json.contains("80"));
    }

    @Test
    void testSerializeReturnsNullForNullMessage() {
        var bytes = serializer.serialize("test-topic", null);
        assertNull(bytes);
    }

    @Test
    void testSerializeWithDifferentTopic() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "s1", 30, new Timestamp(System.currentTimeMillis()));
        var bytes1 = serializer.serialize("topic-1", message);
        var bytes2 = serializer.serialize("topic-2", message);

        assertArrayEquals(bytes1, bytes2);
    }

    @Test
    void testSerializeContainsAllFields() {
        var ts = new Timestamp(1234567890000L);
        var message = new Message("test-warehouse", SensorType.TEMPERATURE, "sensor-1", 42, ts);
        var bytes = serializer.serialize("test-topic", message);

        var json = new String(bytes);
        assertTrue(json.contains("warehouse_id"));
        assertTrue(json.contains("sensor_type"));
        assertTrue(json.contains("sensor_id"));
        assertTrue(json.contains("value"));
        assertTrue(json.contains("timestamp"));
    }

    @Test
    void testSerializeZeroValue() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "s1", 0, new Timestamp(System.currentTimeMillis()));
        var bytes = serializer.serialize("test-topic", message);

        assertNotNull(bytes);
        var json = new String(bytes);
        assertTrue(json.contains("0"));
    }

    @Test
    void testSerializeNegativeValue() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "s1", -10, new Timestamp(System.currentTimeMillis()));
        var bytes = serializer.serialize("test-topic", message);

        assertNotNull(bytes);
        var json = new String(bytes);
        assertTrue(json.contains("-10"));
    }

    @Test
    void testSerializeLargeValue() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "s1", 999999, new Timestamp(System.currentTimeMillis()));
        var bytes = serializer.serialize("test-topic", message);

        assertNotNull(bytes);
        var json = new String(bytes);
        assertTrue(json.contains("999999"));
    }

    @Test
    void testSerializeThrowsRuntimeExceptionWhenObjectMapperFails() throws Exception {
        var throwingMapper = new ObjectMapper() {
            @Override
            public byte[] writeValueAsBytes(Object value) {
                throw new IllegalArgumentException("Test serialization failure");
            }
        };

        var serializer = new MessageSerializer();
        Field field = MessageSerializer.class.getDeclaredField("objectMapper");
        field.setAccessible(true);
        field.set(serializer, throwingMapper);

        var message = new Message("WH-1", SensorType.TEMPERATURE, "s1", 25, new Timestamp(System.currentTimeMillis()));

        var exception = assertThrows(RuntimeException.class, () -> serializer.serialize("test-topic", message));

        assertTrue(exception.getMessage().contains("Cannot serialize Message to JSON"));
        assertEquals(IllegalArgumentException.class, exception.getCause().getClass());
        assertEquals("Test serialization failure", exception.getCause().getMessage());
    }

    @Test
    void testSerializeNullDoesNotThrow() {
        var serializer = new MessageSerializer();
        assertNull(serializer.serialize("test-topic", null));
    }
}
