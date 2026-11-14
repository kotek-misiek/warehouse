package org.app.centralservice;

import org.junit.jupiter.api.Test;
import org.warehouse.common.Message;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.warehouse.common.SensorType.TEMPERATURE;

class MessageDeserializerTest {
    private static final Message NULL_MESSAGE = new Message(null, null, null, 0, null);

    private final MessageDeserializer deserializer = new MessageDeserializer();

    private static byte[] bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void deserializeReturnsNullForNullBytes() {
        assertNull(deserializer.deserialize("temperature-topic", null));
    }

    @Test
    void deserializeValidTemperatureMessage() {
        var json = """
                {
                  "warehouse_id": "WS-01",
                  "sensor_type": "T",
                  "sensor_id": "T-1",
                  "value": 42,
                  "timestamp": "2026-11-14T12:45:33.101Z"
                }
                """;
        var msg = deserializer.deserialize("temperature-topic", bytes(json));

        assertNotNull(msg);
        assertEquals("WS-01", msg.warehouseId());
        assertEquals(TEMPERATURE, msg.type());
        assertEquals("T-1", msg.sensorId());
        assertEquals(42, msg.value());
        assertNotNull(msg.timestamp());
    }

    @Test
    void deserializeEmptyObjectReturnsNullMessege() {
        var json = "{}";
        var result = deserializer.deserialize("temperature-topic", bytes(json));
        assertEquals(NULL_MESSAGE, result);
    }

    @Test
    void deserializeMissingRequiredFieldFails() {
        var json = """
                {
                  "warehouse_id": "WS-01",
                  "sensor_type": "T",
                  "sensor_id": "T-1"
                }
                """;
        var result = deserializer.deserialize("temperature-topic", bytes(json));
        assertNotNull(result);
        assertEquals("WS-01", result.warehouseId());
        assertEquals(TEMPERATURE, result.type());
        assertEquals("T-1", result.sensorId());
        assertEquals(0, result.value());
        assertNull(result.timestamp());
    }

    @Test
    void deserializeUnknownSensorTypeFails() {
        var json = """
                {
                  "warehouse_id": "WS-01",
                  "sensor_type": "U",
                  "sensor_id": "X-1",
                  "value": 10,
                  "timestamp": "2026-11-14T12:45:33.101Z"
                }
                """;
        assertThrows(RuntimeException.class,
                () -> deserializer.deserialize("temperature-topic", bytes(json)));
    }

    @Test
    void deserializeWrongValueTypeFails() {
        var json = """
                {
                  "warehouse_id": "WS-01",
                  "sensor_type": "T",
                  "sensor_id": "T-1",
                  "value": "not-a-number",
                  "timestamp": "2026-11-14T12:45:33.101Z"
                }
                """;
        assertThrows(RuntimeException.class,
                () -> deserializer.deserialize("temperature-topic", bytes(json)));
    }

    @Test
    void deserializeEmptyByteArrayFails() {
        assertThrows(RuntimeException.class,
                () -> deserializer.deserialize("temperature-topic", new byte[0]));
    }

    @Test
    void deserializeInvalidUtf8DoesNotCrashUnexpectedly() {
        var garbage = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFD};
        assertThrows(RuntimeException.class,
                () -> deserializer.deserialize("temperature-topic", garbage));
    }
}