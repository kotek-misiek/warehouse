package org.warehouse.common;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    private final Timestamp timestamp = new Timestamp(1794672291788L);

    @Test
    void testMessageConstruction() {
        var message = new Message("wh-1", SensorType.TEMPERATURE, "sensor-1", 25, timestamp);

        assertEquals("wh-1", message.warehouseId());
        assertEquals(SensorType.TEMPERATURE, message.type());
        assertEquals("sensor-1", message.sensorId());
        assertEquals(25, message.value());
        assertEquals(timestamp, message.timestamp());
    }
    @Test
    void testToString() {
        var message = new Message("WH-1", SensorType.TEMPERATURE, "s1", 42, timestamp);
        var str = message.toString();

        var expected = "warehouse_id=WH-1; sensor_type=T; sensor_id=s1; value=42; timestamp=2026-11-14 19:04:51.788";
        assertEquals(expected, str);
    }
}
