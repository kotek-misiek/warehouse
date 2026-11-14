package org.warehouse.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class SensorTypeTest {
    @Test
    void testFromValue() {
        assertEquals(SensorType.TEMPERATURE, SensorType.fromValue("T"));
        assertEquals(SensorType.TEMPERATURE, SensorType.fromValue("t"));
        assertEquals(SensorType.HUMIDITY, SensorType.fromValue("H"));
        assertEquals(SensorType.HUMIDITY, SensorType.fromValue("h"));
    }

    @Test
    void testFromValue_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> SensorType.fromValue("X"));
        assertThrows(IllegalArgumentException.class, () -> SensorType.fromValue(""));
        assertThrows(NullPointerException.class, () -> SensorType.fromValue(null));
    }

    @Test
    void testGetValue() {
        assertEquals("T", SensorType.TEMPERATURE.getValue());
        assertEquals("H", SensorType.HUMIDITY.getValue());
    }

    @Test
    void testEnumValues() {
        var values = SensorType.values();
        assertEquals(2, values.length);
    }

    @Test
    void testValueOf() {
        assertEquals(SensorType.TEMPERATURE, SensorType.valueOf("TEMPERATURE"));
        assertEquals(SensorType.HUMIDITY, SensorType.valueOf("HUMIDITY"));
    }
}
