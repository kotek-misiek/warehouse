package org.warehouse.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SensorType {
    @JsonProperty("T") TEMPERATURE("T"),
    @JsonProperty("H") HUMIDITY("H");

    private final String value;

    public static SensorType fromValue(String value) {
        return switch (value) {
            case "T", "t" -> TEMPERATURE;
            case "H", "h" -> HUMIDITY;
            default -> throw new IllegalArgumentException("Invalid sensor type: " + value);
        };
    }

    SensorType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
