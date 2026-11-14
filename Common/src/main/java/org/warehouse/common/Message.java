package org.warehouse.common;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING;

public record Message(@JsonProperty("warehouse_id") String warehouseId,
                      @JsonProperty("sensor_type") SensorType type,
                      @JsonProperty("sensor_id") String sensorId,
                      @JsonProperty("value") int value,
                      @JsonFormat(shape = STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") Timestamp timestamp) {
    @Override
    public String toString() {
        return "warehouse_id=" + warehouseId
                + "; sensor_type=" + (type == null ? "null" : type.getValue())
                + "; sensor_id=" + sensorId + "; value=" + value
                + "; timestamp=" + timestamp;
    }
}
