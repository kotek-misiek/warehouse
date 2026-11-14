package org.app.centralservice;

import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import static org.warehouse.common.LogType.WARN;
import static org.warehouse.common.SensorType.TEMPERATURE;
import static org.warehouse.common.LogUtils.log;

public class TemperatureListener implements MessageListener {
    private final int threshold;

    public TemperatureListener(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public SensorType supportedType() {
        return TEMPERATURE;
    }

    @Override
    public void onMessage(Message message) {
        if (message == null) {
            log("Temperature: received null message");
            return;
        }
        if (message.value() > threshold) {
            log(WARN, "Temperature " + message.value() + "°C is too high!");
        }
    }
}
