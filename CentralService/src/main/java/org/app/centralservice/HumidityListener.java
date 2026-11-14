package org.app.centralservice;

import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import static org.warehouse.common.LogType.WARN;
import static org.warehouse.common.SensorType.HUMIDITY;
import static org.warehouse.common.LogUtils.log;

public class HumidityListener implements MessageListener {
    private final int threshold;

    public HumidityListener(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public SensorType supportedType() {
        return HUMIDITY;
    }

    @Override
    public void onMessage(Message message) {
        if (message == null) {
            log("Humidity: received null message");
            return;
        }
        if (message.value() > threshold) {
            log(WARN, "Humidity " + message.value() + "% is too high!");
        }
    }
}
