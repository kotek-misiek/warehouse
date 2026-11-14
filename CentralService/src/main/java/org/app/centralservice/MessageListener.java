package org.app.centralservice;

import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

public interface MessageListener {
    SensorType supportedType();
    void onMessage(Message message);
}
