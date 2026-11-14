package org.app.warehouseservice;

import org.warehouse.common.Configuration;

import java.io.IOException;

import static org.warehouse.common.LogType.ERROR;
import static org.warehouse.common.LogUtils.log;

public class Main {
    public static String serverName = "WS-01";

    public static void main(String[] args) {
        if (args.length > 1) {
            if (args[0].equals("-name")) {
                serverName = args[1];
            }
        }
        log("Welcome to Warehouse");
        var service = new WarehouseService(new Configuration(serverName));

        Runtime.getRuntime().addShutdownHook(new Thread(service::shutdown, "shutdown-hook"));

        try {
            service.run();
        } catch (IOException e) {
            log(ERROR, "Cannot start WarehouseService: " + e.getMessage());
        }
    }
}
