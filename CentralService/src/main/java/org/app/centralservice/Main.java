package org.app.centralservice;

import org.warehouse.common.Configuration;

import java.util.List;

import static org.warehouse.common.LogUtils.log;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        log("Welcome to Central Service");
        var configuration = new Configuration("central-service");
        var listeners = List.of(
                new TemperatureListener(configuration.getTemperatureThreshold()),
                new HumidityListener(configuration.getHumidityThreshold()));
        var consumer = new CentralService(configuration, listeners);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close, "shutdown-hook"));
        consumer.start();
        Thread.currentThread().join();
    }
}
