package org.warehouse.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogUtils {
    private static final DateTimeFormatter TIME_TEMPLATE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void log(LogType type, String msg) {
        System.out.println(type + ": [" + LocalDateTime.now().format(TIME_TEMPLATE) + "] " + msg);
    }

    public static void log(String msg) {
        System.out.println(LogType.INFO + ": [" + LocalDateTime.now().format(TIME_TEMPLATE) + "] " + msg);
    }

    public static void log() {
        System.out.println(LogType.INFO + ": [" + LocalDateTime.now().format(TIME_TEMPLATE) + "] ");
    }
}
