package org.app.centralservice;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TemperatureListenerTest {
    private static final int THRESHOLD = 30;

    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    private PrintStream originalOut;

    @BeforeEach
    void captureStdout() {
        originalOut = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreStdout() {
        System.setOut(originalOut);
    }

    private Message message(int value) {
        return new Message("WS-01", SensorType.TEMPERATURE, "T-1", value,
                new Timestamp(System.currentTimeMillis()));
    }

    @Test
    void supportedTypeIsTemperature() {
        var listener = new TemperatureListener(THRESHOLD);
        assertEquals(SensorType.TEMPERATURE, listener.supportedType());
    }

    @Test
    void onMessageLogsWarningWhenValueAboveThreshold() {
        var listener = new TemperatureListener(THRESHOLD);
        listener.onMessage(message(THRESHOLD + 1));
        var output = captured.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("WARN"), "Expected WARN level, got: " + output);
        assertTrue(output.contains("Temperature " + (THRESHOLD + 1) + "°C is too high!"), "Expected warning text in: " + output);
    }

    @Test
    void onMessageDoesNotLogWhenValueBelowThreshold() {
        var listener = new TemperatureListener(THRESHOLD);
        listener.onMessage(message(THRESHOLD - 1));
        assertEquals("", captured.toString(StandardCharsets.UTF_8), "No log expected below the threshold");
    }

    @Test
    void onMessageLogsForVeryHighValue() {
        var listener = new TemperatureListener(THRESHOLD);
        listener.onMessage(message(1000));
        var output = captured.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("Temperature 1000°C is too high!"));
    }

    @Test
    void onMessageWithNullValue() {
        var listener = new TemperatureListener(THRESHOLD);
        listener.onMessage(null);
        assertTrue(captured.toString(StandardCharsets.UTF_8).contains("Temperature: received null message"));
    }
}