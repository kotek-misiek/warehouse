package org.warehouse.common;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.warehouse.common.LogUtils.log;

class LogUtilsTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    private void setOut() {
        System.setOut(new PrintStream(outContent));
    }

    private void restoreOut() {
        System.setOut(originalOut);
    }

    @Test
    void testLogWithType() {
        setOut();
        try {
            log(LogType.ERROR, "test message");
            var output = outContent.toString();
            assertTrue(output.contains("ERROR"));
            assertTrue(output.contains("test message"));
            assertTrue(output.contains("INFO") || output.contains("WARN") || output.contains("ERROR"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogWithType_Info() {
        setOut();
        try {
            log(LogType.INFO, "info message");
            var output = outContent.toString();
            assertTrue(output.contains("INFO"));
            assertTrue(output.contains("info message"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogWithType_Warn() {
        setOut();
        try {
            log(LogType.WARN, "warn message");
            var output = outContent.toString();
            assertTrue(output.contains("WARN"));
            assertTrue(output.contains("warn message"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogWithType_Error() {
        setOut();
        try {
            log(LogType.ERROR, "error message");
            var output = outContent.toString();
            assertTrue(output.contains("ERROR"));
            assertTrue(output.contains("error message"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogWithMessage() {
        setOut();
        try {
            log("simple message");
            var output = outContent.toString();
            assertTrue(output.contains("INFO"));
            assertTrue(output.contains("simple message"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogEmpty() {
        setOut();
        try {
            log();
            var output = outContent.toString();
            assertTrue(output.contains("INFO"));
        } finally {
            restoreOut();
        }
    }

    @Test
    void testLogIncludesTimestamp() {
        setOut();
        try {
            log("timestamp test");
            var output = outContent.toString();
            var pattern  = Pattern.compile("^INFO:\\s\\[\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d{3}].+");
            assertTrue(pattern.matcher(output).find(), "Output should contain timestamp components: " + output);
        } finally {
            restoreOut();
        }
    }
}
