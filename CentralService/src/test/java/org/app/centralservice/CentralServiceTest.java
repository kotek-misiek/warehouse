package org.app.centralservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.warehouse.common.Configuration;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CentralServiceTest {
    private static final String TEMP_TOPIC = "temperature-topic";
    private static final String HUM_TOPIC  = "humidity-topic";

    @Mock private Configuration configuration;
    @Mock private KafkaConsumer<String, Message> consumer;
    @Mock private MessageListener temperatureListener;
    @Mock private MessageListener humidityListener;

    private CentralService service;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(configuration.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        lenient().when(configuration.getGroupId()).thenReturn("test-group");
        lenient().when(configuration.getTemperatureTopic()).thenReturn(TEMP_TOPIC);
        lenient().when(configuration.getHumidityTopic()).thenReturn(HUM_TOPIC);

        lenient().when(temperatureListener.supportedType()).thenReturn(SensorType.TEMPERATURE);
        lenient().when(humidityListener.supportedType()).thenReturn(SensorType.HUMIDITY);

        service = new CentralService(configuration, List.of(temperatureListener, humidityListener));
        injectConsumer(service, consumer);
    }

    @Test
    void startFinishesWithNoWorker() throws Exception {
        readIsRunning(service).set(true);
        service.start();

        assertNull(readWorker(service));
    }

    @Test
    void closeBeforeStartDoesNothing() {
        service.close();

        verify(consumer, never()).wakeup();
        verify(consumer, never()).close();
    }

    @Test
    void closeAfterStartWakesUpAndClosesConsumer() {
        doNothing().when(consumer).subscribe(anyList());
        when(consumer.poll(any())).thenReturn(emptyRecords());

        service.start();
        verify(consumer, timeout(2000)).subscribe(anyList());

        service.close();

        verify(consumer, timeout(2000)).wakeup();
        verify(consumer, timeout(2000)).close();
    }

    @Test
    void closeAfterStartResetsIsRunningFlag() throws Exception {
        doNothing().when(consumer).subscribe(anyList());
        when(consumer.poll(any())).thenReturn(emptyRecords());

        service.start();
        verify(consumer, timeout(2000)).subscribe(anyList());

        assertTrue(readIsRunning(service).get(), "isRunning must be true after start()");

        service.close();

        assertFalse(readIsRunning(service).get(), "isRunning must be false after close()");
    }

    @Test
    void pollLoopLogsErrorOnUnexpectedWakeup() {
        when(consumer.poll(any())).thenThrow(new WakeupException());
        doNothing().when(consumer).subscribe(anyList());

        var captured = new ByteArrayOutputStream();
        var originalOut = System.out;
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            service.start();

            verify(consumer, timeout(2000)).close();

            var output = captured.toString(StandardCharsets.UTF_8);
            assertTrue(output.contains("ERROR"),
                    "Expected ERROR level, got: " + output);
            assertTrue(output.contains("Unexpected WakeupException"),
                    "Expected 'Unexpected WakeupException' in log, got: " + output);
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void closeInterruptsCurrentThreadOnInterruptedException() {
        doNothing().when(consumer).subscribe(anyList());
        doNothing().when(consumer).wakeup();

        service.start();
        verify(consumer, timeout(2000)).subscribe(anyList());

        Thread.currentThread().interrupt();

        try {
            service.close();
            assertTrue(Thread.currentThread().isInterrupted(),
                    "close() must restore the interrupt flag via Thread.currentThread().interrupt()");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void proceedIgnoresNullMessageValue() throws Exception {
        ConsumerRecord<String, Message> nullRecord =
                new ConsumerRecord<>(TEMP_TOPIC, 0, 0L, "WS-01", null);
        ConsumerRecords<String, Message> batch = new ConsumerRecords<>(
                Map.of(new TopicPartition(TEMP_TOPIC, 0), List.of(nullRecord)));

        var processed = new CountDownLatch(1);
        when(consumer.poll(any()))
                .thenReturn(batch)
                .thenAnswer(inv -> { processed.countDown(); return emptyRecords(); });

        service.start();
        assertTrue(processed.await(2, TimeUnit.SECONDS));

        verify(temperatureListener, never()).onMessage(any());
        verify(humidityListener, never()).onMessage(any());

        service.close();
    }

    @Test
    void proceedIgnoresMessageWithoutMatchingListener() throws Exception {
        var onlyHumidity = new CentralService(configuration, List.of(humidityListener));
        try (onlyHumidity) {
            injectConsumer(onlyHumidity, consumer);
            var msg = temperatureMessage(42);
            var batch = singleRecord(TEMP_TOPIC, msg);

            var processed = new CountDownLatch(1);
            when(consumer.poll(any())).thenReturn(batch)
                    .thenAnswer(inv -> {
                        processed.countDown();
                        return emptyRecords();
                    });
            onlyHumidity.start();
            assertTrue(processed.await(2, TimeUnit.SECONDS));

            verify(humidityListener, never()).onMessage(any());
            verify(consumer, timeout(1000)).commitSync();
        }
    }

    @Test
    void proceedCallsListenerForEachRecordInBatch() throws Exception {
        var first  = temperatureMessage(10);
        var second = temperatureMessage(20);
        var third  = temperatureMessage(30);
        ConsumerRecords<String, Message> batch = new ConsumerRecords<>(
                Map.of(new TopicPartition(TEMP_TOPIC, 0),
                        List.of(record(TEMP_TOPIC, first),
                                record(TEMP_TOPIC, second),
                                record(TEMP_TOPIC, third))));

        var processed = new CountDownLatch(1);
        when(consumer.poll(any())).thenReturn(batch)
                .thenAnswer(inv -> {
                    processed.countDown();
                    return emptyRecords();
                });

        service.start();
        assertTrue(processed.await(2, TimeUnit.SECONDS));

        verify(temperatureListener, timeout(1000)).onMessage(first);
        verify(temperatureListener, timeout(1000)).onMessage(second);
        verify(temperatureListener, timeout(1000)).onMessage(third);

        service.close();
    }

    private static void injectConsumer(CentralService target,
                                       KafkaConsumer<String, Message> mock) throws Exception {
        var field = CentralService.class.getDeclaredField("consumer");
        field.setAccessible(true);
        field.set(target, mock);
    }

    private static AtomicBoolean readIsRunning(CentralService target) throws Exception {
        var field = CentralService.class.getDeclaredField("isRunning");
        field.setAccessible(true);
        return (AtomicBoolean) field.get(target);
    }

    private static Thread readWorker(CentralService target) throws Exception {
        var field = CentralService.class.getDeclaredField("worker");
        field.setAccessible(true);
        return (Thread) field.get(target);
    }

    private Message temperatureMessage(int value) {
        return new Message("WS-01", SensorType.TEMPERATURE, "T-1", value,
                new Timestamp(System.currentTimeMillis()));
    }

    private ConsumerRecord<String, Message> record(String topic, Message msg) {
        return new ConsumerRecord<>(topic, 0, 0L, "WS-01", msg);
    }

    private ConsumerRecords<String, Message> singleRecord(String topic, Message msg) {
        return new ConsumerRecords<>(Map.of(new TopicPartition(topic, 0), List.of(record(topic, msg))));
    }

    private ConsumerRecords<String, Message> emptyRecords() {
        return ConsumerRecords.empty();
    }
}