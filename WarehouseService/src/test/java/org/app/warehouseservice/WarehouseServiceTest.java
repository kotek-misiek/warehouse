package org.app.warehouseservice;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.warehouse.common.Configuration;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.io.IOException;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WarehouseServiceTest {

    private static final String WAREHOUSE_ID = "WS-01";
    private static final String TEMP_TOPIC = "temperature-topic";
    private static final String HUM_TOPIC = "humidity-topic";
    private static final String BOOTSTRAP = "localhost:9092";
    private static final int TEMP_PORT = 3344;
    private static final int HUM_PORT = 3355;
    private static final int PARTITIONS = 36;
    private static final int REPLICATIONS = 1;

    @Mock private Configuration configuration;
    @Mock private KafkaProducer<String, Message> producer;
    @Mock private AdminClient adminClient;

    private WarehouseService service;

    @BeforeEach
    void setUp() {
        when(configuration.getServerName()).thenReturn(WAREHOUSE_ID);
        when(configuration.getTemperaturePort()).thenReturn(TEMP_PORT);
        when(configuration.getHumidityPort()).thenReturn(HUM_PORT);
        when(configuration.getTemperatureTopic()).thenReturn(TEMP_TOPIC);
        when(configuration.getHumidityTopic()).thenReturn(HUM_TOPIC);
        when(configuration.getKafkaBootstrapServers()).thenReturn(BOOTSTRAP);
        when(configuration.getTopicPartitions()).thenReturn(PARTITIONS);
        when(configuration.getTopicReplications()).thenReturn(REPLICATIONS);

        service = new WarehouseService(configuration, producer, adminClient);
    }

    @Test
    void getPortByTypeReturnsTemperaturePort() {
        assertEquals(TEMP_PORT, service.getPortByType(SensorType.TEMPERATURE));
    }

    @Test
    void getPortByTypeReturnsHumidityPort() {
        assertEquals(HUM_PORT, service.getPortByType(SensorType.HUMIDITY));
    }

    @Test
    void getTopicByTypeReturnsTemperatureTopic() {
        assertEquals(TEMP_TOPIC, service.getTopicByType(SensorType.TEMPERATURE));
    }

    @Test
    void getTopicByTypeReturnsHumidityTopic() {
        assertEquals(HUM_TOPIC, service.getTopicByType(SensorType.HUMIDITY));
    }

    @Test
    void ensureTopicsExistCreatesMissingTopics() {
        var listResult = mock(ListTopicsResult.class);
        var createResult = mock(CreateTopicsResult.class);

        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.createTopics(anyCollection())).thenReturn(createResult);
        when(createResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        service.ensureTopicsExist();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<NewTopic>> captor = ArgumentCaptor.forClass(List.class);
        verify(adminClient).createTopics(captor.capture());

        List<NewTopic> created = captor.getValue();
        assertEquals(2, created.size());
        assertTrue(created.stream().anyMatch(t -> t.name().equals(TEMP_TOPIC) && t.numPartitions() == PARTITIONS));
        assertTrue(created.stream().anyMatch(t -> t.name().equals(HUM_TOPIC) && t.numPartitions() == PARTITIONS));
    }

    @Test
    void ensureTopicsExistDoesNothingWhenTopicsAlreadyExist() {
        var listResult = mock(ListTopicsResult.class);

        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(KafkaFuture.completedFuture(Set.of(TEMP_TOPIC, HUM_TOPIC)));

        service.ensureTopicsExist();

        verify(adminClient, never()).createTopics(anyCollection());
    }

    @Test
    void ensureTopicsExistLogsErrorOnExecutionException() throws Exception {
        var listResult = mock(ListTopicsResult.class);
        KafkaFuture<Set<String>> failed = mock(KafkaFuture.class);

        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(failed);
        when(failed.get()).thenThrow(new ExecutionException("boom", new RuntimeException()));

        assertDoesNotThrow(() -> service.ensureTopicsExist());
        verify(adminClient, never()).createTopics(anyCollection());
    }

    @Test
    void ensureTopicsExistInterruptsOnInterruptedException() throws Exception {
        var listResult = mock(ListTopicsResult.class);
        KafkaFuture<Set<String>> failed = mock(KafkaFuture.class);

        when(adminClient.listTopics()).thenReturn(listResult);
        when(listResult.names()).thenReturn(failed);
        when(failed.get()).thenThrow(new InterruptedException());

        service.ensureTopicsExist();

        assertTrue(Thread.currentThread().isInterrupted(),
                "Thread should be interrupted by ensureTopicsExist");
        Thread.interrupted();
    }

    @Test
    void parseTemperatureMessage() throws Exception {
        var msg = service.parse("sensor_id=T-100; value=40");

        assertEquals(WAREHOUSE_ID, msg.warehouseId());
        assertEquals(SensorType.TEMPERATURE, msg.type());
        assertEquals("T-100", msg.sensorId());
        assertEquals(40, msg.value());
        assertNotNull(msg.timestamp());
    }

    @Test
    void parseHumidityMessage() throws Exception {
        var msg = service.parse("sensor_id=H-200; value=55");

        assertEquals(SensorType.HUMIDITY, msg.type());
        assertEquals("H-200", msg.sensorId());
        assertEquals(55, msg.value());
    }

    @Test
    void parseThrowsOnInvalidMessage() {
        assertThrows(IOException.class, () -> service.parse("garbage"));
    }

    @Test
    void parseTriggersShutdownOnExit() throws Exception {
        var spy = spy(service);

        spy.parse("exit");

        verify(spy, atLeastOnce()).shutdown();
    }

    @Test
    void sendKafkaMessageSendsToCorrectTopicAndKey() throws Exception {
        var msg = new Message(WAREHOUSE_ID, SensorType.TEMPERATURE, "T-1", 42,
                new Timestamp(System.currentTimeMillis()));

        Future<RecordMetadata> future = mock(Future.class);
        var metadata = new RecordMetadata(new TopicPartition(TEMP_TOPIC, 0), 0, 0, 0, 0, 0);
        when(future.get()).thenReturn(metadata);
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        service.sendKafkaMessage(msg);

        ArgumentCaptor<ProducerRecord<String, Message>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());

        ProducerRecord<String, Message> record = captor.getValue();
        assertEquals(TEMP_TOPIC, record.topic());
        assertEquals(WAREHOUSE_ID, record.key(), "key must be warehouseId");
        assertEquals(msg, record.value());
    }

    @Test
    void sendKafkaMessageUsesHumidityTopicForHumiditySensor() throws Exception {
        var msg = new Message(WAREHOUSE_ID, SensorType.HUMIDITY, "H-1", 60,
                new Timestamp(System.currentTimeMillis()));

        Future<RecordMetadata> future = mock(Future.class);
        var metadata = new RecordMetadata(new TopicPartition(HUM_TOPIC, 0), 0, 0, 0, 0, 0);
        when(future.get()).thenReturn(metadata);
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        service.sendKafkaMessage(msg);

        ArgumentCaptor<ProducerRecord<String, Message>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        assertEquals(HUM_TOPIC, captor.getValue().topic());
    }

    @Test
    void sendKafkaMessageDoesNotThrowOnExecutionException() throws Exception {
        var msg = new Message(WAREHOUSE_ID, SensorType.TEMPERATURE, "T-1", 42,
                new Timestamp(System.currentTimeMillis()));

        Future<RecordMetadata> future = mock(Future.class);
        when(future.get()).thenThrow(new ExecutionException("kafka down", new RuntimeException()));
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        assertDoesNotThrow(() -> service.sendKafkaMessage(msg));
    }

    @Test
    void handleClientSendsValidMessageToKafka() throws Exception {
        var socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(null);
        when(socket.getLocalPort()).thenReturn(TEMP_PORT);
        when(socket.getInputStream()).thenReturn(new java.io.ByteArrayInputStream("sensor_id=T-1; value=42".getBytes()));

        Future<RecordMetadata> future = mock(Future.class);
        when(future.get()).thenReturn(
                new RecordMetadata(new TopicPartition(TEMP_TOPIC, 0), 0, 0, 0, 0, 0));
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        service.handleClient(socket);

        verify(producer).send(any(ProducerRecord.class));
    }

    @Test
    void handleClientIgnoresEmptyMessage() throws Exception {
        var socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(null);
        when(socket.getLocalPort()).thenReturn(TEMP_PORT);
        when(socket.getInputStream()).thenReturn(new java.io.ByteArrayInputStream(new byte[0]));

        service.handleClient(socket);

        verify(producer, never()).send(any(ProducerRecord.class));
    }

    @Test
    void handleClientIgnoresMessageOnWrongPort() throws Exception {
        var socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(null);
        when(socket.getLocalPort()).thenReturn(HUM_PORT);
        when(socket.getInputStream()).thenReturn(new java.io.ByteArrayInputStream("sensor_id=T-1; value=42".getBytes()));

        service.handleClient(socket);

        verify(producer, never()).send(any(ProducerRecord.class));
    }

    @Test
    void handleClientIgnoresInvalidMessage() throws Exception {
        var socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(null);
        when(socket.getLocalPort()).thenReturn(TEMP_PORT);
        when(socket.getInputStream()).thenReturn(new java.io.ByteArrayInputStream("garbage".getBytes()));

        assertDoesNotThrow(() -> service.handleClient(socket));
        verify(producer, never()).send(any(ProducerRecord.class));
    }

    @Test
    void shutdownClosesProducerAndAdminClient() {
        service.shutdown();

        verify(producer).close();
        verify(adminClient).close();
    }
}