package org.app.warehouseservice;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.warehouse.common.Configuration;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.warehouse.common.LogType.ERROR;
import static org.warehouse.common.LogUtils.log;

public class WarehouseService {
    private final String warehouseId;

    private final int temperaturePort;
    private final int humidityPort;

    private final String temperatureTopic;
    private final String humidityTopic;

    private final String kafkaBootstrapServers;

    private final int topicPartitions;
    private final int topicReplications;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private Thread mainThread;

    private final KafkaProducer<String, Message> producer;
    private final AdminClient adminClient;

    public WarehouseService(Configuration configuration) {
        this(configuration, createProducer(configuration.getKafkaBootstrapServers()),
             createAdminClient(configuration.getKafkaBootstrapServers()));
    }

    protected WarehouseService(Configuration configuration,
                               KafkaProducer<String, Message> producer,
                               AdminClient adminClient) {
        this.warehouseId = configuration.getServerName();

        this.temperaturePort = configuration.getTemperaturePort();
        this.humidityPort = configuration.getHumidityPort();

        this.temperatureTopic = configuration.getTemperatureTopic();
        this.humidityTopic = configuration.getHumidityTopic();

        this.kafkaBootstrapServers = configuration.getKafkaBootstrapServers();

        this.topicPartitions = configuration.getTopicPartitions();
        this.topicReplications = configuration.getTopicReplications();

        this.producer = producer;
        this.adminClient = adminClient;
    }

    protected static KafkaProducer<String, Message> createProducer(String bootstrapServers) {
        var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    protected static AdminClient createAdminClient(String bootstrapServers) {
        var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(adminProps);
    }

    public int getPortByType(SensorType type) {
        return switch (type) {
            case TEMPERATURE -> temperaturePort;
            case HUMIDITY -> humidityPort;
        };
    }

    protected String getTopicByType(SensorType type) {
        return switch (type) {
            case TEMPERATURE -> temperatureTopic;
            case HUMIDITY -> humidityTopic;
        };
    }

    public void run() throws IOException {
        mainThread = Thread.currentThread();
        log("Starting " + warehouseId);

        ensureTopicsExist();

        executor.submit(() -> listen(temperaturePort));
        executor.submit(() -> listen(humidityPort));
        try {
            mainThread.join();
        } catch (InterruptedException e) {
            mainThread.interrupt();
            log(ERROR, "Main thread interrupted, shutting down");
        }
    }

    protected void ensureTopicsExist() {
        var topics = List.of(
                new NewTopic(temperatureTopic, topicPartitions, (short) topicReplications),
                new NewTopic(humidityTopic, topicPartitions, (short) topicReplications)
        );
        try {
            var existing = adminClient.listTopics().names().get();
            var toCreate = topics
                    .stream()
                    .filter(t -> !existing.contains(t.name()))
                    .toList();

            if (toCreate.isEmpty()) {
                log("Kafka topics already exist: " + temperatureTopic + ", " + humidityTopic);
                return;
            }

            adminClient.createTopics(toCreate).all().get();
            log("Created Kafka topics: " + toCreate.stream().map(NewTopic::name).toList());
        } catch (ExecutionException | InterruptedException e) {
            log(ERROR, "Failed to ensure Kafka topics: " + e.getMessage());
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        log("Shutting down " + warehouseId);
        executor.shutdownNow();
        if (producer != null) {
            producer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (mainThread != null && mainThread.isAlive()) {
            mainThread.interrupt();
        }
    }

    protected void listen(int port) {
        try (var serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            log("Listener started on port " + port);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    var clientSocket = serverSocket.accept();
                    executor.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    log(ERROR, "Accept failed on port " + port + ": " + e.getMessage());
                }
            }
        } catch (IOException e) {
            log(ERROR, "Cannot bind port " + port + ": " + e.getMessage());
        }
    }

    protected void handleClient(Socket clientSocket) {
        var remote = String.valueOf(clientSocket.getRemoteSocketAddress());
        var port = clientSocket.getLocalPort();
        try (Socket socket = clientSocket) {
            var message = new String(socket.getInputStream().readAllBytes(), UTF_8).trim();
            if (message.isEmpty()) {
                log("Empty message from " + remote);
                return;
            }
            var kafkaMessage = parse(message);
            if (port != getPortByType(kafkaMessage.type())) {
                throw new IllegalArgumentException("Invalid port for sensor type '" + kafkaMessage.type() + "': [" + message + "]");
            }
            sendKafkaMessage(kafkaMessage);
        } catch (IOException | IllegalArgumentException e) {
            log(ERROR, "Error handling " + remote + " on port " + port + ": " + e.getMessage() + " Message ignored.");
        }
    }

    protected Message parse(String message) throws IOException, IllegalArgumentException {
        if (message.toLowerCase().startsWith("exit")) {
            shutdown();
            return null;
        }
        if (!message.startsWith("sensor_id=")) {
            log(ERROR, "Invalid message: " + message);
            throw new IOException("Invalid message: " + message);
        }
        var split = message.split("sensor_id=|;\\s*value=");

        var sensorId = split[1];
        var type = SensorType.fromValue(sensorId.substring(0, 1));
        var value = Integer.parseInt(split[2]);

        return new Message(warehouseId, type, sensorId, value, new Timestamp(System.currentTimeMillis()));
    }

    protected void sendKafkaMessage(Message message) {
        var topic = getTopicByType(message.type());
        var record = new ProducerRecord<>(topic, message.warehouseId(), message);
        try {
            var metadata = producer.send(record).get();
            log("Message sent to Kafka: topic=" + topic
                    + ", partition=" + metadata.partition()
                    + ", offset=" + metadata.offset()
                    + ", [" + message + "]");
        } catch (ExecutionException | InterruptedException e) {
            log(ERROR, "Failed to send message to Kafka: " + e.getMessage());
            if (e instanceof InterruptedException) {
                mainThread.interrupt();
            }
        }
    }
}