package org.warehouse.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.warehouse.common.LogType.ERROR;
import static org.warehouse.common.LogUtils.log;

public class Configuration {
    private static final String KAFKA_BOOTSTRAP_SERVERS_NAME    = "kafka-bootstrap-servers";
    private static final String TEMPERATURE_TOPIC_NAME          = "kafka-temperature-topic";
    private static final String HUMIDITY_TOPIC_NAME             = "kafka-humidity-topic";
    private static final String GROUP_ID_NAME                   = "kafka-group-id";
    private static final String TEMPERATURE_PORT_NAME           = "server-temperature-port";
    private static final String HUMIDITY_PORT_NAME              = "server-humidity-port";
    private static final String TOPIC_PARTITIONS_NAME           = "kafka-topic-partitions";
    private static final String TOPIC_REPLICATIONS_NAME         = "kafka-topic-replications";
    private static final String TEMPERATURE_THRESHOLD_NAME      = "temperature-threshold";
    private static final String HUMIDITY_THRESHOLD_NAME         = "humidity-threshold";

    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TEMPERATURE_TOPIC       = "temperature-topic";
    private static final String DEFAULT_HUMIDITY_TOPIC          = "humidity-topic";
    private static final String DEFAULT_GROUP_ID                = "central-service";
    private static final int    DEFAULT_TEMPERATURE_PORT        = 3344;
    private static final int    DEFAULT_HUMIDITY_PORT           = 3355;
    private static final int    DEFAULT_TEMPERATURE_THRESHOLD   = 35;
    private static final int    DEFAULT_HUMIDITY_THRESHOLD      = 50;
    private static final int    DEFAULT_TOPIC_PARTITIONS        = 36;
    private static final short  DEFAULT_TOPIC_REPLICATION       = 1;

    private static final Path DEFAULT_PROPERTIES_FILE = Path.of("application.properties");

    private final Properties properties = new Properties();

    private final String serverName;

    private final String temperatureTopic;
    private final String humidityTopic;
    private final int temperaturePort;
    private final int humidityPort;
    private final String kafkaBootstrapServers;
    private final String groupId;
    private final int topicPartitions;
    private final int topicReplications;
    private final int temperatureThreshold;
    private final int humidityThreshold;

    public Configuration(String serverName) {
        this(serverName, DEFAULT_PROPERTIES_FILE);
    }

    public Configuration(String serverName, Path propertiesFile) {
        this.serverName = serverName;
        if (Files.isRegularFile(propertiesFile)) {
            try (InputStream in = Files.newInputStream(propertiesFile)) {
                properties.load(in);
            } catch (IOException e) {
                log(ERROR, "Cannot read " + propertiesFile + ": " + e.getMessage());
            }
        }
        temperatureTopic      = readStringProperty(TEMPERATURE_TOPIC_NAME, DEFAULT_TEMPERATURE_TOPIC);
        humidityTopic         = readStringProperty(HUMIDITY_TOPIC_NAME, DEFAULT_HUMIDITY_TOPIC);
        temperaturePort       = readIntProperty(TEMPERATURE_PORT_NAME, DEFAULT_TEMPERATURE_PORT);
        humidityPort          = readIntProperty(HUMIDITY_PORT_NAME, DEFAULT_HUMIDITY_PORT);
        groupId               = readStringProperty(GROUP_ID_NAME, DEFAULT_GROUP_ID);
        kafkaBootstrapServers = readStringProperty(KAFKA_BOOTSTRAP_SERVERS_NAME, DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        topicPartitions       = readIntProperty(TOPIC_PARTITIONS_NAME, DEFAULT_TOPIC_PARTITIONS);
        topicReplications     = readIntProperty(TOPIC_REPLICATIONS_NAME, DEFAULT_TOPIC_REPLICATION);
        temperatureThreshold  = readIntProperty(TEMPERATURE_THRESHOLD_NAME, DEFAULT_TEMPERATURE_THRESHOLD);
        humidityThreshold     = readIntProperty(HUMIDITY_THRESHOLD_NAME, DEFAULT_HUMIDITY_THRESHOLD);
    }

    private String readStringProperty(String name, String defaultValue) {
        var raw = properties.getProperty(name);
        return raw == null ? defaultValue : raw;
    }

    private int readIntProperty(String name, int defaultValue) {
        var raw = properties.getProperty(name);
        if (raw == null) return defaultValue;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            log(ERROR, name + " is not a number: [" + raw + "], default value is " + defaultValue);
            return defaultValue;
        }
    }

    public String getTemperatureTopic() {
        return temperatureTopic;
    }

    public String getHumidityTopic() {
        return humidityTopic;
    }

    public int getTemperaturePort() {
        return temperaturePort;
    }

    public int getHumidityPort() {
        return humidityPort;
    }

    public String getServerName() {
        return serverName;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getTopicPartitions() {
        return topicPartitions;
    }

    public int getTopicReplications() {
        return topicReplications;
    }

    public int getTemperatureThreshold() {
        return temperatureThreshold;
    }

    public int getHumidityThreshold() {
        return humidityThreshold;
    }
}