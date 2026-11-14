package org.warehouse.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConfigurationTest {
    @TempDir
    Path tempDir;

    @Test
    void testCustomPropertiesFile() throws IOException {
        var propsFile = tempDir.resolve("application.properties");
        Files.writeString(propsFile,
            """
                        server-temperature-port=3344
                        server-humidity-port=3355
                        kafka-temperature-topic=temperature-topic
                        kafka-humidity-topic=humidity-topic
                        kafka-bootstrap-servers=localhost:9092
                        kafka-group-id=central-service
                        kafka-topic-partitions=10
                        kafka-topic-replications=1
                        temperature-threshold=35
                        humidity-threshold=50
                    """
        );
        var config = new Configuration("test-server", propsFile);
        assertNotNull(config);
        assertEquals("test-server", config.getServerName());
        assertEquals(3344, config.getTemperaturePort());
        assertEquals(3355, config.getHumidityPort());
        assertEquals("temperature-topic", config.getTemperatureTopic());
        assertEquals("humidity-topic", config.getHumidityTopic());
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals("central-service", config.getGroupId());
        assertEquals(10, config.getTopicPartitions());
        assertEquals(1, config.getTopicReplications());
    }

    @Test
    void testCustomPropertiesFileWithIncorrectIntProperty() throws Exception {
        var propsFile = tempDir.resolve("application.properties");
        Files.writeString(propsFile,
            """
                    server-temperature-port=incorrect-text-value
                    server-humidity-port=3355
                    kafka-temperature-topic=temperature-topic
                    kafka-humidity-topic=humidity-topic
                    kafka-bootstrap-servers=localhost:9092
                    kafka-group-id=central-service
                    kafka-topic-partitions=10
                    kafka-topic-replications=1
                    temperature-threshold=35
                    humidity-threshold=50
                """
        );
        var config = new Configuration("test", propsFile);
        assertNotNull(config);
        assertEquals(3344, config.getTemperaturePort());
    }

    @Test
    void testConfigParams () {
        var config = new Configuration("test");
        assertNotNull(config.getKafkaBootstrapServers());
        assertFalse(config.getKafkaBootstrapServers().isEmpty());
        assertEquals("temperature-topic", config.getTemperatureTopic());
        assertEquals("humidity-topic", config.getHumidityTopic());
        assertEquals("central-service", config.getGroupId());
        assertEquals(36, config.getTopicPartitions());
        assertEquals(1, config.getTopicReplications());
        assertEquals(35, config.getTemperatureThreshold());
        assertEquals(50, config.getHumidityThreshold());
    }
}
