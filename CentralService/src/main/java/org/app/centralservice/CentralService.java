package org.app.centralservice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.warehouse.common.Configuration;
import org.warehouse.common.Message;
import org.warehouse.common.SensorType;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.warehouse.common.LogType.ERROR;
import static org.warehouse.common.LogUtils.log;

public class CentralService  implements AutoCloseable {
    private final Configuration configuration;
    private final Map<SensorType, MessageListener> listeners = new EnumMap<>(SensorType.class);

    private final KafkaConsumer<String, Message> consumer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Thread worker;

    public CentralService(Configuration configuration, List<MessageListener> listeners) {
        this.configuration = configuration;
        for (var listener : listeners) {
            this.listeners.put(listener.supportedType(), listener);
        }
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        if (!isRunning.compareAndSet(false, true)) {
            return;
        }
        worker = new Thread(this::pollLoop, "kafka-consumer");
        worker.start();
    }

    private void pollLoop() {
        try {
            consumer.subscribe(List.of(configuration.getTemperatureTopic(), configuration.getHumidityTopic()));
            log("Kafka consumer subscribed to: [" + configuration.getTemperatureTopic() + ", " + configuration.getHumidityTopic() + "]");
            while (isRunning.get()) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, Message> record : records) {
                    proceed(record);
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            if (isRunning.get()) {
                log(ERROR, "Unexpected WakeupException: " + e.getMessage());
            }
        } catch (Exception e) {
            log(ERROR, "Consumer loop failed: " + e.getMessage());
        } finally {
            consumer.close();
            log("Kafka consumer closed");
        }
    }

    private void proceed(ConsumerRecord<String, Message> record) {
        var message = record.value();
        if (message == null) {
            log(ERROR, "Null message from topic=" + record.topic() + ", offset=" + record.offset());
            return;
        }
        var listener = listeners.get(message.type());
        if (listener == null) {
            log(ERROR, "No listener for type=" + message.type() + ", message=[" + message + "]");
            return;
        }
        log("Received: topic=" + record.topic()
                + ", partition=" + record.partition()
                + ", offset=" + record.offset()
                + ", key=" + record.key()
                + ", [" + message + "]");
        listener.onMessage(message);
    }

    @Override
    public void close() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }
        consumer.wakeup();
        if (worker != null) {
            try {
                worker.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
