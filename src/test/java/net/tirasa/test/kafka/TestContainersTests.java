package net.tirasa.test.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

class TestContainersTests extends AbstractKafkaTests {

    @Container
    private final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    private Map<String, Object> producerConfigs;

    @BeforeAll
    void setUp() throws UnsupportedOperationException, IOException, InterruptedException {
        kafkaContainer.start();
        kafkaContainer.execInContainer(
                "/bin/sh",
                "-c",
                "/usr/bin/kafka-topics --create --zookeeper $KAFKA_ZOOKEEPER_CONNECT "
                + "--replication-factor 1 --partitions 1 --topic " + TOPIC);

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
        consumerConfigs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerConfigs,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new StringDeserializer()));
        listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, new ContainerProperties(TOPIC));
        listenerContainer.setupMessageListener(listener);
        listenerContainer.start();
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerConfigs.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerConfigs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @AfterAll
    void tearDown() {
        kafkaContainer.stop();
    }

    @Override
    protected Map<String, Object> producerConfigs() {
        return producerConfigs;
    }
}
