package net.tirasa.test.kafka;

import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@EmbeddedKafka
class EmbeddedKafkaTests extends AbstractKafkaTests {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeAll
    void setUp() {
        Map<String, Object> configs = KafkaTestUtils.consumerProps("consumer", "false", embeddedKafkaBroker);
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                configs,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new StringDeserializer()));
        listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, new ContainerProperties(TOPIC));
        listenerContainer.setupMessageListener(listener);
        listenerContainer.start();
        ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterAll
    void tearDown() {
        listenerContainer.stop();
    }

    @Override
    protected Map<String, Object> producerConfigs() {
        return KafkaTestUtils.producerProps(embeddedKafkaBroker);
    }
}
