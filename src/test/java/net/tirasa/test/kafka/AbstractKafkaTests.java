package net.tirasa.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractKafkaTests {

    protected static final String TOPIC = "domain-events";

    @Autowired
    protected SyncopeKafkaListener listener;

    protected ConcurrentMessageListenerContainer<String, String> listenerContainer;

    protected abstract Map<String, Object> producerConfigs();

    @Test
    void ensureSendMessageIsReceived() throws InterruptedException {
        // Arrange
        Producer<String, String> producer = new DefaultKafkaProducerFactory<>(
                producerConfigs(), new StringSerializer(), new StringSerializer()).createProducer();

        // Act
        producer.send(new ProducerRecord<>(TOPIC, "my-aggregate-id", "{\"event\":\"Test Event\"}"));
        producer.flush();

        // Assert
        ConsumerRecord<String, String> singleRecord = listener.poll(100, TimeUnit.MILLISECONDS);
        assertNotNull(singleRecord);
        assertEquals("my-aggregate-id", singleRecord.key());
        assertEquals("{\"event\":\"Test Event\"}", singleRecord.value());
    }
}
