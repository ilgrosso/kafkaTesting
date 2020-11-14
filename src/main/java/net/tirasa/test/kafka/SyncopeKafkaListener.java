package net.tirasa.test.kafka;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class SyncopeKafkaListener implements MessageListener<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncopeKafkaListener.class);

    private final BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

    public ConsumerRecord<String, String> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return records.poll(timeout, unit);
    }

    @Override
    public void onMessage(final ConsumerRecord<String, String> data) {
        LOG.info("XXXXXXX RECEIVED: {} {}", data.key(), data.value());
        records.add(data);
    }
}
