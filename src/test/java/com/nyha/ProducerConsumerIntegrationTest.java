package com.nyha;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ProducerConsumerIntegrationTest {
    @Container
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @Test
    void produceConsume_test() {
        String bootstrapServers = kafka.getBootstrapServers();
        String topic = "test-topic";
        String key = "test-key"; 
        String value = "test-value";
        String group = "test-consumer-group";

        AtLeastOnceProducer producer = new AtLeastOnceProducer();
        producer.send(bootstrapServers, topic, key, value);
        String errLogs = kafka.getLogs(OutputFrame.OutputType.STDERR);
        assertTrue(errLogs == null || errLogs.isBlank());

        AtMostOnceConsumer consumer = new AtMostOnceConsumer();
        List<String> messages = consumer.poll(bootstrapServers, topic, group);
        assertEquals(1, messages.size());
        String actual = messages.get(0);
        assertEquals(value, actual);
    }
}
