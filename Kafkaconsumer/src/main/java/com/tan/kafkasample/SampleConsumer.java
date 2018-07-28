package com.tan.kafkasample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class SampleConsumer {
    private static final long KAFKA_POLLING_TIMEOUT = 200; // 5s
    private KafkaConsumer<String, String> m_kafkaConsumer;
    private String m_consumerGroup;
    private String m_consumerName;
    private List<String> m_subscribedTopics;
    private AtomicBoolean m_stop;
    private ExecutorService m_executorService;

    public SampleConsumer(String consumerGroup, String consumerName, List<String> subscribedTopics, ExecutorService executorService) {
        this.m_executorService = executorService;
        this.m_consumerGroup = consumerGroup;
        this.m_consumerName = consumerName;
        this.m_subscribedTopics = subscribedTopics;
        m_stop = new AtomicBoolean(false);
        initializeConsumer();
    }

    public void startConsume() {
        m_executorService.submit(() -> {
            System.out.println("Consumer [" + m_consumerName + "] in group [" + m_consumerGroup + "] start polling");
            m_stop.getAndSet(false);
            while (!m_stop.get()) {
                ConsumerRecords<String, String> records = m_kafkaConsumer.poll(KAFKA_POLLING_TIMEOUT);
                if (records != null && !records.isEmpty()) {
                    StringBuilder messageBuilder = new StringBuilder();
                    messageBuilder.append("Consumer: [").append(m_consumerName).append("] Received message: ");
                    records.forEach(x -> {
                        messageBuilder.append(" Topic: ").append(x.topic());
                        messageBuilder.append(" Message: ").append(x.value()).append(" partition: ").append(x.partition()).append(" offset: ").append(x.offset());
                        System.out.println(messageBuilder);
                    });
                }
            }
        });
    }

    public void stopConsume() {
        m_stop.getAndSet(true);
    }

    private void initializeConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, m_consumerGroup);
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 60 * 1000); // 60s
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5 * 60 * 1000); // 5 min
        m_kafkaConsumer = new KafkaConsumer<>(properties);
        m_kafkaConsumer.subscribe(m_subscribedTopics);
    }
}
