package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EmbeddedKafkaTestHelper {

    private final EmbeddedKafkaBroker embeddedKafkaBroker;
    private KafkaProducer<String, String> producer;
    private AdminClient adminClient;

    public EmbeddedKafkaTestHelper(EmbeddedKafkaBroker embeddedKafkaBroker) {
        this.embeddedKafkaBroker = embeddedKafkaBroker;
        initializeClients();
    }

    private void initializeClients() {
        // Setup producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);

        // Setup admin client
        adminClient = AdminClient.create(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }

    public void createTopicWithCompaction(String topicName, int partitions) {
        try {
            // Configure topic with log compaction
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "1000"); // Force segments to roll quickly for testing
            topicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01"); // Allow compaction sooner
            topicConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100"); // Shorter retention for tombstones

            NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1)
                    .configs(topicConfigs);

            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
            result.all().get(10, TimeUnit.SECONDS);

            System.out.println("✅ Created compacted topic: " + topicName);
        } catch (TopicExistsException e) {
            System.out.println("Topic " + topicName + " already exists");
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }

    public void createTopic(String topicName, int partitions) {
        createTopicWithCompaction(topicName, partitions);
    }

    public void sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        producer.flush();
    }

    public void sendTombstone(String topic, String key) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, null);
        producer.send(record);
        producer.flush();
    }

    public List<ConsumerRecord<String, String>> consumeMessages(String topic, int expectedCount, Duration timeout) {
        // Create a new consumer for each consumption to avoid offset issues
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, timeout.toMillis(), expectedCount);
            List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                recordList.add(record);
            }
            return recordList;
        }
    }

    public Map<String, ConsumerRecord<String, String>> consumeMessagesAsMap(String topic, Duration timeout) {
        // Create a new consumer for each consumption
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            // Poll multiple times to ensure we get all messages
            Map<String, ConsumerRecord<String, String>> recordMap = new HashMap<>();
            long endTime = System.currentTimeMillis() + timeout.toMillis();

            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    recordMap.put(record.key(), record);
                }

                // If we haven't received any records in the last poll, wait a bit more
                if (records.isEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            return recordMap;
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public String getBrokerList() {
        return embeddedKafkaBroker.getBrokersAsString();
    }
}