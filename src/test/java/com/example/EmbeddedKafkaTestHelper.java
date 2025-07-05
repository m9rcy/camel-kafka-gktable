package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class EmbeddedKafkaTestHelper {

    private final EmbeddedKafkaBroker embeddedKafkaBroker;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
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

        // Setup consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);

        // Setup admin client
        adminClient = AdminClient.create(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }

    public void createTopic(String topicName, int partitions) {
        try {
            NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            // Topic might already exist, log and continue
            System.out.println("Topic " + topicName + " might already exist: " + e.getMessage());
        }
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
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, timeout.toMillis(), expectedCount);
        List<ConsumerRecord<String, String>> recordList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            recordList.add(record);
        }
        return recordList;
    }

    public Map<String, ConsumerRecord<String, String>> consumeMessagesAsMap(String topic, Duration timeout) {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, timeout.toMillis());
        Map<String, ConsumerRecord<String, String>> recordMap = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            recordMap.put(record.key(), record);
        }
        return recordMap;
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public String getBrokerList() {
        return embeddedKafkaBroker.getBrokersAsString();
    }
}