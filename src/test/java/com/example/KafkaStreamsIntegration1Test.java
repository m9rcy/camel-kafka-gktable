package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.OffsetDateTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaStreamsIntegration1Test {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderWindow> inputTopic;
    private TestOutputTopic<String, OrderWindow> outputTopic;
    private KafkaStreamsTopologyConfig config;
    private KafkaTopicConfig topicConfig;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        // Setup topic configuration
        topicConfig = new KafkaTopicConfig();
        topicConfig.setOrderWindowTopic("order-window-topic");
        topicConfig.setOrderWindowFilteredTopic("order-window-filtered-topic");
        
        // Setup object mapper
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        
        // Setup Kafka properties
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.buildConsumerProperties().put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.buildConsumerProperties().put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProperties.buildConsumerProperties().put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        kafkaProperties.buildProducerProperties().put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.buildProducerProperties().put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        kafkaProperties.buildProducerProperties().put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");


        // Setup streams builder and topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        config = new KafkaStreamsTopologyConfig(topicConfig, streamsBuilder);
        
        // Create the GlobalKTable
        GlobalKTable<String, OrderWindow> globalKTable = config.orderWindowGlobalKTable(kafkaProperties, objectMapper);
        
        // Build topology
        Topology topology = streamsBuilder.build();
        
        // Setup test driver
        Properties testProps = new Properties();
        testProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        testProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        testProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        testDriver = new TopologyTestDriver(topology, testProps);
        
        // Setup test topics
        inputTopic = testDriver.createInputTopic(
                topicConfig.getOrderWindowTopic(),
                Serdes.String().serializer(),
                new JsonSerializer<>(objectMapper.constructType(OrderWindow.class), objectMapper)
        );
        
        outputTopic = testDriver.createOutputTopic(
                topicConfig.getOrderWindowFilteredTopic(),
                Serdes.String().deserializer(),
                new JsonDeserializer<>(objectMapper.constructType(OrderWindow.class), objectMapper)
        );
    }
    
    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }
    
    @Test
    void testFilteringApprovedOrders() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .name("Approved Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(1)
                .build();
        
        // When
        inputTopic.pipeInput("key1", approvedOrder);
        
        // Then
        KeyValue<String, OrderWindow> result = outputTopic.readKeyValue();
        assertEquals("key1", result.key);
        assertEquals(OrderStatus.APPROVED, result.value.getStatus());
        assertEquals("order1", result.value.getId());
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    void testFilteringReleasedOrdersWithinTimeWindow() {
        // Given
        OffsetDateTime tenDaysAgo = OffsetDateTime.now().minusDays(10);
        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .name("Released Order")
                .status(OrderStatus.RELEASED)
                .planStartDate(tenDaysAgo.minusDays(1))
                .planEndDate(tenDaysAgo)
                .version(1)
                .build();
        
        // When
        inputTopic.pipeInput("key2", releasedOrder);
        
        // Then
        KeyValue<String, OrderWindow> result = outputTopic.readKeyValue();
        assertEquals("key2", result.key);
        assertEquals(OrderStatus.RELEASED, result.value.getStatus());
        assertEquals("order2", result.value.getId());
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    void testFilteringOutOldReleasedOrders() {
        // Given
        OffsetDateTime fourteenDaysAgo = OffsetDateTime.now().minusDays(14);
        OrderWindow oldReleasedOrder = OrderWindow.builder()
                .id("order3")
                .name("Old Released Order")
                .status(OrderStatus.RELEASED)
                .planStartDate(fourteenDaysAgo.minusDays(1))
                .planEndDate(fourteenDaysAgo)
                .version(1)
                .build();
        
        // When
        inputTopic.pipeInput("key3", oldReleasedOrder);
        
        // Then
        assertTrue(outputTopic.isEmpty(), "Old released orders should be filtered out");
    }
    
    @Test
    void testFilteringOutDraftOrders() {
        // Given
        OrderWindow draftOrder = OrderWindow.builder()
                .id("order4")
                .name("Draft Order")
                .status(OrderStatus.DRAFT)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(1)
                .build();
        
        // When
        inputTopic.pipeInput("key4", draftOrder);
        
        // Then
        assertTrue(outputTopic.isEmpty(), "Draft orders should be filtered out");
    }
    
    @Test
    void testTombstoneMessage() {
        // Given - send a tombstone message (null value)
        
        // When
        inputTopic.pipeInput("key5", null);
        
        // Then
        KeyValue<String, OrderWindow> result = outputTopic.readKeyValue();
        assertEquals("key5", result.key);
        assertNull(result.value);
        assertTrue(outputTopic.isEmpty());
    }
    
    @Test
    void testGlobalKTableStateStore() {
        // Given
        OrderWindow order1 = OrderWindow.builder()
                .id("order1")
                .name("Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(1)
                .build();
        
        OrderWindow order2 = OrderWindow.builder()
                .id("order2")
                .name("Test Order 2")
                .status(OrderStatus.RELEASED)
                .planStartDate(OffsetDateTime.now().minusDays(5))
                .planEndDate(OffsetDateTime.now().minusDays(5))
                .version(1)
                .build();
        
        // When
        inputTopic.pipeInput("key1", order1);
        inputTopic.pipeInput("key2", order2);
        
        // Then
        ReadOnlyKeyValueStore<String, OrderWindow> store = testDriver.getKeyValueStore("order-window-global-store");
        
        OrderWindow storedOrder1 = store.get("key1");
        assertNotNull(storedOrder1);
        assertEquals("order1", storedOrder1.getId());
        assertEquals(OrderStatus.APPROVED, storedOrder1.getStatus());
        
        OrderWindow storedOrder2 = store.get("key2");
        assertNotNull(storedOrder2);
        assertEquals("order2", storedOrder2.getId());
        assertEquals(OrderStatus.RELEASED, storedOrder2.getStatus());
    }
}