package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.OrderWindowPredicateService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.test.context.TestPropertySource;
import javax.validation.Validator;

import java.time.OffsetDateTime;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = {
        KafkaTopicConfig.class,
        TestApplicationConfig.class,
        KafkaProperties.class
})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=dummy:9092",
        "spring.kafka.streams.application-id=test-app",
        "kafka.topic.order-window-topic=test-order-window-topic",
        "kafka.topic.order-window-filtered-topic=test-order-window-filtered-topic"
})
class KafkaStreamsIntegrationTest {

    @Autowired
    private KafkaTopicConfig kafkaTopicConfig;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderWindowPredicateService orderWindowPredicateService;
    @Autowired
    private Validator validator;

    private KafkaStreamsTopologyConfig topologyConfig;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderWindow> inputTopic;
    private TestOutputTopic<String, OrderWindow> outputTopic;
    private JsonSerde<OrderWindow> orderWindowSerde;

    @BeforeEach
    void setUp() {


        // Build topology using the actual configuration
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topologyConfig = new KafkaStreamsTopologyConfig(kafkaTopicConfig, streamsBuilder, orderWindowPredicateService, validator);
        GlobalKTable<String, OrderWindow> globalKTable = topologyConfig.orderWindowGlobalKTable(kafkaProperties, objectMapper);

        // Build the complete topology
        Topology topology = streamsBuilder.build();

        // Setup test driver with Spring Boot Kafka properties
        Properties testProps = buildTestProperties();
        testDriver = new TopologyTestDriver(topology, testProps);

        // Create the serde using the same method as the topology config
        orderWindowSerde = createValueSerde(OrderWindow.class);

        // Setup test topics with proper serdes
        inputTopic = testDriver.createInputTopic(
                kafkaTopicConfig.getOrderWindowTopic(),
                Serdes.String().serializer(),
                orderWindowSerde.serializer()
        );

        outputTopic = testDriver.createOutputTopic(
                kafkaTopicConfig.getOrderWindowFilteredTopic(),
                Serdes.String().deserializer(),
                orderWindowSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
        if (orderWindowSerde != null) {
            orderWindowSerde.close();
        }
    }

    @Test
    void testFilteringApprovedOrders() {
        // Given
        OrderWindow approvedOrder = createTestOrder("order1", OrderStatus.APPROVED, 1);

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
        OrderWindow releasedOrder = createTestOrderWithDates("order2", OrderStatus.RELEASED, 1,
                tenDaysAgo.minusDays(1), tenDaysAgo);

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
        // Given - Order older than 12 days should be filtered out
        OffsetDateTime fourteenDaysAgo = OffsetDateTime.now().minusDays(14);
        OrderWindow oldReleasedOrder = createTestOrderWithDates("order3", OrderStatus.RELEASED, 1,
                fourteenDaysAgo.minusDays(1), fourteenDaysAgo);

        // When
        inputTopic.pipeInput("key3", oldReleasedOrder);

        // Then
        assertTrue(outputTopic.isEmpty(), "Old released orders should be filtered out");
    }

    @Test
    void testFilteringOutDraftOrders() {
        // Given
        OrderWindow draftOrder = createTestOrder("order4", OrderStatus.DRAFT, 1);

        // When
        inputTopic.pipeInput("key4", draftOrder);

        // Then
        assertTrue(outputTopic.isEmpty(), "Draft orders should be filtered out");
    }

    @Test
    void testFilteringOutDoneOrders() {
        // Given
        OrderWindow doneOrder = createTestOrder("order5", OrderStatus.DONE, 1);

        // When
        inputTopic.pipeInput("key5", doneOrder);

        // Then
        assertTrue(outputTopic.isEmpty(), "Done orders should be filtered out");
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
        OrderWindow order1 = createTestOrder("order1", OrderStatus.APPROVED, 1);
        OrderWindow order2 = createTestOrderWithDates("order2", OrderStatus.RELEASED, 1,
                OffsetDateTime.now().minusDays(5), OffsetDateTime.now().minusDays(5));

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


    @Test
    void testMultipleVersionsOfSameOrder() {
        // Given - Multiple versions of the same order ID
        OrderWindow orderV1 = createTestOrder("order1", OrderStatus.DRAFT, 1);
        OrderWindow orderV2 = createTestOrder("order1", OrderStatus.APPROVED, 2);
        OrderWindow orderV3 = createTestOrder("order1", OrderStatus.RELEASED, 3);

        // When & Then - Test each version individually

        // Test 1: DRAFT order should be filtered out
        inputTopic.pipeInput("key1", orderV1);
        assertTrue(outputTopic.isEmpty(), "Draft order should be filtered out");

        // Test 2: APPROVED order should pass through
        inputTopic.pipeInput("key1", orderV2);
        assertFalse(outputTopic.isEmpty(), "Approved order should pass through");
        KeyValue<String, OrderWindow> result1 = outputTopic.readKeyValue();
        assertEquals("key1", result1.key);
        assertEquals(OrderStatus.APPROVED, result1.value.getStatus());
        assertEquals(2, result1.value.getVersion());
        assertTrue(outputTopic.isEmpty(), "Should have consumed all messages");

        // Test 3: RELEASED order should pass through
        inputTopic.pipeInput("key1", orderV3);
        assertFalse(outputTopic.isEmpty(), "Released order should pass through");
        KeyValue<String, OrderWindow> result2 = outputTopic.readKeyValue();
        assertEquals("key1", result2.key);
        assertEquals(OrderStatus.RELEASED, result2.value.getStatus());
        assertEquals(3, result2.value.getVersion());
        assertTrue(outputTopic.isEmpty(), "Should have consumed all messages");
    }

    // Alternative test that shows version progression in GlobalKTable
    @Test
    void testMultipleVersionsInGlobalKTable() {
        // Given - Multiple versions of the same order ID
        OrderWindow orderV1 = createTestOrder("order1", OrderStatus.APPROVED, 1);
        OrderWindow orderV2 = createTestOrder("order1", OrderStatus.APPROVED, 2);
        OrderWindow orderV3 = createTestOrder("order1", OrderStatus.RELEASED, 3);

        // When - Send all versions with the same key (simulating updates)
        inputTopic.pipeInput("same-key", orderV1);
        inputTopic.pipeInput("same-key", orderV2);
        inputTopic.pipeInput("same-key", orderV3);

        // Then - All should pass through to the filtered topic
        assertEquals(3, outputTopic.getQueueSize(), "All 3 versions should be in output topic");

        // Read all messages
        KeyValue<String, OrderWindow> result1 = outputTopic.readKeyValue();
        assertEquals(1, result1.value.getVersion());

        KeyValue<String, OrderWindow> result2 = outputTopic.readKeyValue();
        assertEquals(2, result2.value.getVersion());

        KeyValue<String, OrderWindow> result3 = outputTopic.readKeyValue();
        assertEquals(3, result3.value.getVersion());

        assertTrue(outputTopic.isEmpty(), "All messages should be consumed");

        // Verify GlobalKTable contains the latest version
        ReadOnlyKeyValueStore<String, OrderWindow> store = testDriver.getKeyValueStore("order-window-global-store");
        OrderWindow storedOrder = store.get("same-key");
        assertNotNull(storedOrder);
        assertEquals(3, storedOrder.getVersion(), "GlobalKTable should contain the latest version");
        assertEquals(OrderStatus.RELEASED, storedOrder.getStatus());
    }

    @Test
    void testReleasedOrderDateBoundary() {
        // Given - Test exactly at the 12-day boundary
        OffsetDateTime twelveDaysAgo = OffsetDateTime.now().minusDays(12);
        OffsetDateTime thirteenDaysAgo = OffsetDateTime.now().minusDays(13);

        OrderWindow borderlineOrder = createTestOrderWithDates("borderline", OrderStatus.RELEASED, 1,
                twelveDaysAgo.minusDays(1), twelveDaysAgo);
        OrderWindow tooOldOrder = createTestOrderWithDates("tooOld", OrderStatus.RELEASED, 1,
                thirteenDaysAgo.minusDays(1), thirteenDaysAgo);

        // When
        inputTopic.pipeInput("borderline-key", borderlineOrder);
        inputTopic.pipeInput("tooOld-key", tooOldOrder);

        // Then
        // Borderline order (exactly 12 days) should pass
        KeyValue<String, OrderWindow> result = outputTopic.readKeyValue();
        assertEquals("borderline-key", result.key);
        assertEquals("borderline", result.value.getId());

        // Too old order should be filtered out
        assertTrue(outputTopic.isEmpty(), "Order older than 12 days should be filtered out");
    }

    @Test
    void testCompleteFilteringLogic() {
        // Given - Test all filtering conditions at once
        OrderWindow approved = createTestOrder("approved", OrderStatus.APPROVED, 1);
        OrderWindow recentReleased = createTestOrderWithDates("recentReleased", OrderStatus.RELEASED, 1,
                OffsetDateTime.now().minusDays(5), OffsetDateTime.now().minusDays(5));
        OrderWindow oldReleased = createTestOrderWithDates("oldReleased", OrderStatus.RELEASED, 1,
                OffsetDateTime.now().minusDays(15), OffsetDateTime.now().minusDays(15));
        OrderWindow draft = createTestOrder("draft", OrderStatus.DRAFT, 1);
        OrderWindow lodged = createTestOrder("lodged", OrderStatus.LODGED, 1);
        OrderWindow done = createTestOrder("done", OrderStatus.DONE, 1);

        // When
        inputTopic.pipeInput("approved-key", approved);
        inputTopic.pipeInput("recent-key", recentReleased);
        inputTopic.pipeInput("old-key", oldReleased);
        inputTopic.pipeInput("draft-key", draft);
        inputTopic.pipeInput("lodged-key", lodged);
        inputTopic.pipeInput("done-key", done);
        inputTopic.pipeInput("tombstone-key", null);

        // Then - Only approved, recent released, and tombstone should pass through
        KeyValue<String, OrderWindow> result1 = outputTopic.readKeyValue();
        assertEquals("approved-key", result1.key);
        assertEquals(OrderStatus.APPROVED, result1.value.getStatus());

        KeyValue<String, OrderWindow> result2 = outputTopic.readKeyValue();
        assertEquals("recent-key", result2.key);
        assertEquals(OrderStatus.RELEASED, result2.value.getStatus());

        KeyValue<String, OrderWindow> result3 = outputTopic.readKeyValue();
        assertEquals("tombstone-key", result3.key);
        assertNull(result3.value);

        assertTrue(outputTopic.isEmpty(), "All other orders should be filtered out");
    }

    // Helper methods

    private OrderWindow createTestOrder(String id, OrderStatus status, int version) {
        return OrderWindow.builder()
                .id(id)
                .name("Test Order " + id)
                .status(status)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(version)
                .build();
    }

    private OrderWindow createTestOrderWithDates(String id, OrderStatus status, int version,
                                                 OffsetDateTime startDate, OffsetDateTime endDate) {
        return OrderWindow.builder()
                .id(id)
                .name("Test Order " + id)
                .status(status)
                .planStartDate(startDate)
                .planEndDate(endDate)
                .version(version)
                .build();
    }

    /**
     * Creates a JsonSerde using the same method as KafkaStreamsTopologyConfig
     */
    private JsonSerde<OrderWindow> createValueSerde(Class<OrderWindow> targetType) {
        return topologyConfig.createValueSerde(kafkaProperties, targetType, objectMapper);
    }

    /**
     * Build test properties from Spring Boot KafkaProperties
     */
    private Properties buildTestProperties() {
        Properties props = new Properties();

        // Use Spring Boot's Kafka properties as base
        props.putAll(kafkaProperties.buildStreamsProperties());

        // Override specific test properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Test-specific optimizations
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-test");

        return props;
    }
}