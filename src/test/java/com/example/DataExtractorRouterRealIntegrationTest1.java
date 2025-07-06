package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"integration-order-window-topic", "integration-order-window-filtered-topic"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0",
                "log.dir=/tmp/kafka-integration-test-logs",
                "auto.create.topics.enable=true"
        }
)
@TestPropertySource(properties = {
        // Kafka configuration
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.application-id=integration-test-app-${random.uuid}",
        "spring.kafka.streams.auto-startup=true",
        "spring.kafka.streams.properties.commit.interval.ms=100",
        "spring.kafka.streams.properties.cache.max.bytes.buffering=0",
        "spring.kafka.streams.properties.state.dir=/tmp/kafka-streams-integration-test-${random.uuid}",
        "spring.kafka.streams.properties.processing.guarantee=at_least_once",

        // Topic configuration
        "kafka.topic.order-window-topic=integration-order-window-topic",
        "kafka.topic.order-window-filtered-topic=integration-order-window-filtered-topic",

        // Producer/Consumer configuration
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=integration-test-group",

        // Camel configuration
        "camel.springboot.main-run-controller=true",

        // Logging
        "logging.level.org.apache.kafka=WARN",
        "logging.level.org.springframework.kafka=INFO",
        "logging.level.com.example=DEBUG",
        "logging.level.org.apache.kafka.streams=INFO"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
class DataExtractorRouterRealIntegrationTest1 {

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private KafkaProducer<String, OrderWindow> orderProducer;
    private EmbeddedKafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() throws Exception {
        kafkaHelper = new EmbeddedKafkaTestHelper(embeddedKafkaBroker);

        // Create topics if they don't exist
        kafkaHelper.createTopic("integration-order-window-topic", 1);
        kafkaHelper.createTopic("integration-order-window-filtered-topic", 1);

        // Setup producer for OrderWindow objects
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        orderProducer = new KafkaProducer<>(producerProps);

        // Wait for Kafka Streams to be ready
        await().atMost(30, TimeUnit.SECONDS).until(() -> {
            KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
            return streams != null && streams.state() == KafkaStreams.State.RUNNING;
        });

        System.out.println("KafkaStreams state: " + streamsBuilderFactoryBean.getKafkaStreams().state());

        // Additional wait for state stores to be ready
        Thread.sleep(2000);
    }

    @AfterEach
    void tearDown() {
        if (orderProducer != null) {
            orderProducer.close();
        }
        if (kafkaHelper != null) {
            kafkaHelper.close();
        }
    }

    @Test
    void testCompleteWorkflowWithRealGlobalKTable() throws Exception {
        // Verify KafkaStreams is running
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "KafkaStreams should be running");

        // Step 1: Setup route advice to capture XML output
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-extract-data");
            a.weaveAddLast().to("mock:xml-output");
        });

        camelContext.start();

        MockEndpoint xmlOutput = camelContext.getEndpoint("mock:xml-output", MockEndpoint.class);

        // Step 2: Send initial events to Kafka (these will populate the GlobalKTable)
        System.out.println("Sending initial orders to Kafka...");

        OrderWindow order1 = createOrderWindow("order1", OrderStatus.APPROVED, 1);
        OrderWindow order2 = createOrderWindow("order2", OrderStatus.RELEASED, 1);
        OrderWindow order3 = createOrderWindow("order3", OrderStatus.APPROVED, 2);

        sendOrderToKafka("order1", order1);
        sendOrderToKafka("order2", order2);
        sendOrderToKafka("order3", order3);

        // Wait for messages to be processed by Kafka Streams and populate GlobalKTable
        Thread.sleep(5000);

        // Step 3: Trigger data extraction (should include all 3 orders)
        System.out.println("Triggering first data extraction...");
        xmlOutput.expectedMinimumMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-extract-data", "trigger");

        xmlOutput.assertIsSatisfied(15000);
        String firstXml = xmlOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify all orders are in the XML
        assertTrue(firstXml.contains("<id>order1</id>"), "First XML should contain order1");
        assertTrue(firstXml.contains("<id>order2</id>"), "First XML should contain order2");
        assertTrue(firstXml.contains("<id>order3</id>"), "First XML should contain order3");

        System.out.println("✅ First XML generation (all events):\n" + firstXml);

        // Step 4: Add a new event to GlobalKTable
        System.out.println("Adding new order4...");
        OrderWindow order4 = createOrderWindow("order4", OrderStatus.APPROVED, 1);
        sendOrderToKafka("order4", order4);

        // Wait for the new event to be processed
        Thread.sleep(3000);

        // Step 5: Extract data again (should include the new order4)
        System.out.println("Triggering second data extraction...");
        xmlOutput.reset();
        xmlOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-extract-data", "trigger");

        xmlOutput.assertIsSatisfied(15000);
        String secondXml = xmlOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify the new order is included
        assertTrue(secondXml.contains("<id>order4</id>"), "Second XML should contain new order4");
        System.out.println("✅ Second XML generation (with new event):\n" + secondXml);

        // Step 6: Tombstone order2 (send null value)
        System.out.println("Tombstoning order2...");
        sendTombstoneToKafka("order2");

        // Wait for tombstone to be processed
        Thread.sleep(3000);

        // Step 7: Extract data again (should exclude tombstoned order2)
        System.out.println("Triggering third data extraction...");
        xmlOutput.reset();
        xmlOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-extract-data", "trigger");

        xmlOutput.assertIsSatisfied(15000);
        String thirdXml = xmlOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify tombstoned order is excluded
        assertTrue(thirdXml.contains("<id>order1</id>"), "Third XML should contain order1");
        assertFalse(thirdXml.contains("<id>order2</id>"), "Third XML should NOT contain tombstoned order2");
        assertTrue(thirdXml.contains("<id>order3</id>"), "Third XML should contain order3");
        assertTrue(thirdXml.contains("<id>order4</id>"), "Third XML should contain order4");

        System.out.println("✅ Third XML generation (after tombstone):\n" + thirdXml);
    }

    @Test
    void testTombstoneCleanupWithRealKafka() throws Exception {
        // Verify KafkaStreams is running
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "KafkaStreams should be running");

        // Setup advice for tombstone cleanup route
        AdviceWith.adviceWith(camelContext, "orderWindowTombstoneCleanup", a -> {
            a.replaceFromWith("direct:test-tombstone-cleanup");
            a.weaveAddLast().to("mock:tombstone-sent");
        });

        camelContext.start();

        MockEndpoint tombstoneSent = camelContext.getEndpoint("mock:tombstone-sent", MockEndpoint.class);

        // Step 1: Add old RELEASED orders that should be tombstoned
        System.out.println("Adding old orders for tombstone cleanup...");

        OffsetDateTime fifteenDaysAgo = OffsetDateTime.now().minusDays(15);

        // Create multiple old orders that should be tombstoned
        OrderWindow oldOrder1 = OrderWindow.builder()
                .id("old-order1")
                .name("Old Order 1")
                .status(OrderStatus.RELEASED)
                .planStartDate(fifteenDaysAgo.minusDays(1))
                .planEndDate(fifteenDaysAgo)
                .version(1)
                .build();

        OrderWindow oldOrder2 = OrderWindow.builder()
                .id("old-order2")
                .name("Old Order 2")
                .status(OrderStatus.RELEASED)
                .planStartDate(fifteenDaysAgo.minusDays(1))
                .planEndDate(fifteenDaysAgo)
                .version(1)
                .build();

        OrderWindow oldOrder3 = OrderWindow.builder()
                .id("old-order3")
                .name("Old Order 3")
                .status(OrderStatus.RELEASED)
                .planStartDate(fifteenDaysAgo.minusDays(1))
                .planEndDate(fifteenDaysAgo)
                .version(1)
                .build();

        // Add a recent order that should NOT be tombstoned
        OrderWindow recentOrder = createOrderWindow("recent-order", OrderStatus.RELEASED, 1);

        // Send all orders to Kafka
        sendOrderToKafka("old-order1", oldOrder1);
        sendOrderToKafka("old-order2", oldOrder2);
        sendOrderToKafka("old-order3", oldOrder3);
        sendOrderToKafka("recent-order", recentOrder);

        // Wait for GlobalKTable to be populated
        Thread.sleep(5000);

        // Step 2: Trigger tombstone cleanup
        System.out.println("Triggering tombstone cleanup...");
        tombstoneSent.expectedMinimumMessageCount(3); // Should tombstone the 3 old orders
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-cleanup", "trigger");

        tombstoneSent.assertIsSatisfied(20000);

        System.out.println("✅ Tombstone cleanup sent " + tombstoneSent.getReceivedCounter() + " messages");

        // Step 3: Verify tombstone messages were sent to Kafka
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ConsumerRecord<String, String>> records = kafkaHelper.consumeMessagesAsMap(
                    "integration-order-window-filtered-topic", Duration.ofSeconds(5));

            // Count tombstone messages (null values)
            long tombstoneCount = records.values().stream()
                    .filter(record -> record.value() == null)
                    .count();

            assertTrue(tombstoneCount >= 3,
                    "Should have at least 3 tombstone messages, found: " + tombstoneCount);
        });

        System.out.println("✅ Tombstone cleanup completed successfully");
    }

    @Test
    void testOrderVersioningInGlobalKTable() throws Exception {
        // Verify KafkaStreams is running
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "KafkaStreams should be running");

        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-versioning");
            a.weaveAddLast().to("mock:version-output");
        });

        camelContext.start();

        MockEndpoint versionOutput = camelContext.getEndpoint("mock:version-output", MockEndpoint.class);

        // Step 1: Send order with version 1
        System.out.println("Sending order version 1...");
        OrderWindow orderV1 = createOrderWindow("versioned-order", OrderStatus.DRAFT, 1);
        sendOrderToKafka("versioned-order", orderV1);

        Thread.sleep(2000);

        // Step 2: Send same order with version 2 (should replace version 1)
        System.out.println("Sending order version 2...");
        OrderWindow orderV2 = OrderWindow.builder()
                .id("versioned-order")
                .name("Updated Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(2)
                .build();
        sendOrderToKafka("versioned-order", orderV2);

        Thread.sleep(2000);

        // Step 3: Send same order with version 3
        System.out.println("Sending order version 3...");
        OrderWindow orderV3 = OrderWindow.builder()
                .id("versioned-order")
                .name("Final Order")
                .status(OrderStatus.RELEASED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(3)
                .build();
        sendOrderToKafka("versioned-order", orderV3);

        // Wait for all versions to be processed
        Thread.sleep(5000);

        // Step 4: Extract data (should only contain the latest version)
        System.out.println("Extracting data to verify versioning...");
        versionOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-versioning", "trigger");

        versionOutput.assertIsSatisfied(15000);
        String xml = versionOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify only the latest version is present
        assertTrue(xml.contains("<id>versioned-order</id>"), "XML should contain the versioned order");

        // Count occurrences of the order ID (should be only 1)
        int orderCount = xml.split("<id>versioned-order</id>").length - 1;
        assertEquals(1, orderCount, "Should only have one occurrence of the versioned order (latest version)");

        System.out.println("✅ Versioning test XML:\n" + xml);
    }

    @Test
    void testSimpleDataExtraction() throws Exception {
        // Simplified test to verify basic functionality
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "KafkaStreams should be running");

        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:simple-test");
            a.weaveAddLast().to("mock:simple-output");
        });

        camelContext.start();

        MockEndpoint simpleOutput = camelContext.getEndpoint("mock:simple-output", MockEndpoint.class);

        // Send one simple order
        OrderWindow testOrder = createOrderWindow("test-order", OrderStatus.APPROVED, 1);
        sendOrderToKafka("test-order", testOrder);

        // Wait for processing
        Thread.sleep(3000);

        // Extract data
        simpleOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:simple-test", "trigger");

        simpleOutput.assertIsSatisfied(10000);
        String xml = simpleOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        assertTrue(xml.contains("<id>test-order</id>"), "XML should contain test order");
        System.out.println("✅ Simple test XML:\n" + xml);
    }

    private OrderWindow createOrderWindow(String id, OrderStatus status, int version) {
        return OrderWindow.builder()
                .id(id)
                .name("Test Order " + id)
                .status(status)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(version)
                .build();
    }

    private void sendOrderToKafka(String key, OrderWindow order) {
        try {
            ProducerRecord<String, OrderWindow> record = new ProducerRecord<>(
                    "integration-order-window-topic", key, order);
            orderProducer.send(record).get(); // Wait for send to complete
            orderProducer.flush();
            System.out.println("✅ Sent order: " + key + " -> " + order.getId() + " (v" + order.getVersion() + ")");
        } catch (Exception e) {
            System.err.println("❌ Failed to send order: " + key + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void sendTombstoneToKafka(String key) {
        try {
            ProducerRecord<String, OrderWindow> record = new ProducerRecord<>(
                    "integration-order-window-topic", key, null);
            orderProducer.send(record).get(); // Wait for send to complete
            orderProducer.flush();
            System.out.println("✅ Sent tombstone: " + key);
        } catch (Exception e) {
            System.err.println("❌ Failed to send tombstone: " + key + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}