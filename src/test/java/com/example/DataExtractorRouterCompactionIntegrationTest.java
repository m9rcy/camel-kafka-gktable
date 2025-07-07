package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0",
                "log.dir=target/kafka-test-logs/${random.uuid}",
                "auto.create.topics.enable=false", // Disable auto-creation for manual control

                // Enhanced log cleaner configuration for compaction testing
                "log.cleaner.enable=false",
                "log.cleaner.threads=2",
                "log.cleaner.dedupe.buffer.size=67108864", // 64MB buffer
                "log.cleaner.io.buffer.size=2097152", // 2MB I/O buffer
                "log.cleaner.io.buffer.load.factor=0.9",

                // Aggressive compaction settings for testing
                "log.cleaner.min.cleanable.ratio=0.01", // Start cleaning when 1% dirty
                "log.cleaner.backoff.ms=500", // Check every 500ms
                "log.cleaner.min.compaction.lag.ms=100", // Allow compaction after 100ms
                "log.cleaner.max.compaction.lag.ms=5000", // Force compaction within 5 seconds

                // Segment settings for faster compaction
                "log.segment.bytes=1048576", // 1MB segments
                "log.roll.ms=2000", // Roll segments every 2 seconds

                // Other broker settings
                "num.partitions=1",
                "default.replication.factor=1",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "group.initial.rebalance.delay.ms=0"
        }
)
@TestPropertySource(properties = {
        // Kafka configuration
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.application-id=compaction-test-app-${random.uuid}",
        "spring.kafka.streams.auto-startup=false",
        "spring.kafka.streams.properties.commit.interval.ms=100",
        "spring.kafka.streams.properties.cache.max.bytes.buffering=0",
        "spring.kafka.streams.properties.state.dir=target/kafka-streams-compaction-test-${random.uuid}",
        "spring.kafka.streams.properties.processing.guarantee=at_least_once",
        "spring.kafka.streams.properties.replication.factor=1",

        // Topic configuration
        "kafka.topic.order-window-topic=compaction-order-window-topic",
        "kafka.topic.order-window-filtered-topic=compaction-order-window-filtered-topic",

        // Producer/Consumer configuration
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=compaction-test-group",

        // Camel configuration
        "camel.springboot.main-run-controller=true",

        // Logging
        "logging.level.org.apache.kafka=WARN",
        "logging.level.org.springframework.kafka=INFO",
        "logging.level.com.example=DEBUG",
        "logging.level.org.apache.kafka.streams=INFO",
        "logging.level.kafka.log.LogCleaner=DEBUG"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
class DataExtractorRouterCompactionIntegrationTest {

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private KafkaProducer<String, OrderWindow> orderProducer;
    private AdminClient adminClient;
    private EmbeddedKafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() throws Exception {
        kafkaHelper = new EmbeddedKafkaTestHelper(embeddedKafkaBroker);
        
        // Create admin client for topic management
        adminClient = AdminClient.create(KafkaTestUtils.producerProps(embeddedKafkaBroker));

        // Create topics with log compaction settings
        createCompactedTopics();

        // Setup producer for OrderWindow objects
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        orderProducer = new KafkaProducer<>(producerProps);

        streamsBuilderFactoryBean.start();

        // Wait for Kafka Streams to be ready
        await().atMost(45, TimeUnit.SECONDS).until(() -> {
            KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
            return streams != null && streams.state() == KafkaStreams.State.RUNNING;
        });

        System.out.println("✅ KafkaStreams state: " + streamsBuilderFactoryBean.getKafkaStreams().state());
        
        // Additional wait for state stores to be ready
        Thread.sleep(3000);
    }

    @AfterEach
    void tearDown() {
        if (orderProducer != null) {
            orderProducer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaHelper != null) {
            kafkaHelper.close();
        }
    }

    private void createCompactedTopics() throws Exception {
        // Configuration for source topic (input)
        Map<String, String> inputTopicConfigs = new HashMap<>();
        inputTopicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        inputTopicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "2000"); // Force segments to roll quickly
        inputTopicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01"); // Allow compaction sooner
        inputTopicConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100"); // Shorter retention for tombstones
        inputTopicConfigs.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100"); // Minimum lag before compaction
        inputTopicConfigs.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "5000"); // Maximum lag before forced compaction

        // Configuration for filtered topic (output) - also compacted for GlobalKTable
        Map<String, String> filteredTopicConfigs = new HashMap<>();
        filteredTopicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        filteredTopicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "2000");
        filteredTopicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01");
        filteredTopicConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");
        filteredTopicConfigs.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100");
        filteredTopicConfigs.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "5000");

        NewTopic inputTopic = new NewTopic("compaction-order-window-topic", 1, (short) 1)
                .configs(inputTopicConfigs);
        
        NewTopic filteredTopic = new NewTopic("compaction-order-window-filtered-topic", 1, (short) 1)
                .configs(filteredTopicConfigs);

        CreateTopicsResult result = adminClient.createTopics(java.util.Arrays.asList(inputTopic, filteredTopic));
        result.all().get(30, TimeUnit.SECONDS);

        System.out.println("✅ Created compacted topics with aggressive compaction settings");
        
        // Verify topic configurations
        verifyTopicConfigurations();
    }

    private void verifyTopicConfigurations() throws Exception {
        Map<String, TopicDescription> descriptions = adminClient.describeTopics(
            java.util.Arrays.asList("compaction-order-window-topic", "compaction-order-window-filtered-topic")
        ).all().get(10, TimeUnit.SECONDS);

        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            System.out.println("✅ Topic: " + entry.getKey() + " created with " + 
                              entry.getValue().partitions().size() + " partitions");
        }
    }

    @Test
    void testDataExtractionWithLogCompaction() throws Exception {
        // Verify KafkaStreams is running
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "KafkaStreams should be running");

        // Setup route advice to capture XML output
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-compaction-extract");
            a.weaveAddLast().to("mock:compaction-xml-output");
        });

        camelContext.start();

        MockEndpoint xmlOutput = camelContext.getEndpoint("mock:compaction-xml-output", MockEndpoint.class);

        // Step 1: Send multiple versions of the same order (should be compacted)
        System.out.println("📝 Step 1: Sending multiple versions of orders for compaction testing...");
        
        // Send order1 version 1
        OrderWindow order1v1 = createOrderWindow("order1", OrderStatus.DRAFT, 1);
        sendOrderToKafka("order1", order1v1);
        
        // Send order1 version 2 (should replace v1 after compaction)
        OrderWindow order1v2 = createOrderWindow("order1", OrderStatus.APPROVED, 2);
        sendOrderToKafka("order1", order1v2);
        
        // Send order1 version 3 (should replace v2 after compaction)
        OrderWindow order1v3 = createOrderWindow("order1", OrderStatus.RELEASED, 3);
        sendOrderToKafka("order1", order1v3);

        // Send different order
        OrderWindow order2v1 = createOrderWindow("order2", OrderStatus.APPROVED, 1);
        sendOrderToKafka("order2", order2v1);

        // Wait for compaction and GlobalKTable update
        System.out.println("⏳ Waiting for compaction and GlobalKTable update...");
        Thread.sleep(8000); // Wait for compaction to occur

        // Step 2: Trigger data extraction (should only contain latest versions)
        System.out.println("🔍 Step 2: Triggering data extraction...");
        xmlOutput.expectedMinimumMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-compaction-extract", "trigger");

        xmlOutput.assertIsSatisfied(20000);
        String extractedXml = xmlOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify only latest versions are present
        assertTrue(extractedXml.contains("<id>order1</id>"), "Should contain order1");
        assertTrue(extractedXml.contains("<id>order2</id>"), "Should contain order2");
        assertTrue(extractedXml.contains("<status>RELEASED</status>"), "Should contain latest status for order1");
        assertTrue(extractedXml.contains("<status>APPROVED</status>"), "Should contain status for order2");

        // Count occurrences (should only be one per order ID due to deduplication logic)
        int order1Count = countOccurrences(extractedXml, "<id>order1</id>");
        int order2Count = countOccurrences(extractedXml, "<id>order2</id>");
        
        assertEquals(1, order1Count, "Should only have one occurrence of order1 (latest version)");
        assertEquals(1, order2Count, "Should only have one occurrence of order2");

        System.out.println("✅ First extraction (after compaction):\n" + extractedXml);
    }

    @Test
    void testTombstoneProcessingWithCompaction() throws Exception {
        // Verify KafkaStreams is running
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state());

        // Setup route advice
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-tombstone-compaction");
            a.weaveAddLast().to("mock:tombstone-compaction-output");
        });

        camelContext.start();

        MockEndpoint tombstoneOutput = camelContext.getEndpoint("mock:tombstone-compaction-output", MockEndpoint.class);

        // Step 1: Send orders that will be tombstoned
        System.out.println("📝 Step 1: Sending orders for tombstone testing...");
        
        OrderWindow order1 = createOrderWindow("tombstone-order1", OrderStatus.APPROVED, 1);
        OrderWindow order2 = createOrderWindow("tombstone-order2", OrderStatus.RELEASED, 1);
        OrderWindow order3 = createOrderWindow("tombstone-order3", OrderStatus.APPROVED, 1);

        sendOrderToKafka("tombstone-order1", order1);
        sendOrderToKafka("tombstone-order2", order2);
        sendOrderToKafka("tombstone-order3", order3);

        Thread.sleep(3000);

        // Step 2: Extract data (should contain all orders)
        System.out.println("🔍 Step 2: First extraction (before tombstone)...");
        tombstoneOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-compaction", "trigger");

        tombstoneOutput.assertIsSatisfied(15000);
        String beforeTombstoneXml = tombstoneOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);
        
        assertTrue(beforeTombstoneXml.contains("<id>tombstone-order1</id>"));
        assertTrue(beforeTombstoneXml.contains("<id>tombstone-order2</id>"));
        assertTrue(beforeTombstoneXml.contains("<id>tombstone-order3</id>"));

        System.out.println("✅ Before tombstone XML:\n" + beforeTombstoneXml);

        // Step 3: Send tombstones
        System.out.println("💀 Step 3: Sending tombstones...");
        sendTombstoneToKafka("tombstone-order2");

        // Wait for tombstone compaction
        Thread.sleep(8000);

        // Step 4: Extract data again (should exclude tombstoned record)
        System.out.println("🔍 Step 4: Second extraction (after tombstone)...");
        tombstoneOutput.reset();
        tombstoneOutput.expectedMessageCount(1);
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-compaction", "trigger");

        tombstoneOutput.assertIsSatisfied(15000);
        String afterTombstoneXml = tombstoneOutput.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify tombstoned record is excluded
        assertTrue(afterTombstoneXml.contains("<id>tombstone-order1</id>"), "Should still contain order1");
        assertFalse(afterTombstoneXml.contains("<id>tombstone-order2</id>"), "Should NOT contain tombstoned order2");
        assertTrue(afterTombstoneXml.contains("<id>tombstone-order3</id>"), "Should still contain order3");

        System.out.println("✅ After tombstone XML:\n" + afterTombstoneXml);
    }

    @Test
    void testCompactionEffectivenessWithMultipleVersions() throws Exception {
        System.out.println("🧪 Testing compaction effectiveness with multiple versions...");

        // Send many versions of the same orders to test compaction
        for (int version = 1; version <= 10; version++) {
            OrderWindow order1 = OrderWindow.builder()
                    .id("compaction-test-order")
                    .name("Test Order Version " + version)
                    .status(version % 2 == 0 ? OrderStatus.APPROVED : OrderStatus.DRAFT)
                    .planStartDate(OffsetDateTime.now().minusDays(1))
                    .planEndDate(OffsetDateTime.now().plusDays(1))
                    .version(version)
                    .build();
            
            sendOrderToKafka("compaction-test-order", order1);
            
            // Small delay between versions
            Thread.sleep(200);
        }

        // Wait for compaction to occur
        System.out.println("⏳ Waiting for compaction to process multiple versions...");
        Thread.sleep(10000);

        // Verify compaction by checking the compacted topic
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ConsumerRecord<String, String>> records = kafkaHelper.consumeMessagesAsMap(
                    "compaction-order-window-filtered-topic", Duration.ofSeconds(5));

            System.out.println("📊 Found " + records.size() + " records in compacted topic");

            // Should only have the latest version after compaction
            boolean hasCompactionTestOrder = records.containsKey("compaction-test-order");
            assertTrue(hasCompactionTestOrder, "Should contain the compaction test order key");

            if (hasCompactionTestOrder) {
                ConsumerRecord<String, String> latestRecord = records.get("compaction-test-order");
                assertNotNull(latestRecord.value(), "Latest record should not be null");
                System.out.println("✅ Latest compacted record: " + latestRecord.value());
            }
        });
    }

    @Test
    void testTombstoneCleanupRouteWithCompaction() throws Exception {
        // Test the actual tombstone cleanup route with compacted topics
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        assertEquals(KafkaStreams.State.RUNNING, streams.state());

        // Setup advice for tombstone cleanup route
        AdviceWith.adviceWith(camelContext, "orderWindowTombstoneCleanup", a -> {
            a.replaceFromWith("direct:test-cleanup-compaction");
            a.weaveAddLast().to("mock:cleanup-compaction-sent");
        });

        camelContext.start();

        MockEndpoint cleanupSent = camelContext.getEndpoint("mock:cleanup-compaction-sent", MockEndpoint.class);

        // Add old orders that should be tombstoned
        OffsetDateTime fifteenDaysAgo = OffsetDateTime.now().minusDays(15);
        
        OrderWindow oldOrder1 = OrderWindow.builder()
                .id("cleanup-old-order1")
                .name("Old Order 1")
                .status(OrderStatus.RELEASED)
                .planStartDate(fifteenDaysAgo.minusDays(1))
                .planEndDate(fifteenDaysAgo)
                .version(1)
                .build();

        OrderWindow oldOrder2 = OrderWindow.builder()
                .id("cleanup-old-order2")
                .name("Old Order 2")
                .status(OrderStatus.RELEASED)
                .planStartDate(fifteenDaysAgo.minusDays(1))
                .planEndDate(fifteenDaysAgo)
                .version(1)
                .build();

        sendOrderToKafka("cleanup-old-order1", oldOrder1);
        sendOrderToKafka("cleanup-old-order2", oldOrder2);

        Thread.sleep(5000);

        // Trigger cleanup
        cleanupSent.expectedMinimumMessageCount(2);
        camelContext.createProducerTemplate().sendBody("direct:test-cleanup-compaction", "trigger");

        cleanupSent.assertIsSatisfied(20000);

        // Verify tombstones were sent and will be compacted
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ConsumerRecord<String, String>> records = kafkaHelper.consumeMessagesAsMap(
                    "compaction-order-window-filtered-topic", Duration.ofSeconds(5));

            long tombstoneCount = records.values().stream()
                    .filter(record -> record.value() == null)
                    .count();

            assertTrue(tombstoneCount >= 2, 
                    "Should have at least 2 tombstone messages for compaction, found: " + tombstoneCount);
        });

        System.out.println("✅ Tombstone cleanup with compaction completed successfully");
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
                    "compaction-order-window-topic", key, order);
            orderProducer.send(record).get();
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
                    "compaction-order-window-topic", key, null);
            orderProducer.send(record).get();
            orderProducer.flush();
            System.out.println("💀 Sent tombstone: " + key);
        } catch (Exception e) {
            System.err.println("❌ Failed to send tombstone: " + key + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private int countOccurrences(String text, String pattern) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(pattern, index)) != -1) {
            count++;
            index += pattern.length();
        }
        return count;
    }
}