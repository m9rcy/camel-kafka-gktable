package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorOldProcessor;
import com.example.processor.OrderWindowTombstoneOldProcessor;
import com.example.service.KafkaStateStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@CamelSpringBootTest
@SpringBootTest
@Import(TestApplicationConfig.class)
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-order-window-filtered-topic"},
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:0",
                "port=0",
                "log.dir=/tmp/kafka-test-logs"
        }
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic.order-window-topic=test-order-window-topic",
        "kafka.topic.order-window-filtered-topic=test-order-window-filtered-topic",
        "camel.springboot.main-run-controller=true",
        "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN",
        "spring.kafka.streams.auto-startup=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
class DataExtractorRouterTestWithEmbeddedKafka1 {

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockBean
    private KafkaStateStoreService kafkaStateStoreService;

    @MockBean
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @MockBean
    private OrderWindowDataExtractorOldProcessor orderWindowDataExtractorOldProcessor;

    @MockBean
    private OrderWindowConversionProcessor xmlProcessor;

    @MockBean
    private OrderWindowTombstoneOldProcessor orderWindowTombstoneOldProcessor;

    @Autowired
    private KafkaTopicConfig kafkaTopicConfig;

    @Autowired
    private ObjectMapper objectMapper;

    private EmbeddedKafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() throws Exception {
        kafkaHelper = new EmbeddedKafkaTestHelper(embeddedKafkaBroker);
        kafkaHelper.createTopic("test-order-window-filtered-topic", 1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kafkaHelper != null) {
            kafkaHelper.close();
        }
    }

    @Test
    void testDataExtractorRouteWithGlobalKTableData() throws Exception {
        // Given - Mock GlobalKTable data that includes new events
        List<OrderWindow> mockGlobalKTableData = Arrays.asList(
                TestDataFactory.createOrderWindow("order1", OrderStatus.APPROVED, 1),
                TestDataFactory.createOrderWindow("order2", OrderStatus.RELEASED, 1),
                TestDataFactory.createOrderWindow("order3", OrderStatus.APPROVED, 2) // New event
        );

        // Mock the data extractor to return data from GlobalKTable
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(mockGlobalKTableData);
            exchange.getMessage().setHeader("dataExtractCount", mockGlobalKTableData.size());
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        // Mock the XML processor to generate XML
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            String xmlContent = createTestXmlContent(mockGlobalKTableData);
            exchange.getMessage().setBody(xmlContent);
            return null;
        }).when(xmlProcessor).process(any());

        // Use AdviceWith to modify the actual DataExtractorRouter route
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            // Replace timer with direct endpoint for testing
            a.replaceFromWith("direct:test-data-extraction");

            // Add mock endpoint to capture final result
            a.weaveAddLast().to("mock:xml-result");
        });

        camelContext.start();

        MockEndpoint xmlResult = camelContext.getEndpoint("mock:xml-result", MockEndpoint.class);
        xmlResult.expectedMessageCount(1);

        // When - Trigger the route
        camelContext.createProducerTemplate().sendBody("direct:test-data-extraction", "trigger");

        // Then - Verify XML generation includes all events from GlobalKTable
        xmlResult.assertIsSatisfied(10000);

        String generatedXml = xmlResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify all events are included in XML
        assertTrue(generatedXml.contains("<id>order1</id>"), "XML should contain order1");
        assertTrue(generatedXml.contains("<id>order2</id>"), "XML should contain order2");
        assertTrue(generatedXml.contains("<id>order3</id>"), "XML should contain new order3");
        assertTrue(generatedXml.contains("<status>APPROVED</status>"), "XML should contain APPROVED status");
        assertTrue(generatedXml.contains("<status>RELEASED</status>"), "XML should contain RELEASED status");

        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor).process(any());
    }

    @Test
    void testDataExtractorRouteWithTombstonedEvents() throws Exception {
        // Given - Mock GlobalKTable data where some events are tombstoned (excluded)
        List<OrderWindow> mockGlobalKTableDataAfterTombstone = Arrays.asList(
                TestDataFactory.createOrderWindow("order1", OrderStatus.APPROVED, 1),
                TestDataFactory.createOrderWindow("order3", OrderStatus.APPROVED, 2)
                // order2 is tombstoned (not included in GlobalKTable data)
        );

        // Mock the data extractor to return only non-tombstoned data
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(mockGlobalKTableDataAfterTombstone);
            exchange.getMessage().setHeader("dataExtractCount", mockGlobalKTableDataAfterTombstone.size());
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        // Mock the XML processor
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            String xmlContent = createTestXmlContent(mockGlobalKTableDataAfterTombstone);
            exchange.getMessage().setBody(xmlContent);
            return null;
        }).when(xmlProcessor).process(any());

        // Use AdviceWith to modify the route
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-tombstone-scenario");
            a.weaveAddLast().to("mock:xml-result-tombstone");
        });

        camelContext.start();

        MockEndpoint xmlResult = camelContext.getEndpoint("mock:xml-result-tombstone", MockEndpoint.class);
        xmlResult.expectedMessageCount(1);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-scenario", "trigger");

        // Then - Verify tombstoned events are excluded from XML
        xmlResult.assertIsSatisfied(10000);

        String generatedXml = xmlResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        // Verify remaining events are included
        assertTrue(generatedXml.contains("<id>order1</id>"), "XML should contain order1");
        assertTrue(generatedXml.contains("<id>order3</id>"), "XML should contain order3");

        // Verify tombstoned event is excluded
        assertFalse(generatedXml.contains("<id>order2</id>"), "XML should NOT contain tombstoned order2");

        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor).process(any());
    }

    @Test
    void testTombstoneCleanupRouteWithEmbeddedKafka() throws Exception {
        // Given - Mock tombstone processor to identify keys for cleanup
        List<String> keysToTombstone = Arrays.asList("expired-order1", "expired-order2");

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(keysToTombstone);
            exchange.getMessage().setHeader("tombstoneCount", keysToTombstone.size());
            return null;
        }).when(orderWindowTombstoneOldProcessor).process(any());

        // Use AdviceWith to modify the tombstone route to use embedded Kafka
        AdviceWith.adviceWith(camelContext, "orderWindowTombstoneCleanup", a -> {
            a.replaceFromWith("direct:test-tombstone-cleanup");

            // Replace the Kafka endpoint to use embedded broker
            a.weaveByToString(".*kafka:.*").replace().to(
                    "kafka:test-order-window-filtered-topic?brokers=" + kafkaHelper.getBrokerList() +
                            "&keySerializer=org.apache.kafka.common.serialization.StringSerializer" +
                            "&valueSerializer=org.apache.kafka.common.serialization.StringSerializer"
            );
        });

        camelContext.start();

        // When - Trigger tombstone cleanup
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-cleanup", "trigger");

        // Then - Verify tombstone messages are sent to Kafka
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ConsumerRecord<String, String>> records = kafkaHelper.consumeMessagesAsMap(
                    "test-order-window-filtered-topic", Duration.ofSeconds(5));

            assertEquals(2, records.size(), "Should have received 2 tombstone messages");

            for (String expectedKey : keysToTombstone) {
                assertTrue(records.containsKey(expectedKey),
                        "Should contain tombstone for key: " + expectedKey);
                assertNull(records.get(expectedKey).value(),
                        "Value should be null for tombstone message: " + expectedKey);
            }
        });

        verify(orderWindowTombstoneOldProcessor).process(any());
    }

    @Test
    void testDataExtractorRouteWithNoData() throws Exception {
        // Given - Mock empty GlobalKTable
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(Collections.emptyList());
            exchange.getMessage().setHeader("dataExtractCount", 0);
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        // Use AdviceWith to modify the route
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-no-data");
            a.weaveAddLast().to("mock:no-data-result");
        });

        camelContext.start();

        MockEndpoint noDataResult = camelContext.getEndpoint("mock:no-data-result", MockEndpoint.class);
        noDataResult.expectedMessageCount(1);
        noDataResult.expectedHeaderReceived("dataExtractCount", 0);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-no-data", "trigger");

        // Then
        noDataResult.assertIsSatisfied(5000);

        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor, never()).process(any()); // Should not process XML when no data
    }

    @Test
    void testActualRouteLogicFlow() throws Exception {
        // Test the actual choice/when logic in your DataExtractorRouter

        // Given - Data that should trigger XML processing
        List<OrderWindow> testData = Arrays.asList(
                TestDataFactory.createOrderWindow("test-order", OrderStatus.APPROVED, 1)
        );

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(testData);
            exchange.getMessage().setHeader("dataExtractCount", 1);
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            String xmlContent = createTestXmlContent(testData);
            exchange.getMessage().setBody(xmlContent);
            return null;
        }).when(xmlProcessor).process(any());

        // Use AdviceWith to test the actual route logic
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-route-logic");

            // Intercept log endpoints to verify route flow
            a.mockEndpoints("log:*");
            a.weaveAddLast().to("mock:final-result");
        });

        camelContext.start();

        MockEndpoint finalResult = camelContext.getEndpoint("mock:final-result", MockEndpoint.class);
        finalResult.expectedMessageCount(1);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-route-logic", "trigger");

        // Then
        finalResult.assertIsSatisfied(5000);

        // Verify the XML content was generated
        String result = finalResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);
        assertTrue(result.contains("<id>test-order</id>"));

        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor).process(any());
    }

    @Test
    void testCompleteWorkflowAddEventThenTombstone() throws Exception {
        // This test simulates the complete workflow:
        // 1. Add events to GlobalKTable
        // 2. Generate XML (should include new events)
        // 3. Tombstone some events
        // 4. Generate XML again (should exclude tombstoned events)

        // Step 1: Initial data with multiple events
        List<OrderWindow> initialData = Arrays.asList(
                TestDataFactory.createOrderWindow("order1", OrderStatus.APPROVED, 1),
                TestDataFactory.createOrderWindow("order2", OrderStatus.RELEASED, 1),
                TestDataFactory.createOrderWindow("order3", OrderStatus.APPROVED, 1)
        );

        // Mock first extraction (before tombstone)
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(initialData);
            exchange.getMessage().setHeader("dataExtractCount", initialData.size());
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            String xmlContent = createTestXmlContent((List<OrderWindow>) exchange.getIn().getBody());
            exchange.getMessage().setBody(xmlContent);
            return null;
        }).when(xmlProcessor).process(any());

        // Setup route advice
        AdviceWith.adviceWith(camelContext, "orderWindowConsumer", a -> {
            a.replaceFromWith("direct:test-workflow");
            a.weaveAddLast().to("mock:workflow-result");
        });

        camelContext.start();

        MockEndpoint workflowResult = camelContext.getEndpoint("mock:workflow-result", MockEndpoint.class);
        workflowResult.expectedMinimumMessageCount(1);

        // When - First extraction (all events present)
        camelContext.createProducerTemplate().sendBody("direct:test-workflow", "trigger");

        // Then - Verify all events are in XML
        workflowResult.assertIsSatisfied(5000);
        String firstXml = workflowResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        assertTrue(firstXml.contains("<id>order1</id>"), "First XML should contain order1");
        assertTrue(firstXml.contains("<id>order2</id>"), "First XML should contain order2");
        assertTrue(firstXml.contains("<id>order3</id>"), "First XML should contain order3");

        // Reset for second test
        workflowResult.reset();
        workflowResult.expectedMessageCount(1);

        // Step 2: After tombstone (order2 removed)
        List<OrderWindow> dataAfterTombstone = Arrays.asList(
                TestDataFactory.createOrderWindow("order1", OrderStatus.APPROVED, 1),
                TestDataFactory.createOrderWindow("order3", OrderStatus.APPROVED, 1)
                // order2 tombstoned - not present
        );

        // Update mock to return data without tombstoned events
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(dataAfterTombstone);
            exchange.getMessage().setHeader("dataExtractCount", dataAfterTombstone.size());
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        // When - Second extraction (after tombstone)
        camelContext.createProducerTemplate().sendBody("direct:test-workflow", "trigger");

        // Then - Verify tombstoned event is excluded
        workflowResult.assertIsSatisfied(5000);
        String secondXml = workflowResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);

        assertTrue(secondXml.contains("<id>order1</id>"), "Second XML should still contain order1");
        assertTrue(secondXml.contains("<id>order3</id>"), "Second XML should still contain order3");
        assertFalse(secondXml.contains("<id>order2</id>"), "Second XML should NOT contain tombstoned order2");
    }

    private String createTestXmlContent(List<OrderWindow> orderWindows) {
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\"?><poxp>");
        for (OrderWindow order : orderWindows) {
            xml.append("<out_update><out>")
                    .append("<id>").append(order.getId()).append("</id>")
                    .append("<status>").append(order.getStatus()).append("</status>")
                    .append("<notes></notes>")
                    .append("</out></out_update>");
        }
        xml.append("</poxp>");
        return xml.toString();
    }
}