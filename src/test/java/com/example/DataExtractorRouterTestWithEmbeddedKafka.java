package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorProcessor;
import com.example.processor.OrderWindowTombstoneProcessor;
import com.example.service.KafkaStateStoreService;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
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
import java.time.OffsetDateTime;
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
    "logging.level.kafka=WARN"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
class DataExtractorRouterTestWithEmbeddedKafka {

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockBean
    private KafkaStateStoreService kafkaStateStoreService;

    @MockBean
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @MockBean
    private OrderWindowDataExtractorProcessor orderWindowDataExtractorProcessor;

    @MockBean
    private OrderWindowConversionProcessor xmlProcessor;

    @MockBean
    private OrderWindowTombstoneProcessor orderWindowTombstoneProcessor;

    @Autowired
    private ObjectMapper objectMapper;

    private EmbeddedKafkaTestHelper kafkaHelper;

    @BeforeEach
    void setUp() throws Exception {
        kafkaHelper = new EmbeddedKafkaTestHelper(embeddedKafkaBroker);
        kafkaHelper.createTopic("test-order-window-filtered-topic", 1);
        
        // Start Camel context
        camelContext.start();
    }

    @AfterEach
    void tearDown() {
        if (kafkaHelper != null) {
            kafkaHelper.close();
        }
    }

    @Test
    void testDataExtractionRouteWithMockedProcessors() throws Exception {
        // Given
        List<OrderWindow> testData = Arrays.asList(
            TestDataFactory.createOrderWindow("order1", OrderStatus.APPROVED, 1),
            TestDataFactory.createOrderWindow("order2", OrderStatus.RELEASED, 1)
        );

        // Mock the processors
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(testData);
            exchange.getMessage().setHeader("dataExtractCount", testData.size());
            return null;
        }).when(orderWindowDataExtractorProcessor).process(any());

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            String xmlContent = createTestXmlContent(testData);
            exchange.getMessage().setBody(xmlContent);
            return null;
        }).when(xmlProcessor).process(any());

        // Create test route
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test-data-extraction")
                    .process(orderWindowDataExtractorProcessor)
                    .choice()
                        .when(header("dataExtractCount").isGreaterThan(0))
                            .log("Found ${header.dataExtractCount} records to process")
                            .process(xmlProcessor)
                            .to("mock:xml-processing-result")
                        .endChoice()
                        .otherwise()
                            .log("No records found to extract")
                            .to("mock:no-data-result")
                    .end();
            }
        });

        MockEndpoint xmlProcessingResult = CamelTestUtils.getMockEndpoint(camelContext, "mock:xml-processing-result");
        xmlProcessingResult.expectedMessageCount(1);
        xmlProcessingResult.expectedHeaderReceived("dataExtractCount", 2);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-data-extraction", "trigger");

        // Then
        xmlProcessingResult.assertIsSatisfied(10000);
        
        String xmlResult = xmlProcessingResult.getReceivedExchanges().get(0).getMessage().getBody(String.class);
        assertTrue(xmlResult.contains("<id>order1</id>"));
        assertTrue(xmlResult.contains("<id>order2</id>"));
        
        verify(orderWindowDataExtractorProcessor).process(any());
        verify(xmlProcessor).process(any());
    }

    @Test
    void testTombstoneCleanupRouteWithActualKafka() throws Exception {
        // Given
        List<String> keysToTombstone = Arrays.asList("tombstone-key1", "tombstone-key2", "tombstone-key3");
        String targetTopic = "test-order-window-filtered-topic";

        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(keysToTombstone);
            exchange.getMessage().setHeader("tombstoneCount", keysToTombstone.size());
            return null;
        }).when(orderWindowTombstoneProcessor).process(any());

        // Create tombstone cleanup route
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test-tombstone-cleanup")
                    .log("Starting tombstone cleanup process...")
                    .process(orderWindowTombstoneProcessor)
                    .choice()
                        .when(header("tombstoneCount").isGreaterThan(0))
                            .log("Found ${header.tombstoneCount} records to tombstone")
                            .split(body())
                            .process(exchange -> {
                                String keyToTombstone = exchange.getIn().getBody(String.class);
                                exchange.getMessage().setBody(null); // Tombstone message
                                exchange.getMessage().setHeader("kafka.KEY", keyToTombstone);
                            })
                            .to("kafka:" + targetTopic + "?brokers=" + kafkaHelper.getBrokerList())
                            .log("Tombstoned key: ${header.kafka.KEY}")
                        .endChoice()
                        .otherwise()
                            .log("No records found for tombstone cleanup")
                    .end();
            }
        });

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-tombstone-cleanup", "trigger");

        // Then - Wait for tombstone messages and verify
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, ConsumerRecord<String, String>> records = kafkaHelper.consumeMessagesAsMap(
                targetTopic, Duration.ofSeconds(5));
            
            assertEquals(3, records.size(), "Should have received 3 tombstone messages");
            
            for (String expectedKey : keysToTombstone) {
                assertTrue(records.containsKey(expectedKey), 
                    "Should contain tombstone for key: " + expectedKey);
                assertNull(records.get(expectedKey).value(), 
                    "Value should be null for tombstone message: " + expectedKey);
            }
        });

        verify(orderWindowTombstoneProcessor).process(any());
    }

    @Test
    void testDataExtractionWithNoData() throws Exception {
        // Given
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(Collections.emptyList());
            exchange.getMessage().setHeader("dataExtractCount", 0);
            return null;
        }).when(orderWindowDataExtractorProcessor).process(any());

        // Create test route
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test-no-data")
                    .process(orderWindowDataExtractorProcessor)
                    .choice()
                        .when(header("dataExtractCount").isGreaterThan(0))
                            .process(xmlProcessor)
                            .to("mock:xml-processing-result")
                        .endChoice()
                        .otherwise()
                            .log("No records found to extract")
                            .to("mock:no-data-result")
                    .end();
            }
        });

        MockEndpoint noDataResult = CamelTestUtils.getMockEndpoint(camelContext, "mock:no-data-result");
        noDataResult.expectedMessageCount(1);
        noDataResult.expectedHeaderReceived("dataExtractCount", 0);

        MockEndpoint xmlProcessingResult = CamelTestUtils.getMockEndpoint(camelContext, "mock:xml-processing-result");
        xmlProcessingResult.expectedMessageCount(0);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-no-data", "trigger");

        // Then
        noDataResult.assertIsSatisfied(5000);
        xmlProcessingResult.assertIsSatisfied(5000);
        
        verify(orderWindowDataExtractorProcessor).process(any());
        verify(xmlProcessor, never()).process(any());
    }

    @Test
    void testTombstoneCleanupWithNoEligibleRecords() throws Exception {
        // Given
        doAnswer(invocation -> {
            Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(Collections.emptyList());
            exchange.getMessage().setHeader("tombstoneCount", 0);
            return null;
        }).when(orderWindowTombstoneProcessor).process(any());

        // Create test route
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test-no-tombstone")
                    .process(orderWindowTombstoneProcessor)
                    .choice()
                        .when(header("tombstoneCount").isGreaterThan(0))
                            .to("mock:tombstone-processing-result")
                        .endChoice()
                        .otherwise()
                            .log("No records found for tombstone cleanup")
                            .to("mock:no-tombstone-result")
                    .end();
            }
        });

        MockEndpoint noTombstoneResult = CamelTestUtils.getMockEndpoint(camelContext, "mock:no-tombstone-result");
        noTombstoneResult.expectedMessageCount(1);
        noTombstoneResult.expectedHeaderReceived("tombstoneCount", 0);

        MockEndpoint tombstoneProcessingResult = CamelTestUtils.getMockEndpoint(camelContext, "mock:tombstone-processing-result");
        tombstoneProcessingResult.expectedMessageCount(0);

        // When
        camelContext.createProducerTemplate().sendBody("direct:test-no-tombstone", "trigger");

        // Then
        noTombstoneResult.assertIsSatisfied(5000);
        tombstoneProcessingResult.assertIsSatisfied(5000);
        
        verify(orderWindowTombstoneProcessor).process(any());
    }

    @Test
    void testKafkaConnectivityAndMessageFlow() throws Exception {
        // Given
        String testTopic = "test-order-window-filtered-topic";
        String testKey = "connectivity-test-key";
        String testValue = "connectivity-test-value";

        // Create a simple route to send and receive messages
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:kafka-send")
                    .to("kafka:" + testTopic + "?brokers=" + kafkaHelper.getBrokerList());

                from("kafka:" + testTopic + "?brokers=" + kafkaHelper.getBrokerList() + 
                     "&groupId=test-connectivity-group&autoOffsetReset=earliest")
                    .to("mock:kafka-received");
            }
        });

        MockEndpoint kafkaReceived = CamelTestUtils.getMockEndpoint(camelContext, "mock:kafka-received");
        kafkaReceived.expectedMessageCount(1);
        kafkaReceived.expectedBodiesReceived(testValue);

        // When
        camelContext.createProducerTemplate().sendBodyAndHeader("direct:kafka-send", testValue, "kafka.KEY", testKey);

        // Then
        kafkaReceived.assertIsSatisfied(10000);
        
        String receivedBody = kafkaReceived.getReceivedExchanges().get(0).getMessage().getBody(String.class);
        assertEquals(testValue, receivedBody);
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