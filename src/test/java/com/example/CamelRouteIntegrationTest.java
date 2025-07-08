package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorOldProcessor;
import com.example.processor.OrderWindowTombstoneOldProcessor;
import com.example.service.KafkaStateStoreService;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spring.boot.CamelAutoConfiguration;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@CamelSpringBootTest
@SpringBootTest(classes = CamelRouteIntegrationTest.TestConfiguration.class)
@TestPropertySource(properties = {
        "camel.springboot.main-run-controller=true",
        "kafka.topic.order-window-topic=test-order-window",
        "kafka.topic.order-window-filtered-topic=test-order-window-filtered"
})
class CamelRouteIntegrationTest {

    @Autowired
    private CamelContext camelContext;

    @Produce("direct:test-data-extractor")
    private ProducerTemplate producerTemplate;

    @EndpointInject("mock:result")
    private MockEndpoint mockEndpoint;

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

    @MockBean
    private KafkaTopicConfig kafkaTopicConfig;

    @Test
    void testOrderWindowConsumerRouteWithData() throws Exception {
        // Reset mock endpoint to ensure clean state
        mockEndpoint.reset();

        // Given
        List<OrderWindow> testData = Arrays.asList(
                OrderWindow.builder()
                        .id("order1")
                        .name("Test Order")
                        .status(OrderStatus.APPROVED)
                        .version(1)
                        .build()
        );

        doAnswer(invocation -> {
            org.apache.camel.Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(testData);
            exchange.getMessage().setHeader("dataExtractCount", testData.size());
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        doAnswer(invocation -> {
            org.apache.camel.Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody("<?xml version=\"1.0\"?><poxp><out_update><out><id>order1</id><status>APPROVED</status><notes></notes></out></out_update></poxp>");
            return null;
        }).when(xmlProcessor).process(any());

        mockEndpoint.expectedMessageCount(1);
        mockEndpoint.expectedHeaderReceived("dataExtractCount", 1);

        // When
        producerTemplate.sendBody("direct:test-data-extractor", "");

        // Then
        mockEndpoint.assertIsSatisfied();
        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor).process(any());
    }

    @Test
    void testOrderWindowConsumerRouteWithNoData() throws Exception {
        // Reset mock endpoint from previous test
        mockEndpoint.reset();

        // Given
        doAnswer(invocation -> {
            org.apache.camel.Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(Collections.emptyList());
            exchange.getMessage().setHeader("dataExtractCount", 0);
            return null;
        }).when(orderWindowDataExtractorOldProcessor).process(any());

        mockEndpoint.expectedMessageCount(1);
        mockEndpoint.expectedHeaderReceived("dataExtractCount", 0);

        // When
        producerTemplate.sendBody("direct:test-data-extractor", "");

        // Then
        mockEndpoint.assertIsSatisfied();
        verify(orderWindowDataExtractorOldProcessor).process(any());
        verify(xmlProcessor, never()).process(any());
    }

    @Test
    void testTombstoneCleanupRouteWithData() throws Exception {
        // Reset mock endpoint to ensure clean state
        mockEndpoint.reset();

        // Given
        List<String> keysToTombstone = Arrays.asList("key1", "key2");

        doAnswer(invocation -> {
            org.apache.camel.Exchange exchange = invocation.getArgument(0);
            exchange.getMessage().setBody(keysToTombstone);
            exchange.getMessage().setHeader("tombstoneCount", keysToTombstone.size());
            return null;
        }).when(orderWindowTombstoneOldProcessor).process(any());

        when(kafkaTopicConfig.getOrderWindowFilteredTopic()).thenReturn("test-filtered-topic");

        // Don't expect specific headers for tombstone messages as they're split and transformed
        mockEndpoint.expectedMessageCount(2); // One for each key to tombstone

        // When
        producerTemplate.sendBody("direct:test-tombstone-cleanup", "");

        // Then
        mockEndpoint.assertIsSatisfied();
        verify(orderWindowTombstoneOldProcessor).process(any());

        // Verify that the messages have the expected Kafka key headers
        List<org.apache.camel.Exchange> exchanges = mockEndpoint.getExchanges();
        Assertions.assertEquals(2, exchanges.size());
        Assertions.assertTrue(exchanges.stream().anyMatch(ex -> "key1".equals(ex.getMessage().getHeader("CamelKafkaKey"))));
        Assertions.assertTrue(exchanges.stream().anyMatch(ex -> "key2".equals(ex.getMessage().getHeader("CamelKafkaKey"))));
    }

    @Configuration
    @EnableAutoConfiguration
    @Import(CamelAutoConfiguration.class)
    static class TestConfiguration {

        @Bean("orderWindowDataExtractorProcessor")
        public OrderWindowDataExtractorOldProcessor orderWindowDataExtractorProcessor() {
            return mock(OrderWindowDataExtractorOldProcessor.class);
        }

        @Bean("xmlProcessor")
        public OrderWindowConversionProcessor xmlProcessor() {
            return mock(OrderWindowConversionProcessor.class);
        }

        @Bean("orderWindowTombstoneProcessor")
        public OrderWindowTombstoneOldProcessor orderWindowTombstoneProcessor() {
            return mock(OrderWindowTombstoneOldProcessor.class);
        }

        @Bean
        public RouteBuilder testRouteBuilder() {
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    // Test route for data extractor
                    from("direct:test-data-extractor")
                            .process("orderWindowDataExtractorProcessor")
                            .choice()
                            .when(header("dataExtractCount").isGreaterThan(0))
                            .process("xmlProcessor")
                            .endChoice()
                            .end()
                            .to("mock:result");

                    // Test route for tombstone cleanup
                    from("direct:test-tombstone-cleanup")
                            .process("orderWindowTombstoneProcessor")
                            .choice()
                            .when(header("tombstoneCount").isGreaterThan(0))
                            .split(body())
                            .process(exchange -> {
                                String keyToTombstone = exchange.getIn().getBody(String.class);
                                exchange.getMessage().setBody(null);
                                exchange.getMessage().setHeader("CamelKafkaKey", keyToTombstone);
                            })
                            .to("mock:result")
                            .endChoice()
                            .end();
                }
            };
        }
    }
}