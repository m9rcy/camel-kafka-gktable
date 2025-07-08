package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorOldProcessor;
import com.example.processor.OrderWindowTombstoneOldProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@CamelSpringBootTest
class CamelRouteIntegrationTestNoWorking extends BaseIntegrationTest {

    @Autowired
    private CamelContext camelContext;
    
    @Produce("direct:test-data-extractor")
    private ProducerTemplate producerTemplate;
    
    @EndpointInject("mock:result")
    private MockEndpoint mockEndpoint;
    
    @MockBean
    private OrderWindowDataExtractorOldProcessor orderWindowDataExtractorOldProcessor;
    
    @MockBean
    private OrderWindowConversionProcessor xmlProcessor;
    
    @MockBean
    private OrderWindowTombstoneOldProcessor orderWindowTombstoneOldProcessor;
    
    @MockBean
    private KafkaTopicConfig kafkaTopicConfig;
    
    @BeforeEach
    void setUp() throws Exception {
        // Add test routes to the context
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("direct:test-data-extractor")
                    .process(orderWindowDataExtractorOldProcessor)
                    .choice()
                        .when(header("dataExtractCount").isGreaterThan(0))
                            .process(xmlProcessor)
                        .endChoice()
                    .end()
                    .to("mock:result");
                
                from("direct:test-tombstone-cleanup")
                    .process(orderWindowTombstoneOldProcessor)
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
        });
        
        mockEndpoint.reset();
    }
    
    @Test
    void testOrderWindowConsumerRouteWithData() throws Exception {
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
        // Reset for this test
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
        verify(orderWindowDataExtractorOldProcessor, atLeastOnce()).process(any());
        verify(xmlProcessor, never()).process(any());
    }
    
    @Test
    void testTombstoneCleanupRouteWithData() throws Exception {
        // Reset for this test
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
        
        mockEndpoint.expectedMessageCount(2);
        
        // When
        producerTemplate.sendBody("direct:test-tombstone-cleanup", "");
        
        // Then
        mockEndpoint.assertIsSatisfied();
        verify(orderWindowTombstoneOldProcessor, atLeastOnce()).process(any());
        
        // Verify that the messages have the expected Kafka key headers
        List<org.apache.camel.Exchange> exchanges = mockEndpoint.getExchanges();
        assertEquals(2, exchanges.size());
        assertTrue(exchanges.stream().anyMatch(ex -> "key1".equals(ex.getMessage().getHeader("CamelKafkaKey"))));
        assertTrue(exchanges.stream().anyMatch(ex -> "key2".equals(ex.getMessage().getHeader("CamelKafkaKey"))));
    }
}