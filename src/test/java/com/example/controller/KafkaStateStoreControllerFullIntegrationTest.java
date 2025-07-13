package com.example.controller;

import com.example.model.Code;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.EKafkaStateStoreService;
import com.example.service.KafkaStateStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Full Spring Boot integration test for KafkaStateStoreController
 * Uses full application context with test configuration
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(KafkaStateStoreControllerFullIntegrationTest.KafkaStateStoreTestConfiguration.class)
@ActiveProfiles("test")
class KafkaStateStoreControllerFullIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EKafkaStateStoreService kafkaStateStoreService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void applicationContext_shouldStartSuccessfully() {
        // This test verifies that the application context loads successfully
        // with all the required beans
    }

    @Test
    void endToEndWorkflow_shouldWorkCorrectly() throws Exception {
        // Setup test data
        String orderStoreName = "order-window-global-store";
        String orderId = "order-123";

        OrderWindow testOrder = OrderWindow.builder()
                .id(orderId)
                .name("End-to-End Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(2))
                .planEndDate(OffsetDateTime.now().plusDays(28))
                .version(1)
                .idRef("e2e-ref")
                .build();

        // Mock the service calls
        when(kafkaStateStoreService.getAvailableStoreNames())
                .thenReturn(Set.of(orderStoreName, "code-global-store"));

        when(kafkaStateStoreService.getStoreValue(orderStoreName, orderId))
                .thenReturn(testOrder);

        when(kafkaStateStoreService.getStoreStatus(orderStoreName))
                .thenReturn(Map.of(
                        "approximateCount", 42L,
                        "storeName", orderStoreName,
                        "storeType", "OrderWindow",
                        "statusBreakdown", Map.of(
                                "APPROVED", 25L,
                                "DRAFT", 10L,
                                "LODGED", 5L,
                                "RELEASED", 1L,
                                "DONE", 1L
                        ),
                        "activeOrdersCount", 31L
                ));

        // Test the complete workflow
        
        // 1. Discover available stores
        mockMvc.perform(get("/api/stores"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2));

        // 2. Retrieve specific order
        mockMvc.perform(get("/api/{storeName}/{id}", orderStoreName, orderId))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value(orderId))
                .andExpect(jsonPath("$.name").value("End-to-End Test Order"))
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.version").value(1));

        // 3. Get store statistics
        mockMvc.perform(get("/api/{storeName}/status", orderStoreName))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.approximateCount").value(42))
                .andExpect(jsonPath("$.storeName").value(orderStoreName))
                .andExpect(jsonPath("$.storeType").value("OrderWindow"))
                .andExpect(jsonPath("$.statusBreakdown.APPROVED").value(25))
                .andExpect(jsonPath("$.activeOrdersCount").value(31));
    }

    @Test
    void errorScenarios_shouldBeHandledGracefully() throws Exception {
        // Test various error scenarios
        
        // 1. Service throws exception
        when(kafkaStateStoreService.getAvailableStoreNames())
                .thenThrow(new RuntimeException("Service temporarily unavailable"));

        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isInternalServerError());

        // 2. Invalid store name
        when(kafkaStateStoreService.getStoreValue("invalid-store", "any-id"))
                .thenThrow(new IllegalArgumentException("Store not found: invalid-store"));

        mockMvc.perform(get("/api/invalid-store/any-id"))
                .andExpect(status().isBadRequest());

        // 3. Value not found
        when(kafkaStateStoreService.getStoreValue("order-window-global-store", "missing-id"))
                .thenReturn(null);

        mockMvc.perform(get("/api/order-window-global-store/missing-id"))
                .andExpect(status().isNotFound());
    }

    @Test
    void responseFormat_shouldBeConsistent() throws Exception {
        // Test that all endpoints return consistent JSON format
        
        when(kafkaStateStoreService.getAvailableStoreNames())
                .thenReturn(Set.of("test-store"));

        // All endpoints should return JSON with proper content type
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(header().string("Content-Type", "application/json"));

        Code testCode = Code.builder()
                .id("test-code")
                .name("Test Code")
                .description("Test Description")
                .build();

        when(kafkaStateStoreService.getStoreValue("test-store", "test-code"))
                .thenReturn(testCode);

        mockMvc.perform(get("/api/test-store/test-code"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value("test-code"))
                .andExpect(jsonPath("$.name").value("Test Code"));

        when(kafkaStateStoreService.getStoreStatus("test-store"))
                .thenReturn(Map.of("approximateCount", 1L, "storeName", "test-store"));

        mockMvc.perform(get("/api/test-store/status"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.approximateCount").value(1))
                .andExpect(jsonPath("$.storeName").value("test-store"));
    }

    @TestConfiguration
    public class KafkaStateStoreTestConfiguration {

//        /**
//         * Mock StreamsBuilderFactoryBean for testing
//         * This prevents the need for actual Kafka infrastructure in tests
//         */
//        @Bean
//        public StreamsBuilderFactoryBean streamsBuilderFactoryBean() {
//            return mock(StreamsBuilderFactoryBean.class);
//        }
//
//        /**
//         * Mock StreamsBuilder for testing
//         */
//        @Bean
//        public StreamsBuilder streamsBuilder() {
//            return mock(StreamsBuilder.class);
//        }
    }
}