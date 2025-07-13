package com.example.controller;

import com.example.model.Code;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.EKafkaStateStoreService;
import com.example.service.KafkaStateStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaStateStoreController.class)
class KafkaStateStoreControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EKafkaStateStoreService kafkaStateStoreService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void getAvailableStores_shouldReturnStoreNames() throws Exception {
        // Given
        Set<String> storeNames = Set.of("order-window-global-store", "code-global-store");
        when(kafkaStateStoreService.getAvailableStoreNames()).thenReturn(storeNames);

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[*]").value(org.hamcrest.Matchers.containsInAnyOrder(
                        "order-window-global-store", "code-global-store")));
    }

    @Test
    void getAvailableStores_shouldReturnInternalServerError_whenServiceThrowsException() throws Exception {
        // Given
        when(kafkaStateStoreService.getAvailableStoreNames()).thenThrow(new RuntimeException("Service error"));

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void getStoreValue_shouldReturnOrderWindow_whenExists() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        String id = "order-123";
        OrderWindow orderWindow = OrderWindow.builder()
                .id(id)
                .name("Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(30))
                .version(1)
                .idRef("ref-123")
                .build();

        when(kafkaStateStoreService.getStoreValue(storeName, id)).thenReturn(orderWindow);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/{id}", storeName, id))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.name").value("Test Order"))
                .andExpect(jsonPath("$.status").value("APPROVED"))
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.idRef").value("ref-123"));
    }

    @Test
    void getStoreValue_shouldReturnCode_whenExists() throws Exception {
        // Given
        String storeName = "code-global-store";
        String id = "code-123";
        Code code = Code.builder()
                .id(id)
                .name("Test Code")
                .description("Test Description")
                .build();

        when(kafkaStateStoreService.getStoreValue(storeName, id)).thenReturn(code);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/{id}", storeName, id))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.name").value("Test Code"))
                .andExpect(jsonPath("$.description").value("Test Description"));
    }

    @Test
    void getStoreValue_shouldReturnNotFound_whenValueDoesNotExist() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        String id = "non-existent-id";
        when(kafkaStateStoreService.getStoreValue(storeName, id)).thenReturn(null);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/{id}", storeName, id))
                .andExpect(status().isNotFound());
    }

    @Test
    void getStoreValue_shouldReturnBadRequest_whenStoreNameIsInvalid() throws Exception {
        // Given
        String storeName = "invalid-store";
        String id = "some-id";
        when(kafkaStateStoreService.getStoreValue(storeName, id))
                .thenThrow(new IllegalArgumentException("Store not found"));

        // When & Then
        mockMvc.perform(get("/api/{storeName}/{id}", storeName, id))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getStoreValue_shouldReturnInternalServerError_whenServiceThrowsException() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        String id = "some-id";
        when(kafkaStateStoreService.getStoreValue(storeName, id))
                .thenThrow(new RuntimeException("Service error"));

        // When & Then
        mockMvc.perform(get("/api/{storeName}/{id}", storeName, id))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void getStoreStatus_shouldReturnOrderWindowStatus() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        Map<String, Object> statusMap = Map.of(
                "approximateCount", 100L,
                "storeName", storeName,
                "storeType", "OrderWindow",
                "statusBreakdown", Map.of(
                        "APPROVED", 30L,
                        "DRAFT", 20L,
                        "LODGED", 25L,
                        "RELEASED", 15L,
                        "DONE", 10L
                ),
                "activeOrdersCount", 70L
        );

        when(kafkaStateStoreService.getStoreStatus(storeName)).thenReturn(statusMap);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/status", storeName))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.approximateCount").value(100))
                .andExpect(jsonPath("$.storeName").value(storeName))
                .andExpect(jsonPath("$.storeType").value("OrderWindow"))
                .andExpect(jsonPath("$.statusBreakdown.APPROVED").value(30))
                .andExpect(jsonPath("$.statusBreakdown.DRAFT").value(20))
                .andExpect(jsonPath("$.statusBreakdown.LODGED").value(25))
                .andExpect(jsonPath("$.statusBreakdown.RELEASED").value(15))
                .andExpect(jsonPath("$.statusBreakdown.DONE").value(10))
                .andExpect(jsonPath("$.activeOrdersCount").value(70));
    }

    @Test
    void getStoreStatus_shouldReturnCodeStatus() throws Exception {
        // Given
        String storeName = "code-global-store";
        Map<String, Object> statusMap = Map.of(
                "approximateCount", 50L,
                "storeName", storeName,
                "storeType", "Code",
                "codesWithDescription", 40L,
                "codesWithoutDescription", 10L
        );

        when(kafkaStateStoreService.getStoreStatus(storeName)).thenReturn(statusMap);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/status", storeName))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.approximateCount").value(50))
                .andExpect(jsonPath("$.storeName").value(storeName))
                .andExpect(jsonPath("$.storeType").value("Code"))
                .andExpect(jsonPath("$.codesWithDescription").value(40))
                .andExpect(jsonPath("$.codesWithoutDescription").value(10));
    }

    @Test
    void getStoreStatus_shouldReturnBadRequest_whenStoreNameIsInvalid() throws Exception {
        // Given
        String storeName = "invalid-store";
        when(kafkaStateStoreService.getStoreStatus(storeName))
                .thenThrow(new IllegalArgumentException("Store not found"));

        // When & Then
        mockMvc.perform(get("/api/{storeName}/status", storeName))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getStoreStatus_shouldReturnInternalServerError_whenServiceThrowsException() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        when(kafkaStateStoreService.getStoreStatus(storeName))
                .thenThrow(new RuntimeException("Service error"));

        // When & Then
        mockMvc.perform(get("/api/{storeName}/status", storeName))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void getStoreStatus_shouldHandleStatisticsError() throws Exception {
        // Given
        String storeName = "order-window-global-store";
        Map<String, Object> statusMap = Map.of(
                "approximateCount", 100L,
                "storeName", storeName,
                "storeType", "OrderWindow",
                "statisticsError", "Unable to calculate detailed statistics"
        );

        when(kafkaStateStoreService.getStoreStatus(storeName)).thenReturn(statusMap);

        // When & Then
        mockMvc.perform(get("/api/{storeName}/status", storeName))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.approximateCount").value(100))
                .andExpect(jsonPath("$.statisticsError").value("Unable to calculate detailed statistics"));
    }
}