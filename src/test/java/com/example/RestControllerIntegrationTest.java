package com.example;

import com.example.controller.GlobalKTableMetricsController;
import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderWindow;
import com.example.service.GlobalKTableMetricsService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(GlobalKTableMetricsController.class)
class RestControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
    
    @MockBean
    private GlobalKTableMetricsService metricsService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Test
    void testGetGlobalKTableMetrics() throws Exception {
        // Given
        GlobalKTableMetrics metrics = GlobalKTableMetrics.builder()
                .totalRecords(100)
                .duplicateKeys(5)
                .tombstoneEligible(10)
                .approvedStatus(60)
                .releasedStatus(40)
                .lastUpdated(OffsetDateTime.now())
                .build();
        
        when(metricsService.calculateMetrics()).thenReturn(metrics);
        
        // When & Then
        mockMvc.perform(get("/api/metrics/globalkTable")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.metrics.totalRecords", is(100)))
                .andExpect(jsonPath("$.metrics.duplicateKeys", is(5)))
                .andExpect(jsonPath("$.metrics.tombstoneEligible", is(10)))
                .andExpect(jsonPath("$.metrics.statusBreakdown.approved", is(60)))
                .andExpect(jsonPath("$.metrics.statusBreakdown.released", is(40)))
                .andExpect(jsonPath("$.metrics.lastUpdated", notNullValue()));
    }
    
    @Test
    void testGetDuplicateKeys() throws Exception {
        // Given
        Map<String, List<OrderWindow>> duplicates = Collections.singletonMap("order1", 
                Collections.singletonList(OrderWindow.builder()
                        .id("order1")
                        .name("Test")
                        .version(1)
                        .build()));
        
        when(metricsService.getDetailedDuplicateKeys()).thenReturn(duplicates);
        when(metricsService.getDuplicateKeysCount()).thenReturn(1.0);
        
        // When & Then
        mockMvc.perform(get("/api/metrics/globalkTable/duplicates")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicateKeys", hasKey("order1")))
                .andExpect(jsonPath("$.count", is(1.0)));
    }
    
    @Test
    void testGetTombstoneEligible() throws Exception {
        // Given
        GlobalKTableMetrics metrics = GlobalKTableMetrics.builder()
                .totalRecords(100)
                .duplicateKeys(5)
                .tombstoneEligible(10)
                .approvedStatus(60)
                .releasedStatus(40)
                .lastUpdated(OffsetDateTime.now())
                .build();
        
        when(metricsService.calculateMetrics()).thenReturn(metrics);
        
        // When & Then
        mockMvc.perform(get("/api/metrics/globalkTable/tombstone")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tombstoneEligibleCount", is(10)))
                .andExpect(jsonPath("$.thirteenDaysAgo", notNullValue()))
                .andExpect(jsonPath("$.lastCalculated", notNullValue()));
    }
    
    @Test
    void testGetGlobalKTableHealth() throws Exception {
        // Given
        GlobalKTableMetrics metrics = GlobalKTableMetrics.builder()
                .totalRecords(100)
                .duplicateKeys(5)
                .tombstoneEligible(10)
                .approvedStatus(60)
                .releasedStatus(40)
                .lastUpdated(OffsetDateTime.now())
                .build();
        
        when(metricsService.calculateMetrics()).thenReturn(metrics);
        
        // When & Then
        mockMvc.perform(get("/api/metrics/globalkTable/health")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("UP")))
                .andExpect(jsonPath("$.details.totalRecords", is(100)))
                .andExpect(jsonPath("$.details.duplicateKeys", is(5)))
                .andExpect(jsonPath("$.details.tombstoneEligible", is(10)))
                .andExpect(jsonPath("$.details.healthScore", is(85.0))); // 100 - 5 - 10 = 85
    }
}