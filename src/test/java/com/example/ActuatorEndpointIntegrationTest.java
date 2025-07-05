package com.example;

import com.example.endpoint.GlobalKTableActuatorEndpoint;
import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderWindow;
import com.example.service.GlobalKTableMetricsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ActuatorEndpointIntegrationTest {

    @Mock
    private GlobalKTableMetricsService metricsService;
    
    @InjectMocks
    private GlobalKTableActuatorEndpoint endpoint;
    
    @Test
    void testGlobalKTableMetrics() {
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
        
        // When
        Map<String, Object> result = endpoint.globalKTableMetrics();
        
        // Then
        assertEquals(100L, result.get("totalRecords"));
        assertEquals(5L, result.get("duplicateKeys"));
        assertEquals(10L, result.get("tombstoneEligible"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> statusBreakdown = (Map<String, Object>) result.get("statusBreakdown");
        assertEquals(60L, statusBreakdown.get("approved"));
        assertEquals(40L, statusBreakdown.get("released"));
        
        assertNotNull(result.get("lastUpdated"));
    }
    
    @Test
    void testGlobalKTableMetricsWithSummaryDetail() {
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
        
        // When
        Map<String, Object> result = endpoint.globalKTableMetrics("summary");
        
        // Then
        assertEquals(100L, result.get("totalRecords"));
        assertEquals(5L, result.get("duplicateKeys"));
        assertEquals(10L, result.get("tombstoneEligible"));
    }
    
    @Test
    void testGlobalKTableMetricsWithDuplicatesDetail() {
        // Given
        Map<String, List<OrderWindow>> duplicates = Collections.singletonMap("order1", 
                Collections.singletonList(OrderWindow.builder()
                        .id("order1")
                        .name("Test")
                        .version(1)
                        .build()));
        
        when(metricsService.getDetailedDuplicateKeys()).thenReturn(duplicates);
        when(metricsService.getDuplicateKeysCount()).thenReturn(1.0);
        
        // When
        Map<String, Object> result = endpoint.globalKTableMetrics("duplicates");
        
        // Then
        assertTrue(result.containsKey("duplicateKeys"));
        assertEquals(1.0, result.get("count"));
    }
    
    @Test
    void testGlobalKTableMetricsWithTombstoneDetail() {
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
        
        // When
        Map<String, Object> result = endpoint.globalKTableMetrics("tombstone");
        
        // Then
        assertEquals(10L, result.get("tombstoneEligibleCount"));
        assertTrue(result.containsKey("thirteenDaysAgo"));
    }
    
    @Test
    void testGlobalKTableMetricsWithUnknownDetail() {
        // When
        Map<String, Object> result = endpoint.globalKTableMetrics("unknown");
        
        // Then
        assertTrue(result.containsKey("error"));
        assertEquals("Unknown detail type: unknown. Available: summary, duplicates, tombstone", 
                result.get("error"));
    }
}