// MetricsSchedulerTest.java
package com.example.service;

import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetricsSchedulerTest {

    @Mock
    private GlobalKTableMetricsService metricsService;
    
    private MetricsScheduler scheduler;
    
    @BeforeEach
    void setUp() {
        scheduler = new MetricsScheduler(metricsService);
    }
    
    @Test
    void testUpdateMetrics() {
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
        scheduler.updateMetrics();
        
        // Then
        verify(metricsService).calculateMetrics();
    }
    
    @Test
    void testUpdateMetricsHandlesException() {
        // Given
        when(metricsService.calculateMetrics()).thenThrow(new RuntimeException("Test exception"));
        
        // When & Then (should not throw exception)
        scheduler.updateMetrics();
        
        verify(metricsService).calculateMetrics();
    }
    
    @Test
    void testUpdateDetailedMetrics() {
        // Given
        GlobalKTableMetrics metrics = GlobalKTableMetrics.builder()
                .totalRecords(100)
                .duplicateKeys(5)
                .tombstoneEligible(10)
                .approvedStatus(60)
                .releasedStatus(40)
                .lastUpdated(OffsetDateTime.now())
                .build();
                
        Map<String, java.util.List<OrderWindow>> duplicates = Collections.emptyMap();
        
        when(metricsService.calculateMetrics()).thenReturn(metrics);
        when(metricsService.getDetailedDuplicateKeys()).thenReturn(duplicates);
        
        // When
        scheduler.updateDetailedMetrics();
        
        // Then
        verify(metricsService).calculateMetrics();
        verify(metricsService).getDetailedDuplicateKeys();
    }
    
    @Test
    void testUpdateDetailedMetricsHandlesException() {
        // Given
        when(metricsService.calculateMetrics()).thenThrow(new RuntimeException("Test exception"));
        
        // When & Then (should not throw exception)
        scheduler.updateDetailedMetrics();
        
        verify(metricsService).calculateMetrics();
    }
}