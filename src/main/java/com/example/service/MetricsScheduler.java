package com.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricsScheduler {
    
    private final GlobalKTableMetricsService metricsService;
    
    /**
     * Updates GlobalKTable metrics every 5 minutes
     */
    @Scheduled(fixedRate = 300000) // 5 minutes
    public void updateMetrics() {
        try {
            log.debug("Updating GlobalKTable metrics...");
            metricsService.calculateMetrics();
            log.debug("GlobalKTable metrics updated successfully");
        } catch (Exception e) {
            log.error("Error updating GlobalKTable metrics", e);
        }
    }
    
    /**
     * Updates metrics every hour for more detailed analysis
     */
    @Scheduled(fixedRate = 3600000) // 1 hour
    public void updateDetailedMetrics() {
        try {
            log.info("Performing detailed GlobalKTable metrics analysis...");
            var metrics = metricsService.calculateMetrics();
            var duplicates = metricsService.getDetailedDuplicateKeys();
            
            log.info("Detailed Metrics Summary:");
            log.info("- Total Records: {}", metrics.getTotalRecords());
            log.info("- Duplicate Keys: {} ({})", metrics.getDuplicateKeys(), duplicates.keySet());
            log.info("- Tombstone Eligible: {}", metrics.getTombstoneEligible());
            log.info("- Status Breakdown - Approved: {}, Released: {}", 
                    metrics.getApprovedStatus(), metrics.getReleasedStatus());
                    
        } catch (Exception e) {
            log.error("Error updating detailed GlobalKTable metrics", e);
        }
    }
}