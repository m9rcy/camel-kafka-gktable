package com.example.controller;

import com.example.model.GlobalKTableMetrics;
import com.example.service.GlobalKTableMetricsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class GlobalKTableMetricsController {
    
    private final GlobalKTableMetricsService metricsService;
    
    @GetMapping("/globalkTable")
    public ResponseEntity<Map<String, Object>> getGlobalKTableMetrics() {
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("metrics", Map.of(
            "totalRecords", metrics.getTotalRecords(),
            "duplicateKeys", metrics.getDuplicateKeys(),
            "tombstoneEligible", metrics.getTombstoneEligible(),
            "statusBreakdown", Map.of(
                "approved", metrics.getApprovedStatus(),
                "released", metrics.getReleasedStatus()
            ),
            "lastUpdated", metrics.getLastUpdated()
        ));
        
        return ResponseEntity.ok(response);
    }
    
    @GetMapping("/globalkTable/duplicates")
    public ResponseEntity<Map<String, Object>> getDuplicateKeys() {
        Map<String, Object> response = new HashMap<>();
        response.put("duplicateKeys", metricsService.getDetailedDuplicateKeys());
        response.put("count", metricsService.getDuplicateKeysCount());
        
        return ResponseEntity.ok(response);
    }
    
    @GetMapping("/globalkTable/tombstone")
    public ResponseEntity<Map<String, Object>> getTombstoneEligible() {
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("tombstoneEligibleCount", metrics.getTombstoneEligible());
        response.put("thirteenDaysAgo", java.time.OffsetDateTime.now().minusDays(13));
        response.put("lastCalculated", metrics.getLastUpdated());
        
        return ResponseEntity.ok(response);
    }
    
    @GetMapping("/globalkTable/health")
    public ResponseEntity<Map<String, Object>> getGlobalKTableHealth() {
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("details", Map.of(
            "totalRecords", metrics.getTotalRecords(),
            "duplicateKeys", metrics.getDuplicateKeys(),
            "tombstoneEligible", metrics.getTombstoneEligible(),
            "healthScore", calculateHealthScore(metrics)
        ));
        
        return ResponseEntity.ok(response);
    }
    
    private double calculateHealthScore(GlobalKTableMetrics metrics) {
        // Simple health score calculation
        // Lower duplicate percentage and tombstone percentage = better health
        if (metrics.getTotalRecords() == 0) return 100.0;
        
        double duplicatePercentage = (double) metrics.getDuplicateKeys() / metrics.getTotalRecords() * 100;
        double tombstonePercentage = (double) metrics.getTombstoneEligible() / metrics.getTotalRecords() * 100;
        
        // Health score: 100 - (duplicate% + tombstone%)
        return Math.max(0, 100 - duplicatePercentage - tombstonePercentage);
    }
}