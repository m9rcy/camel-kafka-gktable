package com.example.endpoint;

import com.example.model.GlobalKTableMetrics;
import com.example.service.GlobalKTableMetricsService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Endpoint(id = "globalkTable")
@RequiredArgsConstructor
public class GlobalKTableActuatorEndpoint {
    
    private final GlobalKTableMetricsService metricsService;
    
    @ReadOperation
    public Map<String, Object> globalKTableMetrics() {
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();
        
        Map<String, Object> result = new HashMap<>();
        result.put("totalRecords", metrics.getTotalRecords());
        result.put("duplicateKeys", metrics.getDuplicateKeys());
        result.put("tombstoneEligible", metrics.getTombstoneEligible());
        result.put("statusBreakdown", Map.of(
            "approved", metrics.getApprovedStatus(),
            "released", metrics.getReleasedStatus()
        ));
        result.put("lastUpdated", metrics.getLastUpdated());
        
        return result;
    }
    
    @ReadOperation
    public Map<String, Object> globalKTableMetrics(@Selector String detail) {
        switch (detail.toLowerCase()) {
            case "summary":
                return globalKTableMetrics();
            case "duplicates":
                return Map.of(
                    "duplicateKeys", metricsService.getDetailedDuplicateKeys(),
                    "count", metricsService.getDuplicateKeysCount()
                );
            case "tombstone":
                GlobalKTableMetrics metrics = metricsService.calculateMetrics();
                return Map.of(
                    "tombstoneEligibleCount", metrics.getTombstoneEligible(),
                    "thirteenDaysAgo", java.time.OffsetDateTime.now().minusDays(13)
                );
            default:
                return Map.of("error", "Unknown detail type: " + detail + ". Available: summary, duplicates, tombstone");
        }
    }
}