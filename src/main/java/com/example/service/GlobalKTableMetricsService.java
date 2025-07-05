package com.example.service;

import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GlobalKTableMetricsService {

    private final KafkaStateStoreService kafkaStateStoreService;

    private final GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;
    
    private final AtomicLong totalRecordsCount = new AtomicLong(0);
    private final AtomicLong duplicateKeysCount = new AtomicLong(0);
    private final AtomicLong tombstoneEligibleCount = new AtomicLong(0);
    private final AtomicLong approvedStatusCount = new AtomicLong(0);
    private final AtomicLong releasedStatusCount = new AtomicLong(0);
    
    private final Counter metricsUpdateCounter;
    private final Counter tombstoneEligibleCounter;
    
    public GlobalKTableMetricsService(KafkaStateStoreService kafkaStateStoreService,
                                      GlobalKTable<String, OrderWindow> orderWindowGlobalKTable,
                                      MeterRegistry meterRegistry) {

        this.kafkaStateStoreService = kafkaStateStoreService;
        this.orderWindowGlobalKTable = orderWindowGlobalKTable;

        // Register gauges for real-time metrics
        Gauge.builder("globalkTable.total.records", this, GlobalKTableMetricsService::getTotalRecordsCount)
                .description("Total number of records in GlobalKTable")
                .register(meterRegistry);
                
        Gauge.builder("globalkTable.duplicate.keys", this, GlobalKTableMetricsService::getDuplicateKeysCount)
                .description("Number of keys with multiple versions in GlobalKTable")
                .register(meterRegistry);
                
        Gauge.builder("globalkTable.tombstone.eligible", this, GlobalKTableMetricsService::getTombstoneEligibleCount)
                .description("Number of records eligible for tombstone")
                .register(meterRegistry);
                
        Gauge.builder("globalkTable.status.approved", this, GlobalKTableMetricsService::getApprovedStatusCount)
                .description("Number of records with APPROVED status")
                .register(meterRegistry);
                
        Gauge.builder("globalkTable.status.released", this, GlobalKTableMetricsService::getReleasedStatusCount)
                .description("Number of records with RELEASED status")
                .register(meterRegistry);
        
        // Register counters for tracking operations
        this.metricsUpdateCounter = Counter.builder("globalkTable.metrics.updates")
                .description("Number of times metrics were updated")
                .register(meterRegistry);
                
        this.tombstoneEligibleCounter = Counter.builder("globalkTable.tombstone.eligible.total")
                .description("Total number of records identified as tombstone eligible")
                .register(meterRegistry);
    }
    
    public GlobalKTableMetrics calculateMetrics() {
        try {
            ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
            
            Map<String, List<OrderWindow>> groupedByKey = new HashMap<>();
            List<OrderWindow> allRecords = new ArrayList<>();
            OffsetDateTime thirteenDaysAgo = OffsetDateTime.now().minusDays(13);
            
            long totalCount = 0;
            long approvedCount = 0;
            long releasedCount = 0;
            long tombstoneEligibleCount = 0;
            
            // Iterate through all key-value pairs in the store
            try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, OrderWindow> entry = iterator.next();
                    OrderWindow orderWindow = entry.value;
                    
                    if (orderWindow != null) {
                        totalCount++;
                        allRecords.add(orderWindow);
                        
                        // Group by ID for duplicate detection
                        groupedByKey.computeIfAbsent(orderWindow.getId(), k -> new ArrayList<>()).add(orderWindow);
                        
                        // Count by status
                        if (orderWindow.getStatus() == OrderStatus.APPROVED) {
                            approvedCount++;
                        } else if (orderWindow.getStatus() == OrderStatus.RELEASED) {
                            releasedCount++;
                            
                            // Check if eligible for tombstone
                            if (orderWindow.getPlanEndDate().isBefore(thirteenDaysAgo)) {
                                tombstoneEligibleCount++;
                            }
                        }
                    }
                }
            }
            
            // Calculate keys with multiple versions
            long duplicateKeys = groupedByKey.values().stream()
                    .mapToLong(orderWindows -> orderWindows.size() > 1 ? 1 : 0)
                    .sum();
            
            // Update atomic counters for gauges
            this.totalRecordsCount.set(totalCount);
            this.duplicateKeysCount.set(duplicateKeys);
            this.tombstoneEligibleCount.set(tombstoneEligibleCount);
            this.approvedStatusCount.set(approvedCount);
            this.releasedStatusCount.set(releasedCount);
            
            // Increment counters
            metricsUpdateCounter.increment();
            tombstoneEligibleCounter.increment(tombstoneEligibleCount);
            
            log.info("GlobalKTable metrics updated - Total: {}, Duplicates: {}, Tombstone Eligible: {}", 
                    totalCount, duplicateKeys, tombstoneEligibleCount);
            
            return GlobalKTableMetrics.builder()
                    .totalRecords(totalCount)
                    .duplicateKeys(duplicateKeys)
                    .tombstoneEligible(tombstoneEligibleCount)
                    .approvedStatus(approvedCount)
                    .releasedStatus(releasedCount)
                    .lastUpdated(OffsetDateTime.now())
                    .build();
                    
        } catch (Exception e) {
            log.error("Error calculating GlobalKTable metrics", e);
            throw new RuntimeException("Failed to calculate GlobalKTable metrics", e);
        }
    }
    
    public double getTotalRecordsCount() {
        return totalRecordsCount.get();
    }
    
    public double getDuplicateKeysCount() {
        return duplicateKeysCount.get();
    }
    
    public double getTombstoneEligibleCount() {
        return tombstoneEligibleCount.get();
    }
    
    public double getApprovedStatusCount() {
        return approvedStatusCount.get();
    }
    
    public double getReleasedStatusCount() {
        return releasedStatusCount.get();
    }
    
    public Map<String, List<OrderWindow>> getDetailedDuplicateKeys() {
        try {
            ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
            Map<String, List<OrderWindow>> groupedByKey = new HashMap<>();
            
            try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, OrderWindow> entry = iterator.next();
                    OrderWindow orderWindow = entry.value;
                    
                    if (orderWindow != null) {
                        groupedByKey.computeIfAbsent(orderWindow.getId(), k -> new ArrayList<>()).add(orderWindow);
                    }
                }
            }
            
            // Return only keys with multiple versions
            return groupedByKey.entrySet().stream()
                    .filter(entry -> entry.getValue().size() > 1)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
        } catch (Exception e) {
            log.error("Error getting detailed duplicate keys", e);
            return Collections.emptyMap();
        }
    }
}