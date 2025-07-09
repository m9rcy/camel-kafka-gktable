package com.example.service;

import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Updated GlobalKTableMetricsService using the centralized GlobalKTableQueryService.
 * Provides metrics and monitoring capabilities for GlobalKTable data.
 */
@Service
@Slf4j
public class GlobalKTableMetricsService {

    private final GlobalKTableQueryService globalKTableQueryService;

    private final AtomicLong totalRecordsCount = new AtomicLong(0);
    private final AtomicLong duplicateKeysCount = new AtomicLong(0);
    private final AtomicLong tombstoneEligibleCount = new AtomicLong(0);
    private final AtomicLong approvedStatusCount = new AtomicLong(0);
    private final AtomicLong releasedStatusCount = new AtomicLong(0);

    private final Counter metricsUpdateCounter;
    private final Counter tombstoneEligibleCounter;

    public GlobalKTableMetricsService(GlobalKTableQueryService globalKTableQueryService,
                                      MeterRegistry meterRegistry) {
        this.globalKTableQueryService = globalKTableQueryService;

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

    /**
     * Calculate comprehensive metrics for the GlobalKTable.
     *
     * @return GlobalKTableMetrics object with current statistics
     */
    public GlobalKTableMetrics calculateMetrics() {
        try {
            log.debug("Calculating GlobalKTable metrics using query service");

            // Get all values to analyze duplicates
            List<OrderWindow> allValues = globalKTableQueryService.queryAllValues();

            // Calculate total records
            long totalCount = allValues.size();

            // Calculate duplicate keys (multiple versions of same ID)
            Map<String, Long> idCounts = allValues.stream()
                    .collect(Collectors.groupingBy(OrderWindow::getId, Collectors.counting()));
            long duplicateKeys = idCounts.values().stream()
                    .mapToLong(count -> count > 1 ? 1 : 0)
                    .sum();

            // Count by status using query service (deduplicated counts)
            long approvedCount = globalKTableQueryService.countDeduplicated(
                    GlobalKTableQueryService.OrderWindowPredicates.isApproved()
            );

            long releasedCount = globalKTableQueryService.countDeduplicated(
                    GlobalKTableQueryService.OrderWindowPredicates.isReleased()
            );

            // Count tombstone eligible records
            long tombstoneEligibleCount = globalKTableQueryService.count(
                    GlobalKTableQueryService.OrderWindowPredicates.isTombstoneEligible(13)
            );

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

    /**
     * Get detailed information about duplicate keys.
     *
     * @return Map of order IDs to their multiple versions
     */
    public Map<String, List<OrderWindow>> getDetailedDuplicateKeys() {
        try {
            log.debug("Getting detailed duplicate keys using query service");

            // Get all values and group by ID
            List<OrderWindow> allValues = globalKTableQueryService.queryAllValues();

            Map<String, List<OrderWindow>> groupedByKey = allValues.stream()
                    .collect(Collectors.groupingBy(OrderWindow::getId));

            // Return only keys with multiple versions
            return groupedByKey.entrySet().stream()
                    .filter(entry -> entry.getValue().size() > 1)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
        } catch (Exception e) {
            log.error("Error getting detailed duplicate keys", e);
            return Map.of();
        }
    }

    /**
     * Get orders that are eligible for tombstone cleanup.
     *
     * @return List of OrderWindow objects eligible for tombstone
     */
    public List<OrderWindow> getTombstoneEligibleOrders() {
        log.debug("Getting tombstone eligible orders using query service");

        return globalKTableQueryService.queryValues(
                GlobalKTableQueryService.OrderWindowPredicates.isTombstoneEligible(13)
        );
    }

    /**
     * Get orders by status with optional deduplication.
     *
     * @param status The status to filter by
     * @param deduplicated Whether to return deduplicated results
     * @return List of OrderWindow objects with the specified status
     */
    public List<OrderWindow> getOrdersByStatus(OrderStatus status, boolean deduplicated) {
        log.debug("Getting orders by status: {} (deduplicated: {})", status, deduplicated);

        if (deduplicated) {
            return globalKTableQueryService.queryDeduplicatedValues(
                    GlobalKTableQueryService.OrderWindowPredicates.hasStatus(status)
            );
        } else {
            return globalKTableQueryService.queryValues(
                    GlobalKTableQueryService.OrderWindowPredicates.hasStatus(status)
            );
        }
    }

    /**
     * Get orders ending within the specified number of days.
     *
     * @param days Number of days from now
     * @param deduplicated Whether to return deduplicated results
     * @return List of OrderWindow objects ending within the specified days
     */
    public List<OrderWindow> getOrdersEndingWithinDays(int days, boolean deduplicated) {
        log.debug("Getting orders ending within {} days (deduplicated: {})", days, deduplicated);

        OffsetDateTime cutoffDate = OffsetDateTime.now().plusDays(days);

        if (deduplicated) {
            return globalKTableQueryService.queryDeduplicatedValues(
                    GlobalKTableQueryService.OrderWindowPredicates.endDateBeforeOrEqual(cutoffDate)
            );
        } else {
            return globalKTableQueryService.queryValues(
                    GlobalKTableQueryService.OrderWindowPredicates.endDateBeforeOrEqual(cutoffDate)
            );
        }
    }

    /**
     * Get overdue orders (past their planned end date).
     *
     * @param deduplicated Whether to return deduplicated results
     * @return List of overdue OrderWindow objects
     */
    public List<OrderWindow> getOverdueOrders(boolean deduplicated) {
        log.debug("Getting overdue orders (deduplicated: {})", deduplicated);

        if (deduplicated) {
            return globalKTableQueryService.queryDeduplicatedValues(
                    GlobalKTableQueryService.OrderWindowPredicates.endDateBefore(OffsetDateTime.now())
            );
        } else {
            return globalKTableQueryService.queryValues(
                    GlobalKTableQueryService.OrderWindowPredicates.endDateBefore(OffsetDateTime.now())
            );
        }
    }

    /**
     * Check if there are any orders matching the given criteria.
     *
     * @param status The status to check for
     * @return true if any orders exist with the specified status
     */
    public boolean hasOrdersWithStatus(OrderStatus status) {
        return globalKTableQueryService.exists(
                GlobalKTableQueryService.OrderWindowPredicates.hasStatus(status)
        );
    }

    /**
     * Get count of orders by status (deduplicated).
     *
     * @param status The status to count
     * @return Count of orders with the specified status
     */
    public long getOrderCountByStatus(OrderStatus status) {
        return globalKTableQueryService.countDeduplicated(
                GlobalKTableQueryService.OrderWindowPredicates.hasStatus(status)
        );
    }

    /**
     * Get advanced metrics including complex queries.
     *
     * @return Advanced metrics object
     */
    public AdvancedGlobalKTableMetrics getAdvancedMetrics() {
        log.debug("Calculating advanced GlobalKTable metrics");

        OffsetDateTime now = OffsetDateTime.now();

        // Count by various criteria
        long overdueCount = globalKTableQueryService.countDeduplicated(
                GlobalKTableQueryService.OrderWindowPredicates.endDateBefore(now)
        );

        long endingThisWeek = globalKTableQueryService.countDeduplicated(
                GlobalKTableQueryService.OrderWindowPredicates
                        .endDateAfter(now)
                        .and(GlobalKTableQueryService.OrderWindowPredicates.endDateBeforeOrEqual(now.plusDays(7)))
        );

        long activeApproved = globalKTableQueryService.countDeduplicated(
                GlobalKTableQueryService.OrderWindowPredicates.isApproved()
                        .and(GlobalKTableQueryService.OrderWindowPredicates.endDateAfter(now))
        );

        long activeReleased = globalKTableQueryService.countDeduplicated(
                GlobalKTableQueryService.OrderWindowPredicates.isReleased()
                        .and(GlobalKTableQueryService.OrderWindowPredicates.endDateAfter(now))
        );

        return AdvancedGlobalKTableMetrics.builder()
                .overdueOrders(overdueCount)
                .endingThisWeek(endingThisWeek)
                .activeApprovedOrders(activeApproved)
                .activeReleasedOrders(activeReleased)
                .calculatedAt(now)
                .build();
    }

    // Getter methods for Micrometer gauges
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
}

/**
 * DTO for advanced GlobalKTable metrics.
 */
@lombok.Builder
@lombok.Data
class AdvancedGlobalKTableMetrics {
    private final long overdueOrders;
    private final long endingThisWeek;
    private final long activeApprovedOrders;
    private final long activeReleasedOrders;
    private final OffsetDateTime calculatedAt;
}