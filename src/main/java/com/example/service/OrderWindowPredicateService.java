package com.example.service;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.function.Predicate;

@Service
@Slf4j
public class OrderWindowPredicateService {

    private static final int GLOBAL_KTABLE_RETENTION_DAYS = 12;
    private static final int TOMBSTONE_THRESHOLD_DAYS = 13;

    private final Clock clock;

    /**
     * Constructor with Clock dependency injection.
     * Uses system clock by default, but can be overridden for testing.
     */
    public OrderWindowPredicateService(Clock clock) {
        this.clock = clock;
    }

    /**
     * Default constructor that uses system clock.
     * For backward compatibility and when clock injection is not needed.
     */
    public OrderWindowPredicateService() {
        this(Clock.systemDefaultZone());
    }

    /**
     * Get current time using the injected clock.
     */
    private OffsetDateTime now() {
        return OffsetDateTime.now(clock);
    }

    /**
     * Creates a predicate for GlobalKTable eligibility
     * - APPROVED orders are always eligible
     * - RELEASED orders are eligible if planEndDate is within retention period
     * - All other statuses are filtered out
     */
    public Predicate<OrderWindow> createGlobalKTableEligibilityPredicate() {
        return isApproved().or(isReleasedWithinRetentionPeriod());
    }

    /**
     * Creates a predicate for tombstone eligibility
     * - RELEASED orders with planEndDate older than threshold days
     */
    public Predicate<OrderWindow> createTombstoneEligibilityPredicate() {
        return isReleased().and(isOlderThan(TOMBSTONE_THRESHOLD_DAYS));
    }

    /**
     * Creates a predicate for data extraction
     * - All records that are eligible for GlobalKTable
     */
    public Predicate<OrderWindow> createDataExtractionPredicate() {
        return createGlobalKTableEligibilityPredicate();
    }

    // ========== Base Predicates ==========

    public Predicate<OrderWindow> isApproved() {
        return orderWindow -> orderWindow.getStatus() == OrderStatus.APPROVED;
    }

    public Predicate<OrderWindow> isReleased() {
        return orderWindow -> orderWindow.getStatus() == OrderStatus.RELEASED;
    }

    public Predicate<OrderWindow> isDraft() {
        return orderWindow -> orderWindow.getStatus() == OrderStatus.DRAFT;
    }

    public Predicate<OrderWindow> isDone() {
        return orderWindow -> orderWindow.getStatus() == OrderStatus.DONE;
    }

    public Predicate<OrderWindow> isLodged() {
        return orderWindow -> orderWindow.getStatus() == OrderStatus.LODGED;
    }

    public Predicate<OrderWindow> hasStatus(OrderStatus status) {
        return orderWindow -> orderWindow.getStatus() == status;
    }

    // ========== Date-based Predicates (using Clock) ==========

    public Predicate<OrderWindow> isOlderThan(int days) {
        return orderWindow -> {
            OffsetDateTime threshold = now().minusDays(days);
            return orderWindow.getPlanEndDate().isBefore(threshold);
        };
    }

    public Predicate<OrderWindow> isNewerThan(int days) {
        return orderWindow -> {
            OffsetDateTime threshold = now().minusDays(days);
            return orderWindow.getPlanEndDate().isAfter(threshold);
        };
    }

    public Predicate<OrderWindow> isWithinDays(int days) {
        return orderWindow -> {
            OffsetDateTime threshold = now().minusDays(days);
            return orderWindow.getPlanEndDate().isAfter(threshold) ||
                    orderWindow.getPlanEndDate().equals(threshold);
        };
    }

    public Predicate<OrderWindow> endDateBefore(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isBefore(date);
    }

    public Predicate<OrderWindow> endDateAfter(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isAfter(date);
    }

    public Predicate<OrderWindow> endDateBetween(OffsetDateTime start, OffsetDateTime end) {
        return orderWindow -> {
            OffsetDateTime planEndDate = orderWindow.getPlanEndDate();
            return planEndDate.isAfter(start) && planEndDate.isBefore(end);
        };
    }

    // ========== Version-based Predicates ==========

    public Predicate<OrderWindow> hasVersionGreaterThan(int version) {
        return orderWindow -> orderWindow.getVersion() > version;
    }

    public Predicate<OrderWindow> hasVersionLessThan(int version) {
        return orderWindow -> orderWindow.getVersion() < version;
    }

    public Predicate<OrderWindow> hasVersion(int version) {
        return orderWindow -> orderWindow.getVersion() == version;
    }

    // ========== Composite Predicates (using Clock) ==========

    public Predicate<OrderWindow> isReleasedWithinRetentionPeriod() {
        return isReleased().and(isWithinDays(GLOBAL_KTABLE_RETENTION_DAYS));
    }

    public Predicate<OrderWindow> isApprovedOrRecentlyReleased() {
        return isApproved().or(isReleasedWithinRetentionPeriod());
    }

    public Predicate<OrderWindow> isExpiredReleased() {
        return isReleased().and(isOlderThan(GLOBAL_KTABLE_RETENTION_DAYS));
    }

    // ========== Utility Methods ==========

    /**
     * Creates a predicate that matches any of the given statuses
     */
    public Predicate<OrderWindow> hasAnyStatus(OrderStatus... statuses) {
        return orderWindow -> {
            for (OrderStatus status : statuses) {
                if (orderWindow.getStatus() == status) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Creates a predicate that excludes the given statuses
     */
    public Predicate<OrderWindow> doesNotHaveStatus(OrderStatus... statuses) {
        return hasAnyStatus(statuses).negate();
    }

    /**
     * Creates a predicate for records that need immediate processing (using Clock)
     */
    public Predicate<OrderWindow> requiresImmediateProcessing() {
        return isApproved().or(
                isReleased().and(isWithinDays(1))
        );
    }

    /**
     * Creates a predicate for records that are eligible for archival (using Clock)
     */
    public Predicate<OrderWindow> isEligibleForArchival() {
        return (isDone().or(isReleased())).and(isOlderThan(30));
    }

    // ========== Configuration Methods ==========

    public int getGlobalKTableRetentionDays() {
        return GLOBAL_KTABLE_RETENTION_DAYS;
    }

    public int getTombstoneThresholdDays() {
        return TOMBSTONE_THRESHOLD_DAYS;
    }

    /**
     * Get the current time from the injected clock (useful for testing)
     */
    public OffsetDateTime getCurrentTime() {
        return now();
    }

    /**
     * Logs the predicate logic for debugging
     */
    public void logPredicateLogic(String predicateName, OrderWindow orderWindow, boolean result) {
        log.debug("Predicate '{}' evaluation for OrderWindow[id={}, status={}, planEndDate={}]: {}",
                predicateName, orderWindow.getId(), orderWindow.getStatus(),
                orderWindow.getPlanEndDate(), result);
    }
}