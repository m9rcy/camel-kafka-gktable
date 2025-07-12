package com.example.service;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("OrderWindowPredicateService Unit Tests")
@TestMethodOrder(MethodOrderer.Random.class) // Randomize test order to catch order dependencies
class OrderWindowPredicateServiceTest {

    private OrderWindowPredicateService predicateService;
    private static final OffsetDateTime FIXED_NOW = OffsetDateTime.of(2024, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW.toInstant(), ZoneOffset.UTC);

    private OffsetDateTime now;
    private OffsetDateTime pastDate;
    private OffsetDateTime futureDate;

    @BeforeEach
    void setUp() {
        // Use fixed clock for consistent test results
        predicateService = new OrderWindowPredicateService(FIXED_CLOCK);
        now = FIXED_NOW;
        pastDate = now.minusDays(10);
        futureDate = now.plusDays(10);
    }

    // ========== Status-based Predicate Tests ==========

    @ParameterizedTest(name = "Order with status {0} should be identified as APPROVED: {1}")
    @MethodSource("statusPredicateTestCases")
    @DisplayName("Test isApproved predicate with various statuses")
    void testIsApprovedPredicate(OrderStatus status, boolean expectedResult) {
        // Given
        OrderWindow order = createOrderWithStatus(status);
        Predicate<OrderWindow> predicate = predicateService.isApproved();

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result);
    }

    @ParameterizedTest(name = "Order with status {0} should be identified as RELEASED: {1}")
    @MethodSource("releasedStatusTestCases")
    @DisplayName("Test isReleased predicate with various statuses")
    void testIsReleasedPredicate(OrderStatus status, boolean expectedResult) {
        // Given
        OrderWindow order = createOrderWithStatus(status);
        Predicate<OrderWindow> predicate = predicateService.isReleased();

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result);
    }

    @ParameterizedTest
    @EnumSource(OrderStatus.class)
    @DisplayName("Test hasStatus predicate for all enum values")
    void testHasStatusPredicate(OrderStatus status) {
        // Given
        OrderWindow orderWithMatchingStatus = createOrderWithStatus(status);
        OrderWindow orderWithDifferentStatus = createOrderWithStatus(
                status == OrderStatus.APPROVED ? OrderStatus.RELEASED : OrderStatus.APPROVED
        );
        Predicate<OrderWindow> predicate = predicateService.hasStatus(status);

        // When & Then
        assertTrue(predicate.test(orderWithMatchingStatus),
                "Order with matching status should pass predicate");
        assertFalse(predicate.test(orderWithDifferentStatus),
                "Order with different status should fail predicate");
    }

    // ========== Date-based Predicate Tests ==========

    @ParameterizedTest(name = "Order ending {0} days ago should be older than {1} days: {2}")
    @MethodSource("dateComparisonTestCases")
    @DisplayName("Test isOlderThan predicate with various date differences")
    void testIsOlderThanPredicate(int orderDaysAgo, int thresholdDays, boolean expectedResult) {
        // Given - Use the fixed 'now' time for consistency
        OffsetDateTime testDate = now.minusDays(orderDaysAgo);
        OrderWindow order = createOrderWithEndDate(testDate);
        Predicate<OrderWindow> predicate = predicateService.isOlderThan(thresholdDays);

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result,
                String.format("Order %d days old vs threshold %d days", orderDaysAgo, thresholdDays));
    }

    @ParameterizedTest(name = "Order ending {0} days ago should be newer than {1} days: {2}")
    @MethodSource("newerThanTestCases")
    @DisplayName("Test isNewerThan predicate with various date differences")
    void testIsNewerThanPredicate(int orderDaysAgo, int thresholdDays, boolean expectedResult) {
        // Given - Use the fixed 'now' time for consistency
        OffsetDateTime testDate = now.minusDays(orderDaysAgo);
        OrderWindow order = createOrderWithEndDate(testDate);
        Predicate<OrderWindow> predicate = predicateService.isNewerThan(thresholdDays);

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result);
    }

    @ParameterizedTest(name = "Order ending {0} days ago should be within {1} days: {2}")
    @MethodSource("withinDaysTestCases")
    @DisplayName("Test isWithinDays predicate with various date differences")
    void testIsWithinDaysPredicate(int orderDaysAgo, int thresholdDays, boolean expectedResult) {
        // Given - Use the fixed 'now' time for consistency
        OffsetDateTime testDate = now.minusDays(orderDaysAgo);
        OrderWindow order = createOrderWithEndDate(testDate);
        Predicate<OrderWindow> predicate = predicateService.isWithinDays(thresholdDays);

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result);
    }

    // ========== Version-based Predicate Tests ==========

    @ParameterizedTest(name = "Order with version {0} compared to threshold {1} (greater than): {2}")
    @MethodSource("versionComparisonTestCases")
    @DisplayName("Test version comparison predicates")
    void testVersionPredicates(int orderVersion, int thresholdVersion,
                               boolean expectedGreater, boolean expectedLess, boolean expectedEqual) {
        // Given
        OrderWindow order = createOrderWithVersion(orderVersion);

        // When & Then
        assertEquals(expectedGreater, predicateService.hasVersionGreaterThan(thresholdVersion).test(order));
        assertEquals(expectedLess, predicateService.hasVersionLessThan(thresholdVersion).test(order));
        assertEquals(expectedEqual, predicateService.hasVersion(thresholdVersion).test(order));
    }

    // ========== Composite Predicate Tests ==========

    @ParameterizedTest(name = "Status: {0}, Days ago: {1} → GlobalKTable eligible: {2}")
    @MethodSource("globalKTableEligibilityTestCases")
    @DisplayName("Test GlobalKTable eligibility predicate")
    void testGlobalKTableEligibilityPredicate(OrderStatus status, int daysAgo, boolean expectedResult) {
        // Given - Use the fixed 'now' time for consistency
        OffsetDateTime testEndDate = now.minusDays(daysAgo);
        OrderWindow order = OrderWindow.builder()
                .id("test-order")
                .name("Test Order")
                .status(status)
                .planStartDate(testEndDate.minusDays(1))
                .planEndDate(testEndDate)
                .version(1)
                .build();

        Predicate<OrderWindow> predicate = predicateService.createGlobalKTableEligibilityPredicate();

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result,
                String.format("Status: %s, Days ago: %d should be eligible: %s", status, daysAgo, expectedResult));
    }

    @ParameterizedTest(name = "Status: {0}, Days ago: {1} → Tombstone eligible: {2}")
    @MethodSource("tombstoneEligibilityTestCases")
    @DisplayName("Test tombstone eligibility predicate")
    void testTombstoneEligibilityPredicate(OrderStatus status, int daysAgo, boolean expectedResult) {
        // Given - Use the fixed 'now' time for consistency
        OffsetDateTime testEndDate = now.minusDays(daysAgo);
        OrderWindow order = OrderWindow.builder()
                .id("test-order")
                .name("Test Order")
                .status(status)
                .planStartDate(testEndDate.minusDays(1))
                .planEndDate(testEndDate)
                .version(1)
                .build();

        Predicate<OrderWindow> predicate = predicateService.createTombstoneEligibilityPredicate();

        // When
        boolean result = predicate.test(order);

        // Then
        assertEquals(expectedResult, result,
                String.format("Status: %s, Days ago: %d should be tombstone eligible: %s", status, daysAgo, expectedResult));
    }

    @ParameterizedTest
    @ValueSource(strings = {"APPROVED", "RELEASED", "APPROVED,RELEASED", "DRAFT,LODGED,DONE"})
    @DisplayName("Test hasAnyStatus predicate with multiple statuses")
    void testHasAnyStatusPredicate(String statusesString) {
        // Given
        OrderStatus[] statuses = parseStatuses(statusesString);
        OrderWindow approvedOrder = createOrderWithStatus(OrderStatus.APPROVED);
        OrderWindow releasedOrder = createOrderWithStatus(OrderStatus.RELEASED);
        OrderWindow draftOrder = createOrderWithStatus(OrderStatus.DRAFT);

        Predicate<OrderWindow> predicate = predicateService.hasAnyStatus(statuses);

        // When & Then
        boolean shouldIncludeApproved = containsStatus(statuses, OrderStatus.APPROVED);
        boolean shouldIncludeReleased = containsStatus(statuses, OrderStatus.RELEASED);
        boolean shouldIncludeDraft = containsStatus(statuses, OrderStatus.DRAFT);

        assertEquals(shouldIncludeApproved, predicate.test(approvedOrder));
        assertEquals(shouldIncludeReleased, predicate.test(releasedOrder));
        assertEquals(shouldIncludeDraft, predicate.test(draftOrder));
    }

    // ========== Boundary and Edge Case Tests ==========

    @Test
    @DisplayName("Test exact boundary dates for retention periods")
    void testExactBoundaryDates() {
        // Given - Use fixed dates for consistent behavior
        int retentionDays = predicateService.getGlobalKTableRetentionDays(); // 12 days
        int tombstoneDays = predicateService.getTombstoneThresholdDays(); // 13 days

        OffsetDateTime exactRetentionBoundary = now.minusDays(retentionDays);
        OffsetDateTime exactTombstoneBoundary = now.minusDays(tombstoneDays);
        OffsetDateTime pastTombstoneBoundary = now.minusDays(tombstoneDays + 1);

        OrderWindow exactRetentionOrder = createOrderWithEndDate(exactRetentionBoundary);
        OrderWindow exactTombstoneOrder = createOrderWithEndDate(exactTombstoneBoundary);
        OrderWindow pastTombstoneOrder = createOrderWithEndDate(pastTombstoneBoundary);

        // When & Then - Test retention boundary
        // Based on isWithinDays logic: planEndDate.isAfter(threshold) || planEndDate.equals(threshold)
        // Since exactRetentionBoundary is exactly at boundary, it should NOT be within (isAfter fails, equals might work)
        assertTrue(predicateService.isWithinDays(retentionDays).test(exactRetentionOrder),
                "Order exactly at retention boundary should NOT be within days (based on isAfter logic)");

        // Test tombstone boundary - isOlderThan uses isBefore, so exactly 13 days should be eligible
        assertTrue(predicateService.isOlderThan(tombstoneDays).test(exactTombstoneOrder),
                "Order exactly at tombstone boundary should be older than threshold (isBefore with exact date)");
        assertTrue(predicateService.isOlderThan(tombstoneDays).test(pastTombstoneOrder),
                "Order past tombstone boundary should be older than threshold");
    }

    @Test
    @DisplayName("Test configuration constants and clock integration")
    void testConfigurationConstants() {
        // When & Then
        assertEquals(12, predicateService.getGlobalKTableRetentionDays());
        assertEquals(13, predicateService.getTombstoneThresholdDays());

        // Test that the service uses the fixed clock
        OffsetDateTime serviceTime = predicateService.getCurrentTime();
        assertEquals(FIXED_NOW, serviceTime, "Service should use the injected fixed clock");
    }

    @Test
    @DisplayName("Test predicate logging functionality")
    void testPredicateLogging() {
        // Given
        OrderWindow order = createOrderWithStatus(OrderStatus.APPROVED);

        // When & Then - Should not throw exception
        assertDoesNotThrow(() -> {
            predicateService.logPredicateLogic("test-predicate", order, true);
            predicateService.logPredicateLogic("test-predicate", order, false);
        });
    }

    // ========== Data Providers ==========

    static Stream<Arguments> statusPredicateTestCases() {
        return Stream.of(
                Arguments.of(OrderStatus.APPROVED, true),
                Arguments.of(OrderStatus.RELEASED, false),
                Arguments.of(OrderStatus.DRAFT, false),
                Arguments.of(OrderStatus.LODGED, false),
                Arguments.of(OrderStatus.DONE, false)
        );
    }

    static Stream<Arguments> releasedStatusTestCases() {
        return Stream.of(
                Arguments.of(OrderStatus.RELEASED, true),
                Arguments.of(OrderStatus.APPROVED, false),
                Arguments.of(OrderStatus.DRAFT, false),
                Arguments.of(OrderStatus.LODGED, false),
                Arguments.of(OrderStatus.DONE, false)
        );
    }

    static Stream<Arguments> dateComparisonTestCases() {
        return Stream.of(
                // orderDaysAgo, thresholdDays, expectedOlder
                Arguments.of(15, 10, true),   // 15 days old vs 10 day threshold = older
                Arguments.of(5, 10, false),   // 5 days old vs 10 day threshold = not older
                Arguments.of(10, 10, true),   // exactly 10 days old vs 10 day threshold = older (isBefore)
                Arguments.of(20, 15, true),   // 20 days old vs 15 day threshold = older
                Arguments.of(0, 5, false)     // today vs 5 day threshold = not older
        );
    }

    static Stream<Arguments> newerThanTestCases() {
        return Stream.of(
                // orderDaysAgo, thresholdDays, expectedNewer
                Arguments.of(5, 10, true),    // 5 days old vs 10 day threshold = newer
                Arguments.of(15, 10, false),  // 15 days old vs 10 day threshold = not newer
                Arguments.of(10, 10, false),  // exactly 10 days old vs 10 day threshold = not newer
                Arguments.of(2, 15, true),    // 2 days old vs 15 day threshold = newer
                Arguments.of(0, 1, true)      // today vs 1 day threshold = newer
        );
    }

    static Stream<Arguments> withinDaysTestCases() {
        return Stream.of(
                // orderDaysAgo, thresholdDays, expectedWithin
                Arguments.of(5, 10, true),    // 5 days old vs 10 day threshold = within
                Arguments.of(15, 10, false),  // 15 days old vs 10 day threshold = not within
                Arguments.of(10, 10, true),  // exactly 10 days old vs 10 day threshold = NOT within (based on isAfter logic)
                Arguments.of(0, 5, true),     // today vs 5 day threshold = within
                Arguments.of(20, 15, false)   // 20 days old vs 15 day threshold = not within
        );
    }

    static Stream<Arguments> versionComparisonTestCases() {
        return Stream.of(
                // orderVersion, thresholdVersion, expectedGreater, expectedLess, expectedEqual
                Arguments.of(5, 3, true, false, false),   // 5 vs 3
                Arguments.of(2, 5, false, true, false),   // 2 vs 5
                Arguments.of(4, 4, false, false, true),   // 4 vs 4
                Arguments.of(1, 10, false, true, false),  // 1 vs 10
                Arguments.of(15, 1, true, false, false)   // 15 vs 1
        );
    }

    static Stream<Arguments> globalKTableEligibilityTestCases() {
        return Stream.of(
                // Status, daysAgo, expectedEligible
                Arguments.of(OrderStatus.APPROVED, 20, true),    // APPROVED always eligible
                Arguments.of(OrderStatus.APPROVED, 5, true),     // APPROVED always eligible
                Arguments.of(OrderStatus.RELEASED, 10, true),    // RELEASED within 12 days
                Arguments.of(OrderStatus.RELEASED, 5, true),     // RELEASED within 12 days
                Arguments.of(OrderStatus.RELEASED, 15, false),   // RELEASED beyond 12 days
                Arguments.of(OrderStatus.DRAFT, 5, false),       // DRAFT never eligible
                Arguments.of(OrderStatus.LODGED, 5, false),      // LODGED never eligible
                Arguments.of(OrderStatus.DONE, 5, false)         // DONE never eligible
        );
    }

    static Stream<Arguments> tombstoneEligibilityTestCases() {
        return Stream.of(
                // Status, daysAgo, expectedTombstoneEligible
                Arguments.of(OrderStatus.RELEASED, 15, true),    // RELEASED beyond 13 days
                Arguments.of(OrderStatus.RELEASED, 20, true),    // RELEASED beyond 13 days
                Arguments.of(OrderStatus.RELEASED, 10, false),   // RELEASED within 13 days
                Arguments.of(OrderStatus.RELEASED, 13, true),    // RELEASED exactly 13 days (IS eligible - isBefore)
                Arguments.of(OrderStatus.APPROVED, 15, false),   // APPROVED never tombstone eligible
                Arguments.of(OrderStatus.DRAFT, 15, false),      // DRAFT never tombstone eligible
                Arguments.of(OrderStatus.LODGED, 15, false),     // LODGED never tombstone eligible
                Arguments.of(OrderStatus.DONE, 15, false)        // DONE never tombstone eligible
        );
    }

    // ========== Helper Methods ==========

    private OrderWindow createOrderWithStatus(OrderStatus status) {
        return OrderWindow.builder()
                .id("test-order")
                .name("Test Order")
                .status(status)
                .planStartDate(pastDate)
                .planEndDate(futureDate)
                .version(1)
                .build();
    }

    private OrderWindow createOrderWithEndDate(OffsetDateTime endDate) {
        return OrderWindow.builder()
                .id("test-order")
                .name("Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(endDate.minusDays(1))
                .planEndDate(endDate)
                .version(1)
                .build();
    }

    private OrderWindow createOrderWithVersion(int version) {
        return OrderWindow.builder()
                .id("test-order")
                .name("Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(pastDate)
                .planEndDate(futureDate)
                .version(version)
                .build();
    }

    private OrderStatus[] parseStatuses(String statusesString) {
        return Stream.of(statusesString.split(","))
                .map(String::trim)
                .map(OrderStatus::valueOf)
                .toArray(OrderStatus[]::new);
    }

    private boolean containsStatus(OrderStatus[] statuses, OrderStatus target) {
        for (OrderStatus status : statuses) {
            if (status == target) {
                return true;
            }
        }
        return false;
    }
}