package com.example.service;

import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GlobalKTableMetricsServiceTest {

    @Mock
    private GlobalKTableQueryService globalKTableQueryService;

    private MeterRegistry meterRegistry;
    private GlobalKTableMetricsService metricsService;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metricsService = new GlobalKTableMetricsService(globalKTableQueryService, meterRegistry);
    }

    @Test
    void testCalculateMetrics() {
        // Given
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime fourteenDaysAgo = now.minusDays(14);
        OffsetDateTime tenDaysAgo = now.minusDays(10);

        // Multiple versions of order1 to test duplicates
        OrderWindow order1v1 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow order1v2 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(2)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow order2v1 = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .version(1)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow order3v1 = OrderWindow.builder()
                .id("order3")
                .status(OrderStatus.RELEASED)
                .version(1)
                .planEndDate(fourteenDaysAgo) // Tombstone eligible
                .build();

        List<OrderWindow> allValues = Arrays.asList(order1v1, order1v2, order2v1, order3v1);

        // Mock the query service calls
        when(globalKTableQueryService.queryAllValues()).thenReturn(allValues);

        // Mock countDeduplicated calls - first for APPROVED, then for RELEASED
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class)))
                .thenAnswer(invocation -> {
                    Predicate<OrderWindow> predicate = invocation.getArgument(0);
                    // Filter by predicate, then deduplicate by ID
                    return allValues.stream()
                            .filter(predicate)
                            .map(OrderWindow::getId)
                            .distinct()
                            .count();
                });

        // Mock count call for tombstone eligible
        when(globalKTableQueryService.count(any(Predicate.class)))
                .thenAnswer(invocation -> {
                    Predicate<OrderWindow> predicate = invocation.getArgument(0);
                    return allValues.stream().filter(predicate).count();
                });

        // When
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();

        // Then
        assertEquals(4, metrics.getTotalRecords()); // All records including duplicates
        assertEquals(1, metrics.getDuplicateKeys()); // Only order1 has duplicates
        assertEquals(1, metrics.getTombstoneEligible()); // Only order3 is tombstone eligible
        assertEquals(1, metrics.getApprovedStatus()); // Only order1 (deduplicated)
        assertEquals(2, metrics.getReleasedStatus()); // order2 and order3 (deduplicated)
        assertNotNull(metrics.getLastUpdated());

        // Verify the query service was called correctly
        verify(globalKTableQueryService).queryAllValues();
        verify(globalKTableQueryService, times(2)).countDeduplicated(any(Predicate.class)); // Called for APPROVED and RELEASED
        verify(globalKTableQueryService).count(any(Predicate.class)); // Called for tombstone eligible
    }

    @Test
    void testCalculateMetricsWithEmptyStore() {
        // Given
        when(globalKTableQueryService.queryAllValues()).thenReturn(List.of());
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class))).thenReturn(0L);
        when(globalKTableQueryService.count(any(Predicate.class))).thenReturn(0L);

        // When
        GlobalKTableMetrics metrics = metricsService.calculateMetrics();

        // Then
        assertEquals(0, metrics.getTotalRecords());
        assertEquals(0, metrics.getDuplicateKeys());
        assertEquals(0, metrics.getTombstoneEligible());
        assertEquals(0, metrics.getApprovedStatus());
        assertEquals(0, metrics.getReleasedStatus());
        assertNotNull(metrics.getLastUpdated());
    }

    @Test
    void testGetDetailedDuplicateKeys() {
        // Given
        OrderWindow order1v1 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        OrderWindow order1v2 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.RELEASED)
                .version(2)
                .build();

        OrderWindow order2v1 = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        List<OrderWindow> allValues = Arrays.asList(order1v1, order1v2, order2v1);
        when(globalKTableQueryService.queryAllValues()).thenReturn(allValues);

        // When
        Map<String, List<OrderWindow>> duplicates = metricsService.getDetailedDuplicateKeys();

        // Then
        assertEquals(1, duplicates.size());
        assertTrue(duplicates.containsKey("order1"));
        assertEquals(2, duplicates.get("order1").size());
        assertFalse(duplicates.containsKey("order2")); // order2 has only one version
    }

    @Test
    void testGetTombstoneEligibleOrders() {
        // Given
        OrderWindow eligibleOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(15))
                .build();

        List<OrderWindow> tombstoneEligible = List.of(eligibleOrder);
        when(globalKTableQueryService.queryValues(any(Predicate.class))).thenReturn(tombstoneEligible);

        // When
        List<OrderWindow> result = metricsService.getTombstoneEligibleOrders();

        // Then
        assertEquals(1, result.size());
        assertEquals("order1", result.get(0).getId());
        assertEquals(OrderStatus.RELEASED, result.get(0).getStatus());
        verify(globalKTableQueryService).queryValues(any(Predicate.class));
    }

    @Test
    void testGetOrdersByStatus() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        List<OrderWindow> approvedOrders = List.of(approvedOrder);
        when(globalKTableQueryService.queryDeduplicatedValues(any(Predicate.class))).thenReturn(approvedOrders);
        when(globalKTableQueryService.queryValues(any(Predicate.class))).thenReturn(approvedOrders);

        // When - with deduplication
        List<OrderWindow> deduplicatedResult = metricsService.getOrdersByStatus(OrderStatus.APPROVED, true);

        // Then
        assertEquals(1, deduplicatedResult.size());
        assertEquals(OrderStatus.APPROVED, deduplicatedResult.get(0).getStatus());
        verify(globalKTableQueryService).queryDeduplicatedValues(any(Predicate.class));

        // When - without deduplication
        List<OrderWindow> nonDeduplicatedResult = metricsService.getOrdersByStatus(OrderStatus.APPROVED, false);

        // Then
        assertEquals(1, nonDeduplicatedResult.size());
        assertEquals(OrderStatus.APPROVED, nonDeduplicatedResult.get(0).getStatus());
        verify(globalKTableQueryService).queryValues(any(Predicate.class));
    }

    @Test
    void testGetOrdersEndingWithinDays() {
        // Given
        OrderWindow endingSoonOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().plusDays(3))
                .build();

        List<OrderWindow> endingSoonOrders = List.of(endingSoonOrder);
        when(globalKTableQueryService.queryDeduplicatedValues(any(Predicate.class))).thenReturn(endingSoonOrders);

        // When
        List<OrderWindow> result = metricsService.getOrdersEndingWithinDays(7, true);

        // Then
        assertEquals(1, result.size());
        assertEquals("order1", result.get(0).getId());
        verify(globalKTableQueryService).queryDeduplicatedValues(any(Predicate.class));
    }

    @Test
    void testGetOverdueOrders() {
        // Given
        OrderWindow overdueOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().minusDays(5))
                .build();

        List<OrderWindow> overdueOrders = List.of(overdueOrder);
        when(globalKTableQueryService.queryDeduplicatedValues(any(Predicate.class))).thenReturn(overdueOrders);

        // When
        List<OrderWindow> result = metricsService.getOverdueOrders(true);

        // Then
        assertEquals(1, result.size());
        assertEquals("order1", result.get(0).getId());
        assertTrue(result.get(0).getPlanEndDate().isBefore(OffsetDateTime.now()));
        verify(globalKTableQueryService).queryDeduplicatedValues(any(Predicate.class));
    }

    @Test
    void testHasOrdersWithStatus() {
        // Given
        when(globalKTableQueryService.exists(any(Predicate.class))).thenReturn(true);

        // When
        boolean hasApprovedOrders = metricsService.hasOrdersWithStatus(OrderStatus.APPROVED);

        // Then
        assertTrue(hasApprovedOrders);
        verify(globalKTableQueryService).exists(any(Predicate.class));
    }

    @Test
    void testGetOrderCountByStatus() {
        // Given
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class))).thenReturn(5L);

        // When
        long count = metricsService.getOrderCountByStatus(OrderStatus.APPROVED);

        // Then
        assertEquals(5L, count);
        verify(globalKTableQueryService).countDeduplicated(any(Predicate.class));
    }

    @Test
    void testGetAdvancedMetrics() {
        // Given
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class))).thenReturn(10L);

        // When
        AdvancedGlobalKTableMetrics advanced = metricsService.getAdvancedMetrics();

        // Then
        assertEquals(10L, advanced.getOverdueOrders());
        assertEquals(10L, advanced.getEndingThisWeek());
        assertEquals(10L, advanced.getActiveApprovedOrders());
        assertEquals(10L, advanced.getActiveReleasedOrders());
        assertNotNull(advanced.getCalculatedAt());

        // Verify multiple calls were made for different predicates
        verify(globalKTableQueryService, times(4)).countDeduplicated(any(Predicate.class));
    }

    @Test
    void testCalculateMetricsHandlesException() {
        // Given
        when(globalKTableQueryService.queryAllValues()).thenThrow(new RuntimeException("Test exception"));

        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            metricsService.calculateMetrics();
        });

        assertEquals("Failed to calculate GlobalKTable metrics", exception.getMessage());
        assertEquals("Test exception", exception.getCause().getMessage());
    }

    @Test
    void testGetDetailedDuplicateKeysHandlesException() {
        // Given
        when(globalKTableQueryService.queryAllValues()).thenThrow(new RuntimeException("Test exception"));

        // When
        Map<String, List<OrderWindow>> result = metricsService.getDetailedDuplicateKeys();

        // Then
        assertTrue(result.isEmpty());
        verify(globalKTableQueryService).queryAllValues();
    }

    @Test
    void testMicrometerGauges() {
        // Given
        List<OrderWindow> testOrders = Arrays.asList(
                OrderWindow.builder().id("order1").status(OrderStatus.APPROVED).version(1).build(),
                OrderWindow.builder().id("order2").status(OrderStatus.RELEASED).version(1).build()
        );

        when(globalKTableQueryService.queryAllValues()).thenReturn(testOrders);
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class))).thenReturn(1L);
        when(globalKTableQueryService.count(any(Predicate.class))).thenReturn(0L);

        // When
        metricsService.calculateMetrics();

        // Then - verify gauge values are updated
        assertEquals(2.0, metricsService.getTotalRecordsCount());
        assertEquals(0.0, metricsService.getDuplicateKeysCount()); // No duplicates
        assertEquals(0.0, metricsService.getTombstoneEligibleCount());
        assertEquals(1.0, metricsService.getApprovedStatusCount());
        assertEquals(1.0, metricsService.getReleasedStatusCount());
    }

    @Test
    void testPredicateUsageInMetrics() {
        // This test verifies that the correct predicates are being used
        // Given
        when(globalKTableQueryService.queryAllValues()).thenReturn(List.of());
        when(globalKTableQueryService.countDeduplicated(any(Predicate.class))).thenReturn(0L);
        when(globalKTableQueryService.count(any(Predicate.class))).thenReturn(0L);

        // When
        metricsService.calculateMetrics();

        // Then - verify the correct predicates are being used
        // First call should be for APPROVED status
        verify(globalKTableQueryService).countDeduplicated(argThat(predicate -> {
            // Test that the APPROVED predicate works correctly
            OrderWindow approvedOrder = OrderWindow.builder()
                    .status(OrderStatus.APPROVED)
                    .build();
            OrderWindow releasedOrder = OrderWindow.builder()
                    .status(OrderStatus.RELEASED)
                    .build();
            return predicate.test(approvedOrder) && !predicate.test(releasedOrder);
        }));

        // Second call should be for RELEASED status
        verify(globalKTableQueryService).countDeduplicated(argThat(predicate -> {
            // Test that the RELEASED predicate works correctly
            OrderWindow releasedOrder = OrderWindow.builder()
                    .status(OrderStatus.RELEASED)
                    .build();
            OrderWindow approvedOrder = OrderWindow.builder()
                    .status(OrderStatus.APPROVED)
                    .build();
            return predicate.test(releasedOrder) && !predicate.test(approvedOrder);
        }));

        // Third call should be for tombstone eligible
        verify(globalKTableQueryService).count(argThat(predicate -> {
            // Test that the tombstone eligible predicate works correctly
            OrderWindow tombstoneEligibleOrder = OrderWindow.builder()
                    .status(OrderStatus.RELEASED)
                    .planEndDate(OffsetDateTime.now().minusDays(15))
                    .build();
            OrderWindow notEligibleOrder = OrderWindow.builder()
                    .status(OrderStatus.APPROVED)
                    .planEndDate(OffsetDateTime.now().minusDays(15))
                    .build();
            return predicate.test(tombstoneEligibleOrder) && !predicate.test(notEligibleOrder);
        }));
    }
}