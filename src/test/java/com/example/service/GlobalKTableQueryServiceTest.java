package com.example.service;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.predicate.OrderWindowPredicates;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GlobalKTableQueryServiceTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, OrderWindow> store;

    @Mock
    private KeyValueIterator<String, OrderWindow> iterator;

    private OrderWindowQueryService queryService;

    @BeforeEach
    void setUp() {
        queryService = new OrderWindowQueryService(kafkaStateStoreService, orderWindowGlobalKTable);
    }

    @Test
    void testQueryWithFilter() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .version(1)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        Predicate<OrderWindow> predicate = OrderWindowPredicates.isApproved();

        // When
        List<String> result = queryService.queryWithFilter(predicate, kv -> kv.key);

        // Then
        assertEquals(1, result.size());
        assertEquals("key1", result.get(0));
        verify(iterator).close();
    }

    @Test
    void testQueryKeys() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        List<String> keys = queryService.queryKeys(OrderWindowPredicates.isApproved());

        // Then
        assertEquals(1, keys.size());
        assertEquals("key1", keys.get(0));
        verify(iterator).close();
    }

    @Test
    void testQueryValues() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        List<OrderWindow> values = queryService.queryValues(OrderWindowPredicates.isApproved());

        // Then
        assertEquals(1, values.size());
        assertEquals("order1", values.get(0).getId());
        assertEquals(OrderStatus.APPROVED, values.get(0).getStatus());
        verify(iterator).close();
    }

    @Test
    void testQueryDeduplicatedValues() {
        // Given
        OrderWindow order1v1 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        OrderWindow order1v2 = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .version(2)
                .build();

        OrderWindow order2v1 = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", order1v1);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", order1v2);
        KeyValue<String, OrderWindow> kv3 = new KeyValue<>("key3", order2v1);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2, kv3);

        // When
        List<OrderWindow> values = queryService.queryDeduplicatedValues(
            OrderWindowPredicates.isApproved()
        );

        // Then
        assertEquals(2, values.size());
        
        // Verify order1 has version 2 (latest)
        OrderWindow order1Result = values.stream()
                .filter(ow -> ow.getId().equals("order1"))
                .findFirst()
                .orElseThrow();
        assertEquals(2, order1Result.getVersion());
        
        // Verify order2 exists
        assertTrue(values.stream().anyMatch(ow -> ow.getId().equals("order2")));
        
        verify(iterator).close();
    }

    @Test
    void testCount() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        long count = queryService.count(OrderWindowPredicates.isApproved());

        // Then
        assertEquals(1, count);
        verify(iterator).close();
    }

    @Test
    void testExists() {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(kv1);

        // When
        boolean exists = queryService.exists(OrderWindowPredicates.isApproved());
        boolean notExists = queryService.exists(OrderWindowPredicates.isReleased());

        // Then
        assertTrue(exists);
        assertFalse(notExists);
        verify(iterator, times(2)).close();
    }

    @Test
    void testGetByKey() {
        // Given
        OrderWindow expectedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.get("key1")).thenReturn(expectedOrder);

        // When
        OrderWindow result = queryService.getByKey("key1");

        // Then
        assertNotNull(result);
        assertEquals("order1", result.getId());
        assertEquals(OrderStatus.APPROVED, result.getStatus());
    }

    @Test
    void testDeduplicateByVersion() {
        // Given
        OrderWindow order1v1 = OrderWindow.builder()
                .id("order1")
                .version(1)
                .build();

        OrderWindow order1v3 = OrderWindow.builder()
                .id("order1")
                .version(3)
                .build();

        OrderWindow order1v2 = OrderWindow.builder()
                .id("order1")
                .version(2)
                .build();

        OrderWindow order2v1 = OrderWindow.builder()
                .id("order2")
                .version(1)
                .build();

        List<OrderWindow> input = List.of(order1v1, order1v3, order1v2, order2v1);

        // When
        List<OrderWindow> result = queryService.deduplicateByVersion(input);

        // Then
        assertEquals(2, result.size());
        
        OrderWindow order1Result = result.stream()
                .filter(ow -> ow.getId().equals("order1"))
                .findFirst()
                .orElseThrow();
        assertEquals(3, order1Result.getVersion());
        
        OrderWindow order2Result = result.stream()
                .filter(ow -> ow.getId().equals("order2"))
                .findFirst()
                .orElseThrow();
        assertEquals(1, order2Result.getVersion());
    }

    @Test
    void testOrderWindowPredicates() {
        // Given
        OffsetDateTime testDate = OffsetDateTime.now();
        OffsetDateTime pastDate = testDate.minusDays(5);
        OffsetDateTime futureDate = testDate.plusDays(5);

        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .name("Test Order")
                .version(2)
                .planEndDate(pastDate)
                .planStartDate(pastDate)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .name("Another Order")
                .version(1)
                .planEndDate(futureDate)
                .planStartDate(futureDate)
                .build();

        // Test status predicates
        assertTrue(OrderWindowPredicates.isApproved().test(approvedOrder));
        assertFalse(OrderWindowPredicates.isApproved().test(releasedOrder));
        
        assertTrue(OrderWindowPredicates.isReleased().test(releasedOrder));
        assertFalse(OrderWindowPredicates.isReleased().test(approvedOrder));

        assertTrue(OrderWindowPredicates.hasStatus(OrderStatus.APPROVED).test(approvedOrder));
        assertTrue(OrderWindowPredicates.hasStatus(OrderStatus.RELEASED).test(releasedOrder));

        // Test date predicates
        assertTrue(OrderWindowPredicates.endDateBefore(testDate).test(approvedOrder));
        assertFalse(OrderWindowPredicates.endDateBefore(testDate).test(releasedOrder));

        assertTrue(OrderWindowPredicates.endDateAfter(testDate).test(releasedOrder));
        assertFalse(OrderWindowPredicates.endDateAfter(testDate).test(approvedOrder));

        assertTrue(OrderWindowPredicates.endDateBeforeOrEqual(testDate).test(approvedOrder));
        assertTrue(OrderWindowPredicates.endDateAfterOrEqual(testDate).test(releasedOrder));

        // Test ID and name predicates
        assertTrue(OrderWindowPredicates.hasId("order1").test(approvedOrder));
        assertFalse(OrderWindowPredicates.hasId("order1").test(releasedOrder));

        assertTrue(OrderWindowPredicates.hasName("Test Order").test(approvedOrder));
        assertFalse(OrderWindowPredicates.hasName("Test Order").test(releasedOrder));

        assertTrue(OrderWindowPredicates.nameContains("test").test(approvedOrder));
        assertTrue(OrderWindowPredicates.nameContains("another").test(releasedOrder));
        assertFalse(OrderWindowPredicates.nameContains("nonexistent").test(approvedOrder));

        // Test version predicates
        assertTrue(OrderWindowPredicates.hasVersion(2).test(approvedOrder));
        assertFalse(OrderWindowPredicates.hasVersion(2).test(releasedOrder));

        assertTrue(OrderWindowPredicates.versionGreaterThan(1).test(approvedOrder));
        assertFalse(OrderWindowPredicates.versionGreaterThan(1).test(releasedOrder));

        assertTrue(OrderWindowPredicates.versionLessThan(3).test(approvedOrder));
        assertTrue(OrderWindowPredicates.versionLessThan(3).test(releasedOrder));

        // Test created date predicates (if createdDate is set)
        if (approvedOrder.getPlanStartDate() != null) {
            assertTrue(OrderWindowPredicates.createdBefore(testDate).test(approvedOrder));
            assertFalse(OrderWindowPredicates.createdBefore(testDate).test(releasedOrder));
        }

        // Test tombstone eligibility
        OrderWindow oldReleasedOrder = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(15))
                .build();

        assertTrue(OrderWindowPredicates.isTombstoneEligible(13).test(oldReleasedOrder));
        assertFalse(OrderWindowPredicates.isTombstoneEligible(13).test(approvedOrder));
        assertFalse(OrderWindowPredicates.isTombstoneEligible(13).test(releasedOrder));
    }

    @Test
    void testPredicateCombination() {
        // Given
        OrderWindow approvedOldOrder = OrderWindow.builder()
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().minusDays(5))
                .build();

        OrderWindow releasedOldOrder = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(5))
                .build();

        OrderWindow approvedNewOrder = OrderWindow.builder()
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().plusDays(5))
                .build();

        // Test AND combination
        Predicate<OrderWindow> approvedAndOld = OrderWindowPredicates
                .and(
                    OrderWindowPredicates.isApproved(),
                    OrderWindowPredicates.endDateBefore(OffsetDateTime.now())
                );

        assertTrue(approvedAndOld.test(approvedOldOrder));
        assertFalse(approvedAndOld.test(releasedOldOrder));
        assertFalse(approvedAndOld.test(approvedNewOrder));

        // Test OR combination
        Predicate<OrderWindow> approvedOrReleased = OrderWindowPredicates
                .or(
                    OrderWindowPredicates.isApproved(),
                    OrderWindowPredicates.isReleased()
                );

        assertTrue(approvedOrReleased.test(approvedOldOrder));
        assertTrue(approvedOrReleased.test(releasedOldOrder));
        assertTrue(approvedOrReleased.test(approvedNewOrder));

        // Test NOT combination
        Predicate<OrderWindow> notApproved = OrderWindowPredicates
                .not(OrderWindowPredicates.isApproved());

        assertFalse(notApproved.test(approvedOldOrder));
        assertTrue(notApproved.test(releasedOldOrder));
        assertFalse(notApproved.test(approvedNewOrder));
    }

    @Test
    void testQueryWithNullValues() {
        // Given
        OrderWindow validOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", validOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", null);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        List<OrderWindow> values = queryService.queryValues(
            OrderWindowPredicates.isApproved()
        );

        // Then
        assertEquals(1, values.size());
        assertEquals("order1", values.get(0).getId());
        verify(iterator).close();
    }
}