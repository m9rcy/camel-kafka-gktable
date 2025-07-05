package com.example.service;

import com.example.model.GlobalKTableMetrics;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GlobalKTableMetricsServiceTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, OrderWindow> store;

    @Mock
    private KeyValueIterator<String, OrderWindow> iterator;

    private MeterRegistry meterRegistry;
    private GlobalKTableMetricsService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new GlobalKTableMetricsService(kafkaStateStoreService, orderWindowGlobalKTable, meterRegistry);
    }

    @Test
    void testCalculateMetrics() {
        // Given
        OffsetDateTime fourteenDaysAgo = OffsetDateTime.now().minusDays(14);
        OffsetDateTime tenDaysAgo = OffsetDateTime.now().minusDays(10);

        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow releasedRecentOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow releasedOldOrder = OrderWindow.builder()
                .id("order3")
                .status(OrderStatus.RELEASED)
                .planEndDate(fourteenDaysAgo)
                .build();

        OrderWindow duplicateOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .planEndDate(tenDaysAgo)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedRecentOrder);
        KeyValue<String, OrderWindow> kv3 = new KeyValue<>("key3", releasedOldOrder);
        KeyValue<String, OrderWindow> kv4 = new KeyValue<>("key4", duplicateOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2, kv3, kv4);

        // When
        GlobalKTableMetrics metrics = service.calculateMetrics();

        // Then
        assertEquals(4, metrics.getTotalRecords());
        assertEquals(1, metrics.getDuplicateKeys()); // order1 appears twice
        assertEquals(1, metrics.getTombstoneEligible()); // only releasedOldOrder is eligible
        assertEquals(2, metrics.getApprovedStatus());
        assertEquals(2, metrics.getReleasedStatus());
        assertNotNull(metrics.getLastUpdated());

        verify(iterator).close();
    }

    @Test
    void testCalculateMetricsWithNullValues() {
        // Given
        OrderWindow validOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().minusDays(10))
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", validOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", null);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        GlobalKTableMetrics metrics = service.calculateMetrics();

        // Then
        assertEquals(1, metrics.getTotalRecords());
        assertEquals(0, metrics.getDuplicateKeys());
        assertEquals(0, metrics.getTombstoneEligible());
        assertEquals(1, metrics.getApprovedStatus());
        assertEquals(0, metrics.getReleasedStatus());

        verify(iterator).close();
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

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", order1v1);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", order1v2);
        KeyValue<String, OrderWindow> kv3 = new KeyValue<>("key3", order2v1);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2, kv3);

        // When
        Map<String, java.util.List<OrderWindow>> duplicates = service.getDetailedDuplicateKeys();

        // Then
        assertEquals(1, duplicates.size());
        assertTrue(duplicates.containsKey("order1"));
        assertEquals(2, duplicates.get("order1").size());

        verify(iterator).close();
    }
}