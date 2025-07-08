package com.example.processor;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderWindowTombstoneOldProcessorTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, OrderWindow> store;

    @Mock
    private KeyValueIterator<String, OrderWindow> iterator;

    private OrderWindowTombstoneOldProcessor processor;
    private Exchange exchange;

    @BeforeEach
    void setUp() {
        processor = new OrderWindowTombstoneOldProcessor(kafkaStateStoreService, orderWindowGlobalKTable);
        exchange = new DefaultExchange(new DefaultCamelContext());
    }

    @Test
    void testProcessWithTombstoneEligibleRecords() throws Exception {
        // Given
        OffsetDateTime fourteenDaysAgo = OffsetDateTime.now().minusDays(14);
        OffsetDateTime tenDaysAgo = OffsetDateTime.now().minusDays(10);

        OrderWindow eligibleOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.RELEASED)
                .planEndDate(fourteenDaysAgo)
                .build();

        OrderWindow notEligibleOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .planEndDate(tenDaysAgo)
                .build();

        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order3")
                .status(OrderStatus.APPROVED)
                .planEndDate(fourteenDaysAgo)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", eligibleOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", notEligibleOrder);
        KeyValue<String, OrderWindow> kv3 = new KeyValue<>("key3", approvedOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2, kv3);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(1, result.size());
        assertEquals("key1", result.get(0));
        assertEquals(1, exchange.getMessage().getHeader("tombstoneCount"));

        verify(iterator).close();
    }

    @Test
    void testProcessWithNoTombstoneEligibleRecords() throws Exception {
        // Given
        OffsetDateTime tenDaysAgo = OffsetDateTime.now().minusDays(10);

        OrderWindow notEligibleOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.RELEASED)
                .planEndDate(tenDaysAgo)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", notEligibleOrder);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(kv1);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(0, result.size());
        assertEquals(0, exchange.getMessage().getHeader("tombstoneCount"));

        verify(iterator).close();
    }
}