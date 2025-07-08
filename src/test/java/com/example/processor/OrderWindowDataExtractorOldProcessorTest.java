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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderWindowDataExtractorOldProcessorTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, OrderWindow> store;

    @Mock
    private KeyValueIterator<String, OrderWindow> iterator;

    private OrderWindowDataExtractorOldProcessor processor;
    private Exchange exchange;

    @BeforeEach
    void setUp() {
        processor = new OrderWindowDataExtractorOldProcessor(kafkaStateStoreService, orderWindowGlobalKTable);
        exchange = new DefaultExchange(new DefaultCamelContext());
    }

    @Test
    void testProcessWithMultipleVersions() throws Exception {
        // Given
        OrderWindow order1v1 = OrderWindow.builder()
                .id("order1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        OrderWindow order1v2 = OrderWindow.builder()
                .id("order1")
                .name("Order 1 Updated")
                .status(OrderStatus.RELEASED)
                .version(2)
                .build();

        OrderWindow order2v1 = OrderWindow.builder()
                .id("order2")
                .name("Order 2")
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
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(2, result.size());
        assertEquals(2, exchange.getMessage().getHeader("dataExtractCount"));

        // Verify latest versions are selected
        assertTrue(result.stream().anyMatch(ow -> ow.getId().equals("order1") && ow.getVersion() == 2));
        assertTrue(result.stream().anyMatch(ow -> ow.getId().equals("order2") && ow.getVersion() == 1));

        verify(iterator).close();
    }

    @Test
    void testProcessWithNullValues() throws Exception {
        // Given
        OrderWindow order1 = OrderWindow.builder()
                .id("order1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .version(1)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", order1);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", null);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(1, result.size());
        assertEquals("order1", result.get(0).getId());
        assertEquals(1, exchange.getMessage().getHeader("dataExtractCount"));

        verify(iterator).close();
    }

    @Test
    void testProcessWithEmptyStore() throws Exception {
        // Given
        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(0, result.size());
        assertEquals(0, exchange.getMessage().getHeader("dataExtractCount"));

        verify(iterator).close();
    }
}