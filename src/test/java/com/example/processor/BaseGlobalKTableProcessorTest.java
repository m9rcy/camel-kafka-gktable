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
class BaseGlobalKTableProcessorTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, OrderWindow> store;

    @Mock
    private KeyValueIterator<String, OrderWindow> iterator;

    private Exchange exchange;

    // Test implementation of BaseGlobalKTableProcessor
    private static class TestProcessor extends BaseGlobalKTableProcessor<String> {

        public TestProcessor(KafkaStateStoreService kafkaStateStoreService,
                           GlobalKTable<String, OrderWindow> orderWindowGlobalKTable) {
            super(kafkaStateStoreService, orderWindowGlobalKTable);
        }

        @Override
        protected boolean shouldIncludeEntry(KeyValue<String, OrderWindow> entry) {
            // Only include APPROVED orders for testing
            return entry.value.getStatus() == OrderStatus.APPROVED;
        }

        @Override
        protected String transformEntry(KeyValue<String, OrderWindow> entry) {
            // Transform to order ID
            return entry.value.getId();
        }

        @Override
        protected String getCountHeaderName() {
            return "testCount";
        }
    }

    private TestProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new TestProcessor(kafkaStateStoreService, orderWindowGlobalKTable);
        exchange = new DefaultExchange(new DefaultCamelContext());
    }

    @Test
    void testProcessWithFilteringAndTransformation() throws Exception {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .id("order2")
                .status(OrderStatus.RELEASED)
                .build();

        OrderWindow draftOrder = OrderWindow.builder()
                .id("order3")
                .status(OrderStatus.DRAFT)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", releasedOrder);
        KeyValue<String, OrderWindow> kv3 = new KeyValue<>("key3", draftOrder);

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
        assertEquals("order1", result.get(0)); // Only approved order should be included
        assertEquals(1, exchange.getMessage().getHeader("testCount"));

        verify(iterator).close();
    }

    @Test
    void testProcessWithNullValues() throws Exception {
        // Given
        OrderWindow approvedOrder = OrderWindow.builder()
                .id("order1")
                .status(OrderStatus.APPROVED)
                .build();

        KeyValue<String, OrderWindow> kv1 = new KeyValue<>("key1", approvedOrder);
        KeyValue<String, OrderWindow> kv2 = new KeyValue<>("key2", null);

        when(kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable)).thenReturn(store);
        when(store.all()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(kv1, kv2);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(1, result.size());
        assertEquals("order1", result.get(0));
        assertEquals(1, exchange.getMessage().getHeader("testCount"));

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
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(0, result.size());
        assertEquals(0, exchange.getMessage().getHeader("testCount"));

        verify(iterator).close();
    }

    @Test
    void testOrderWindowPredicates() {
        // Given
        OffsetDateTime testDate = OffsetDateTime.now().minusDays(5);
        
        OrderWindow approvedOrder = OrderWindow.builder()
                .status(OrderStatus.APPROVED)
                .planEndDate(testDate.minusDays(1))
                .build();

        OrderWindow releasedOrder = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(testDate.plusDays(1))
                .build();

        // When & Then
        assertTrue(BaseGlobalKTableProcessor.OrderWindowPredicates
                .isApproved().test(approvedOrder));
        assertFalse(BaseGlobalKTableProcessor.OrderWindowPredicates
                .isApproved().test(releasedOrder));

        assertTrue(BaseGlobalKTableProcessor.OrderWindowPredicates
                .isReleased().test(releasedOrder));
        assertFalse(BaseGlobalKTableProcessor.OrderWindowPredicates
                .isReleased().test(approvedOrder));

        assertTrue(BaseGlobalKTableProcessor.OrderWindowPredicates
                .endDateBefore(testDate).test(approvedOrder));
        assertFalse(BaseGlobalKTableProcessor.OrderWindowPredicates
                .endDateBefore(testDate).test(releasedOrder));

        assertTrue(BaseGlobalKTableProcessor.OrderWindowPredicates
                .endDateAfter(testDate).test(releasedOrder));
        assertFalse(BaseGlobalKTableProcessor.OrderWindowPredicates
                .endDateAfter(testDate).test(approvedOrder));
    }
}