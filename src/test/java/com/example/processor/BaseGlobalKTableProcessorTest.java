package com.example.processor;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import com.example.service.OrderWindowPredicateService;
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
import java.util.function.Predicate;

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

    @Mock
    private OrderWindowPredicateService predicateService;

    private Exchange exchange;

    // Test implementation of BaseGlobalKTableProcessor
    private static class TestProcessor extends BaseGlobalKTableProcessor<String> {

        private final OrderWindowPredicateService predicateService;

        public TestProcessor(KafkaStateStoreService kafkaStateStoreService,
                             GlobalKTable<String, OrderWindow> orderWindowGlobalKTable,
                             OrderWindowPredicateService predicateService) {
            super(kafkaStateStoreService, orderWindowGlobalKTable, predicateService);
            this.predicateService = predicateService;
        }

        @Override
        protected Predicate<OrderWindow> getFilterPredicate() {
            // Only include APPROVED orders for testing
            return predicateService.isApproved();
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
        processor = new TestProcessor(kafkaStateStoreService, orderWindowGlobalKTable, predicateService);
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

        // Mock the predicate service to return APPROVED predicate
        when(predicateService.isApproved()).thenReturn(order -> order.getStatus() == OrderStatus.APPROVED);

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
        verify(predicateService).isApproved();
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

        // Mock the predicate service
        when(predicateService.isApproved()).thenReturn(order -> order.getStatus() == OrderStatus.APPROVED);

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
        when(predicateService.isApproved()).thenReturn(order -> order.getStatus() == OrderStatus.APPROVED);
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
    void testOrderWindowPredicatesFromService() {
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

        // Mock the predicate service methods
        when(predicateService.isApproved()).thenReturn(order -> order.getStatus() == OrderStatus.APPROVED);
        when(predicateService.isReleased()).thenReturn(order -> order.getStatus() == OrderStatus.RELEASED);
        when(predicateService.endDateBefore(testDate)).thenReturn(order -> order.getPlanEndDate().isBefore(testDate));
        when(predicateService.endDateAfter(testDate)).thenReturn(order -> order.getPlanEndDate().isAfter(testDate));

        // When & Then
        assertTrue(predicateService.isApproved().test(approvedOrder));
        assertFalse(predicateService.isApproved().test(releasedOrder));

        assertTrue(predicateService.isReleased().test(releasedOrder));
        assertFalse(predicateService.isReleased().test(approvedOrder));

        assertTrue(predicateService.endDateBefore(testDate).test(approvedOrder));
        assertFalse(predicateService.endDateBefore(testDate).test(releasedOrder));

        assertTrue(predicateService.endDateAfter(testDate).test(releasedOrder));
        assertFalse(predicateService.endDateAfter(testDate).test(approvedOrder));
    }

    @Test
    void testPredicateServiceIntegration() throws Exception {
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

        // Mock the predicate service to return an actual predicate
        Predicate<OrderWindow> approvedPredicate = order -> order.getStatus() == OrderStatus.APPROVED;
        when(predicateService.isApproved()).thenReturn(approvedPredicate);

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

        verify(predicateService).isApproved();
        verify(iterator).close();
    }
}