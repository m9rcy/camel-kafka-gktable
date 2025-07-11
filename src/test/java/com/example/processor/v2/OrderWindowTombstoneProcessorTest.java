package com.example.processor.v2;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.processor.OrderWindowTombstoneProcessor;
import com.example.service.GlobalKTableQueryService;
import com.example.service.OrderWindowQueryService;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderWindowTombstoneProcessorTest {

    @Mock
    private OrderWindowQueryService globalKTableQueryService;

    private OrderWindowTombstoneFinalProcessor processor;
    private Exchange exchange;

    @BeforeEach
    void setUp() {
        processor = new OrderWindowTombstoneFinalProcessor(globalKTableQueryService);
        exchange = new DefaultExchange(new DefaultCamelContext());
    }

    @Test
    void testProcessWithTombstoneEligibleKeys() throws Exception {
        // Given
        List<String> tombstoneKeys = Arrays.asList("key1", "key2", "key3");

        when(globalKTableQueryService.queryKeys(any(Predicate.class))).thenReturn(tombstoneKeys);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(3, result.size());
        assertEquals(3, exchange.getMessage().getHeader("tombstoneCount"));

        // Verify the correct predicate is used
        verify(globalKTableQueryService).queryKeys(argThat(predicate -> {
            // Test that the predicate correctly identifies tombstone eligible orders
            OrderWindow eligibleOrder = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(15))
                .build();
            
            OrderWindow notEligibleOrder = OrderWindow.builder()
                .status(OrderStatus.APPROVED)
                .planEndDate(OffsetDateTime.now().minusDays(15))
                .build();
            
            return predicate.test(eligibleOrder) && !predicate.test(notEligibleOrder);
        }));
    }

    @Test
    void testProcessWithNoTombstoneEligibleKeys() throws Exception {
        // Given
        when(globalKTableQueryService.queryKeys(any(Predicate.class))).thenReturn(Collections.emptyList());

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<String> result = exchange.getMessage().getBody(List.class);
        assertEquals(0, result.size());
        assertEquals(0, exchange.getMessage().getHeader("tombstoneCount"));

        verify(globalKTableQueryService).queryKeys(any(Predicate.class));
    }

    @Test
    void testProcessWithCorrectThresholdDays() throws Exception {
        // Given
        List<String> tombstoneKeys = Arrays.asList("key1");
        when(globalKTableQueryService.queryKeys(any(Predicate.class))).thenReturn(tombstoneKeys);

        // When
        processor.process(exchange);

        // Then
        verify(globalKTableQueryService).queryKeys(argThat(predicate -> {
            // Test that exactly 13 days is used as threshold
            OrderWindow exactlyThirteenDays = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(13))
                .build();
            
            OrderWindow fourteenDaysAgo = OrderWindow.builder()
                .status(OrderStatus.RELEASED)
                .planEndDate(OffsetDateTime.now().minusDays(14))
                .build();
            
            // Should not include exactly 13 days, but should include 14 days
            return !predicate.test(exactlyThirteenDays) && predicate.test(fourteenDaysAgo);
        }));
    }

    @Test
    void testProcessHandlesServiceException() {
        // Given
        when(globalKTableQueryService.queryKeys(any(Predicate.class)))
            .thenThrow(new RuntimeException("Service error"));

        // When & Then
        assertThrows(RuntimeException.class, () -> processor.process(exchange));
        verify(globalKTableQueryService).queryKeys(any(Predicate.class));
    }
}