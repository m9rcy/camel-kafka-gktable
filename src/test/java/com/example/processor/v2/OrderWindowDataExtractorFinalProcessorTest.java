package com.example.processor.v2;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

// ==================== OrderWindowDataExtractorProcessorTest ====================

@ExtendWith(MockitoExtension.class)
class OrderWindowDataExtractorFinalProcessorTest {

    @Mock
    private OrderWindowQueryService globalKTableQueryService;

    private OrderWindowDataExtractorFinalProcessor processor;
    private Exchange exchange;

    @BeforeEach
    void setUp() {
        processor = new OrderWindowDataExtractorFinalProcessor(globalKTableQueryService);
        exchange = new DefaultExchange(new DefaultCamelContext());
    }

    @Test
    void testProcessWithDeduplicatedData() throws Exception {
        // Given
        List<OrderWindow> mockDeduplicatedData = Arrays.asList(
            OrderWindow.builder()
                .id("order1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .version(2) // Latest version
                .build(),
            OrderWindow.builder()
                .id("order2")
                .name("Order 2")
                .status(OrderStatus.RELEASED)
                .version(1)
                .build()
        );

        when(globalKTableQueryService.queryAllDeduplicatedValues()).thenReturn(mockDeduplicatedData);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(2, result.size());
        assertEquals(2, exchange.getMessage().getHeader("dataExtractCount"));

        verify(globalKTableQueryService).queryAllDeduplicatedValues();
    }

    @Test
    void testProcessWithEmptyData() throws Exception {
        // Given
        when(globalKTableQueryService.queryAllDeduplicatedValues()).thenReturn(Collections.emptyList());

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(0, result.size());
        assertEquals(0, exchange.getMessage().getHeader("dataExtractCount"));

        verify(globalKTableQueryService).queryAllDeduplicatedValues();
    }

    @Test
    void testProcessWithLargeDataset() throws Exception {
        // Given
        List<OrderWindow> largeDataset = Arrays.asList(
            OrderWindow.builder().id("order1").status(OrderStatus.APPROVED).version(1).build(),
            OrderWindow.builder().id("order2").status(OrderStatus.RELEASED).version(1).build(),
            OrderWindow.builder().id("order3").status(OrderStatus.APPROVED).version(1).build()
        );

        when(globalKTableQueryService.queryAllDeduplicatedValues()).thenReturn(largeDataset);

        // When
        processor.process(exchange);

        // Then
        @SuppressWarnings("unchecked")
        List<OrderWindow> result = exchange.getMessage().getBody(List.class);
        assertEquals(3, result.size());
        assertEquals(3, exchange.getMessage().getHeader("dataExtractCount"));

        verify(globalKTableQueryService).queryAllDeduplicatedValues();
    }

    @Test
    void testProcessHandlesServiceException() {
        // Given
        when(globalKTableQueryService.queryAllDeduplicatedValues())
            .thenThrow(new RuntimeException("Service error"));

        // When & Then
        assertThrows(RuntimeException.class, () -> processor.process(exchange));
        verify(globalKTableQueryService).queryAllDeduplicatedValues();
    }
}