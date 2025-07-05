package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

public class TestDataFactory {
    
    public static OrderWindow createOrderWindow(String id, OrderStatus status, int version) {
        return OrderWindow.builder()
                .id(id)
                .name("Test Order " + id)
                .status(status)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(1))
                .version(version)
                .build();
    }
    
    public static OrderWindow createOrderWindowWithEndDate(String id, OrderStatus status, OffsetDateTime endDate) {
        return OrderWindow.builder()
                .id(id)
                .name("Test Order " + id)
                .status(status)
                .planStartDate(endDate.minusDays(1))
                .planEndDate(endDate)
                .version(1)
                .build();
    }
    
    public static OrderWindow createTombstoneEligibleOrder(String id) {
        return createOrderWindowWithEndDate(id, OrderStatus.RELEASED, OffsetDateTime.now().minusDays(14));
    }
    
    public static OrderWindow createNonTombstoneEligibleOrder(String id) {
        return createOrderWindowWithEndDate(id, OrderStatus.RELEASED, OffsetDateTime.now().minusDays(10));
    }
    
    public static List<OrderWindow> createMultipleVersionsOfSameOrder(String id) {
        return Arrays.asList(
                OrderWindow.builder()
                        .id(id)
                        .name("Test Order " + id + " v1")
                        .status(OrderStatus.DRAFT)
                        .version(1)
                        .build(),
                OrderWindow.builder()
                        .id(id)
                        .name("Test Order " + id + " v2")
                        .status(OrderStatus.APPROVED)
                        .version(2)
                        .build(),
                OrderWindow.builder()
                        .id(id)
                        .name("Test Order " + id + " v3")
                        .status(OrderStatus.RELEASED)
                        .version(3)
                        .build()
        );
    }
    
    public static List<OrderWindow> createMixedStatusOrders() {
        return Arrays.asList(
                createOrderWindow("order1", OrderStatus.APPROVED, 1),
                createOrderWindow("order2", OrderStatus.RELEASED, 1),
                createOrderWindow("order3", OrderStatus.DRAFT, 1),
                createOrderWindow("order4", OrderStatus.LODGED, 1),
                createOrderWindow("order5", OrderStatus.DONE, 1)
        );
    }
}