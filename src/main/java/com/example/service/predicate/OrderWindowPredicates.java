package com.example.service.predicate;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Predefined predicates for OrderWindow filtering scenarios.
 */
@Service
@Slf4j
public class OrderWindowPredicates {
    
    public static Predicate<OrderWindow> hasStatus(OrderStatus status) {
        return orderWindow -> orderWindow.getStatus() == status;
    }
    
    public static Predicate<OrderWindow> endDateBefore(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isBefore(date);
    }
    
    public static Predicate<OrderWindow> endDateAfter(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isAfter(date);
    }
    
    public static Predicate<OrderWindow> endDateBeforeOrEqual(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isBefore(date) || 
                             orderWindow.getPlanEndDate().equals(date);
    }
    
    public static Predicate<OrderWindow> endDateAfterOrEqual(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanEndDate().isAfter(date) || 
                             orderWindow.getPlanEndDate().equals(date);
    }
    
    public static Predicate<OrderWindow> isReleased() {
        return hasStatus(OrderStatus.RELEASED);
    }
    
    public static Predicate<OrderWindow> isApproved() {
        return hasStatus(OrderStatus.APPROVED);
    }
    
    public static Predicate<OrderWindow> isDraft() {
        return hasStatus(OrderStatus.DRAFT);
    }
    
    public static Predicate<OrderWindow> hasId(String id) {
        return orderWindow -> Objects.equals(orderWindow.getId(), id);
    }
    
    public static Predicate<OrderWindow> hasName(String name) {
        return orderWindow -> Objects.equals(orderWindow.getName(), name);
    }
    
    public static Predicate<OrderWindow> nameContains(String substring) {
        return orderWindow -> orderWindow.getName() != null && 
                             orderWindow.getName().toLowerCase().contains(substring.toLowerCase());
    }
    
    public static Predicate<OrderWindow> hasVersion(int version) {
        return orderWindow -> orderWindow.getVersion() == version;
    }
    
    public static Predicate<OrderWindow> versionGreaterThan(int version) {
        return orderWindow -> orderWindow.getVersion() > version;
    }
    
    public static Predicate<OrderWindow> versionLessThan(int version) {
        return orderWindow -> orderWindow.getVersion() < version;
    }
    
    public static Predicate<OrderWindow> createdAfter(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanStartDate() != null &&
                             orderWindow.getPlanStartDate().isAfter(date);
    }
    
    public static Predicate<OrderWindow> createdBefore(OffsetDateTime date) {
        return orderWindow -> orderWindow.getPlanStartDate() != null &&
                             orderWindow.getPlanStartDate().isBefore(date);
    }
    
    public static Predicate<OrderWindow> isTombstoneEligible(int thresholdDays) {
        OffsetDateTime cutoffDate = OffsetDateTime.now().minusDays(thresholdDays);
        return isReleased().and(endDateBefore(cutoffDate));
    }
    
    // Utility methods for combining predicates
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }
    
    public static <T> Predicate<T> and(Predicate<T> first, Predicate<T> second) {
        return first.and(second);
    }
    
    public static <T> Predicate<T> or(Predicate<T> first, Predicate<T> second) {
        return first.or(second);
    }
}