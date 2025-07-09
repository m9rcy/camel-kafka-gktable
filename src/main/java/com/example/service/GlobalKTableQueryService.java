package com.example.service;

import com.example.model.OrderWindow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Centralized service for querying GlobalKTable data.
 * Provides reusable methods for filtering, transforming, and aggregating OrderWindow data.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class GlobalKTableQueryService {

    private final KafkaStateStoreService kafkaStateStoreService;
    private final GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    /**
     * Query GlobalKTable with a predicate filter and transformation function.
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @param transformer Function to transform matching entries
     * @param <T> The type of result produced by the transformer
     * @return List of transformed results
     */
    public <T> List<T> queryWithFilter(Predicate<OrderWindow> predicate, Function<KeyValue<String, OrderWindow>, T> transformer) {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        List<T> results = new ArrayList<>();
        
        log.debug("Querying GlobalKTable with predicate filter");
        
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                
                // Skip null values and apply predicate
                if (entry.value != null && predicate.test(entry.value)) {
                    T result = transformer.apply(entry);
                    if (result != null) {
                        results.add(result);
                    }
                }
            }
        }
        
        log.debug("Found {} matching records", results.size());
        return results;
    }

    /**
     * Query GlobalKTable and return keys matching the predicate.
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return List of keys for matching entries
     */
    public List<String> queryKeys(Predicate<OrderWindow> predicate) {
        return queryWithFilter(predicate, kv -> kv.key);
    }

    /**
     * Query GlobalKTable and return OrderWindow values matching the predicate.
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return List of OrderWindow values for matching entries
     */
    public List<OrderWindow> queryValues(Predicate<OrderWindow> predicate) {
        return queryWithFilter(predicate, kv -> kv.value);
    }

    /**
     * Query GlobalKTable and return deduplicated OrderWindow values (latest version per ID).
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return List of deduplicated OrderWindow values
     */
    public List<OrderWindow> queryDeduplicatedValues(Predicate<OrderWindow> predicate) {
        List<OrderWindow> values = queryValues(predicate);
        return deduplicateByVersion(values);
    }

    /**
     * Query GlobalKTable and return all OrderWindow values (no filtering).
     * 
     * @return List of all OrderWindow values
     */
    public List<OrderWindow> queryAllValues() {
        return queryValues(orderWindow -> true);
    }

    /**
     * Query GlobalKTable and return all deduplicated OrderWindow values (latest version per ID).
     * 
     * @return List of all deduplicated OrderWindow values
     */
    public List<OrderWindow> queryAllDeduplicatedValues() {
        return queryDeduplicatedValues(orderWindow -> true);
    }

    /**
     * Count entries matching the predicate.
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return Count of matching entries
     */
    public long count(Predicate<OrderWindow> predicate) {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        long count = 0;
        
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                if (entry.value != null && predicate.test(entry.value)) {
                    count++;
                }
            }
        }
        
        log.debug("Found {} matching records for count", count);
        return count;
    }

    /**
     * Count deduplicated entries matching the predicate (counts unique IDs only).
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return Count of unique IDs matching the predicate
     */
    public long countDeduplicated(Predicate<OrderWindow> predicate) {
        List<OrderWindow> values = queryValues(predicate);
        return deduplicateByVersion(values).size();
    }

    /**
     * Check if any entry matches the predicate.
     * 
     * @param predicate Filter to apply to OrderWindow entries
     * @return true if any entry matches, false otherwise
     */
    public boolean exists(Predicate<OrderWindow> predicate) {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                if (entry.value != null && predicate.test(entry.value)) {
                    return true;
                }
            }
        }
        
        return false;
    }

    /**
     * Get a specific OrderWindow by key.
     * 
     * @param key The key to lookup
     * @return The OrderWindow value or null if not found
     */
    public OrderWindow getByKey(String key) {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        return store.get(key);
    }

    /**
     * Deduplicate OrderWindow list by keeping the highest version for each ID.
     * 
     * @param orderWindows List of OrderWindow objects to deduplicate
     * @return Deduplicated list with highest version per ID
     */
    public List<OrderWindow> deduplicateByVersion(List<OrderWindow> orderWindows) {
        Map<String, OrderWindow> latestVersions = orderWindows.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        OrderWindow::getId,
                        Function.identity(),
                        (existing, replacement) ->
                                existing.getVersion() > replacement.getVersion() ? existing : replacement
                ));

        return new ArrayList<>(latestVersions.values());
    }

    /**
     * Predefined predicates for common OrderWindow filtering scenarios.
     */
    public static class OrderWindowPredicates {
        
        public static Predicate<OrderWindow> hasStatus(com.example.model.OrderStatus status) {
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
            return hasStatus(com.example.model.OrderStatus.RELEASED);
        }
        
        public static Predicate<OrderWindow> isApproved() {
            return hasStatus(com.example.model.OrderStatus.APPROVED);
        }
        
        public static Predicate<OrderWindow> isDraft() {
            return hasStatus(com.example.model.OrderStatus.DRAFT);
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
}