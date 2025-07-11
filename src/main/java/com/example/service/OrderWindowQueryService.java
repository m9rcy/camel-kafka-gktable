package com.example.service;

import com.example.model.OrderWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Specialized OrderWindow query service with deduplication capabilities.
 */
@Service
@Slf4j
public class OrderWindowQueryService extends GlobalKTableQueryService<String, OrderWindow> {

    public OrderWindowQueryService(KafkaStateStoreService kafkaStateStoreService, 
                                  GlobalKTable<String, OrderWindow> orderWindowGlobalKTable) {
        super(kafkaStateStoreService, orderWindowGlobalKTable);
    }

    /**
     * Query GlobalKTable and return deduplicated OrderWindow values (latest version per ID).
     * 
     * @param predicate Filter to apply to entries
     * @return List of deduplicated OrderWindow values
     */
    public List<OrderWindow> queryDeduplicatedValues(Predicate<OrderWindow> predicate) {
        List<com.example.model.OrderWindow> values = queryValues(predicate);
        return deduplicateByVersion(values);
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
     * Count deduplicated entries matching the predicate (counts unique IDs only).
     * 
     * @param predicate Filter to apply to entries
     * @return Count of unique IDs matching the predicate
     */
    public long countDeduplicated(Predicate<OrderWindow> predicate) {
        List<OrderWindow> values = queryValues(predicate);
        return deduplicateByVersion(values).size();
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
}