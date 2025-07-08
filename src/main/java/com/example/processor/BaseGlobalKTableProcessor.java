package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Base processor for operations on GlobalKTable data.
 * Provides common functionality for iterating through GlobalKTable entries
 * and applying filters and transformations.
 *
 * @param <T> The type of result produced by the processor
 */
@RequiredArgsConstructor
@Slf4j
public abstract class BaseGlobalKTableProcessor<T> implements Processor {

    protected final KafkaStateStoreService kafkaStateStoreService;
    protected final GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Override
    public void process(Exchange exchange) throws Exception {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        List<T> results = new ArrayList<>();
        
        log.debug("Starting {} process", this.getClass().getSimpleName());
        
        // Iterate through all key-value pairs in the store
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                
                // Skip null values (tombstones in store are handled by Kafka)
                if (entry.value != null && shouldIncludeEntry(entry)) {
                    T result = transformEntry(entry);
                    if (result != null) {
                        results.add(result);
                    }
                }
            }
        }
        
        // Apply post-processing (e.g., deduplication, sorting)
        List<T> processedResults = postProcess(results);
        
        log.info("Found {} records to process", processedResults.size());
        exchange.getMessage().setBody(processedResults);
        exchange.getMessage().setHeader(getCountHeaderName(), processedResults.size());
    }

    /**
     * Determines whether a GlobalKTable entry should be included in processing.
     * Subclasses should implement this to define their filtering logic.
     *
     * @param entry The key-value entry from the GlobalKTable
     * @return true if the entry should be processed, false otherwise
     */
    protected abstract boolean shouldIncludeEntry(KeyValue<String, OrderWindow> entry);

    /**
     * Transforms a GlobalKTable entry into the desired result type.
     * Subclasses should implement this to define their transformation logic.
     *
     * @param entry The key-value entry from the GlobalKTable
     * @return The transformed result, or null if the entry should be skipped
     */
    protected abstract T transformEntry(KeyValue<String, OrderWindow> entry);

    /**
     * Performs post-processing on the collected results.
     * Default implementation returns the results as-is.
     * Subclasses can override this for additional processing like deduplication.
     *
     * @param results The raw results from transformation
     * @return The post-processed results
     */
    protected List<T> postProcess(List<T> results) {
        return results;
    }

    /**
     * Returns the name of the header to set with the count of processed items.
     * Subclasses should implement this to provide the appropriate header name.
     *
     * @return The header name for the count
     */
    protected abstract String getCountHeaderName();

    /**
     * Convenience method for creating predicates based on OrderWindow properties.
     * Can be used by subclasses to build complex filtering logic.
     */
    protected static class OrderWindowPredicates {
        
        public static Predicate<OrderWindow> hasStatus(com.example.model.OrderStatus status) {
            return orderWindow -> orderWindow.getStatus() == status;
        }
        
        public static Predicate<OrderWindow> endDateBefore(java.time.OffsetDateTime date) {
            return orderWindow -> orderWindow.getPlanEndDate().isBefore(date);
        }
        
        public static Predicate<OrderWindow> endDateAfter(java.time.OffsetDateTime date) {
            return orderWindow -> orderWindow.getPlanEndDate().isAfter(date);
        }
        
        public static Predicate<OrderWindow> endDateBeforeOrEqual(java.time.OffsetDateTime date) {
            return orderWindow -> orderWindow.getPlanEndDate().isBefore(date) || 
                                 orderWindow.getPlanEndDate().equals(date);
        }
        
        public static Predicate<OrderWindow> isReleased() {
            return hasStatus(com.example.model.OrderStatus.RELEASED);
        }
        
        public static Predicate<OrderWindow> isApproved() {
            return hasStatus(com.example.model.OrderStatus.APPROVED);
        }
    }
}