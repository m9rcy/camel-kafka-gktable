package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import com.example.service.OrderWindowPredicateService;
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
import java.util.function.Predicate;

/**
 * Base processor for operations on GlobalKTable data using reusable predicates.
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
    protected final OrderWindowPredicateService predicateService;

    @Override
    public void process(Exchange exchange) throws Exception {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        List<T> results = new ArrayList<>();
        
        log.debug("Starting {} process", this.getClass().getSimpleName());
        
        // Get the predicate for this processor
        Predicate<OrderWindow> filterPredicate = getFilterPredicate();
        
        // Iterate through all key-value pairs in the store
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                
                // Skip null values (tombstones in store are handled by Kafka)
                if (entry.value != null && filterPredicate.test(entry.value)) {
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
     * Returns the predicate to use for filtering records.
     * Subclasses should implement this to define their filtering logic using
     * the predicateService.
     *
     * @return The predicate to use for filtering
     */
    protected abstract Predicate<OrderWindow> getFilterPredicate();

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
     * Logs predicate evaluation for debugging
     */
    protected void logPredicateEvaluation(String predicateName, OrderWindow orderWindow, boolean result) {
        predicateService.logPredicateLogic(predicateName, orderWindow, result);
    }
}