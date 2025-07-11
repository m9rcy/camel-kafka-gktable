package com.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Generic service for querying GlobalKTable data.
 * Provides reusable methods for filtering, transforming, and aggregating data from any GlobalKTable.
 *
 * @param <K> The key type of the GlobalKTable
 * @param <V> The value type of the GlobalKTable
 */
@RequiredArgsConstructor
@Slf4j
public class GlobalKTableQueryService<K, V> {

    private final KafkaStateStoreService kafkaStateStoreService;
    private final GlobalKTable<K, V> globalKTable;

    /**
     * Query GlobalKTable with a predicate filter and transformation function.
     *
     * @param predicate Filter to apply to entries
     * @param transformer Function to transform matching entries
     * @param <T> The type of result produced by the transformer
     * @return List of transformed results
     */
    public <T> List<T> queryWithFilter(Predicate<V> predicate, Function<KeyValue<K, V>, T> transformer) {
        ReadOnlyKeyValueStore<K, V> store = kafkaStateStoreService.getStoreFor(globalKTable);
        List<T> results = new ArrayList<>();

        log.debug("Querying GlobalKTable with predicate filter");

        try (KeyValueIterator<K, V> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<K, V> entry = iterator.next();

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
     * @param predicate Filter to apply to entries
     * @return List of keys for matching entries
     */
    public List<K> queryKeys(Predicate<V> predicate) {
        return queryWithFilter(predicate, kv -> kv.key);
    }

    /**
     * Query GlobalKTable and return values matching the predicate.
     *
     * @param predicate Filter to apply to entries
     * @return List of values for matching entries
     */
    public List<V> queryValues(Predicate<V> predicate) {
        return queryWithFilter(predicate, kv -> kv.value);
    }

    /**
     * Query GlobalKTable and return all values (no filtering).
     *
     * @return List of all values
     */
    public List<V> queryAllValues() {
        return queryValues(value -> true);
    }

    /**
     * Count entries matching the predicate.
     *
     * @param predicate Filter to apply to entries
     * @return Count of matching entries
     */
    public long count(Predicate<V> predicate) {
        ReadOnlyKeyValueStore<K, V> store = kafkaStateStoreService.getStoreFor(globalKTable);
        long count = 0;

        try (KeyValueIterator<K, V> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<K, V> entry = iterator.next();
                if (entry.value != null && predicate.test(entry.value)) {
                    count++;
                }
            }
        }

        log.debug("Found {} matching records for count", count);
        return count;
    }

    /**
     * Check if any entry matches the predicate.
     *
     * @param predicate Filter to apply to entries
     * @return true if any entry matches, false otherwise
     */
    public boolean exists(Predicate<V> predicate) {
        ReadOnlyKeyValueStore<K, V> store = kafkaStateStoreService.getStoreFor(globalKTable);

        try (KeyValueIterator<K, V> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<K, V> entry = iterator.next();
                if (entry.value != null && predicate.test(entry.value)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Get a specific value by key.
     *
     * @param key The key to lookup
     * @return The value or null if not found
     */
    public V getByKey(K key) {
        ReadOnlyKeyValueStore<K, V> store = kafkaStateStoreService.getStoreFor(globalKTable);
        return store.get(key);
    }
}