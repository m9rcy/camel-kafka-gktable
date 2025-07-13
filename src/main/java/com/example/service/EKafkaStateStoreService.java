package com.example.service;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class EKafkaStateStoreService {
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final Map<String, String> storeNameRegistry = new ConcurrentHashMap<>();
    
    // Auto-register known stores
    @PostConstruct
    public void registerKnownStores() {
        storeNameRegistry.put("order-window-global-store", "OrderWindow");
        storeNameRegistry.put("code-global-store", "Code");
    }

    /**
     * Register a new store name with its type for dynamic discovery
     */
    public void registerStore(String storeName, String storeType) {
        storeNameRegistry.put(storeName, storeType);
    }

    /**
     * Get all available store names
     */
    public Set<String> getAvailableStoreNames() {
        return new HashSet<>(storeNameRegistry.keySet());
    }

    public <K, V> ReadOnlyKeyValueStore<K, V> getStoreFor(GlobalKTable<K, V> globalKTable) {
        return getStore(globalKTable.queryableStoreName());
    }

    public <V, K> ReadOnlyKeyValueStore<K, V> getStore(String storeName) {
        if (!storeNameRegistry.containsKey(storeName)) {
            throw new IllegalArgumentException("Store not found: " + storeName);
        }
        
        QueryableStoreType<ReadOnlyKeyValueStore<K, V>> storeType = QueryableStoreTypes.keyValueStore();
        return getKafkaStream().store(StoreQueryParameters.fromNameAndType(storeName, storeType));
    }

    /**
     * Get value from store by id
     */
    public Object getStoreValue(String storeName, String id) {
        ReadOnlyKeyValueStore<String, Object> store = getStore(storeName);
        return store.get(id);
    }

    /**
     * Get store status including count and custom statistics
     */
    public Map<String, Object> getStoreStatus(String storeName) {
        ReadOnlyKeyValueStore<String, Object> store = getStore(storeName);
        Map<String, Object> status = new HashMap<>();
        
        // Basic count
        long approximateCount = store.approximateNumEntries();
        status.put("approximateCount", approximateCount);
        status.put("storeName", storeName);
        status.put("storeType", storeNameRegistry.get(storeName));
        
        // Custom statistics based on store type
        if ("OrderWindow".equals(storeNameRegistry.get(storeName))) {
            addOrderWindowStatistics(store, status);
        } else if ("Code".equals(storeNameRegistry.get(storeName))) {
            addCodeStatistics(store, status);
        }
        
        return status;
    }

    private void addOrderWindowStatistics(ReadOnlyKeyValueStore<String, Object> store, Map<String, Object> status) {
        try {
            Map<OrderStatus, Long> statusCount = new HashMap<>();
            
            // Initialize counters
            for (OrderStatus orderStatus : OrderStatus.values()) {
                statusCount.put(orderStatus, 0L);
            }
            
            // Count by status
            try (KeyValueIterator<String, Object> iterator = store.all()) {
                while (iterator.hasNext()) {
                    var keyValue = iterator.next();
                    if (keyValue.value instanceof OrderWindow) {
                        OrderWindow orderWindow = (OrderWindow) keyValue.value;
                        OrderStatus currentStatus = orderWindow.getStatus();
                        statusCount.put(currentStatus, statusCount.get(currentStatus) + 1);
                    }
                }
            }
            
            status.put("statusBreakdown", statusCount);
            
            // Additional statistics
            long activeOrders = statusCount.get(OrderStatus.APPROVED) + 
                               statusCount.get(OrderStatus.LODGED) + 
                               statusCount.get(OrderStatus.RELEASED);
            status.put("activeOrdersCount", activeOrders);
            
        } catch (Exception e) {
            log.warn("Error calculating OrderWindow statistics for store {}", store, e);
            status.put("statisticsError", "Unable to calculate detailed statistics");
        }
    }

    private void addCodeStatistics(ReadOnlyKeyValueStore<String, Object> store, Map<String, Object> status) {
        try {
            // For Code store, we could add statistics like:
            // - Count of codes with descriptions vs without
            // - Average description length, etc.
            
            long codesWithDescription = 0;
            long codesWithoutDescription = 0;
            
            try (KeyValueIterator<String, Object> iterator = store.all()) {
                while (iterator.hasNext()) {
                    var keyValue = iterator.next();
                    if (keyValue.value instanceof com.example.model.Code) {
                        com.example.model.Code code = (com.example.model.Code) keyValue.value;
                        if (code.getDescription() != null && !code.getDescription().trim().isEmpty()) {
                            codesWithDescription++;
                        } else {
                            codesWithoutDescription++;
                        }
                    }
                }
            }
            
            status.put("codesWithDescription", codesWithDescription);
            status.put("codesWithoutDescription", codesWithoutDescription);
            
        } catch (Exception e) {
            log.warn("Error calculating Code statistics for store {}", store, e);
            status.put("statisticsError", "Unable to calculate detailed statistics");
        }
    }

    private KafkaStreams getKafkaStream() {
        return Optional.ofNullable(streamsBuilderFactoryBean.getKafkaStreams())
                .orElseThrow(() -> new IllegalStateException("Kafka Streams not available yet"));
    }
}