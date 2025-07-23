package com.example.controller.v2;

import com.example.service.GlobalKTableRegistry;
import com.example.service.KafkaStateStoreService;
import com.example.service.v2.KafkaStateStoreServiceV2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping(value = "/api/stores", produces = "application/json")
@RequiredArgsConstructor
@Slf4j
public class StoreController {

    private final KafkaStateStoreServiceV2 kafkaStateStoreService;
    private final Map<String, GlobalKTable<?, ?>> globalKTableLookup;

    @GetMapping
    public ResponseEntity<Object> getAllStores() {
        try {
            Set<String> storeNames = globalKTableLookup.keySet();
            log.info("Retrieved {} store names", storeNames.size());
            return ResponseEntity.ok(Map.of("stores",storeNames));
        } catch (Exception e) {
            log.error("Error retrieving store names", e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/{storeName}/{id}")
    public ResponseEntity<Object> getStoreNameById(@PathVariable String storeName, @PathVariable String id) {
        try {
            ReadOnlyKeyValueStore<String, Object> store = getStore(storeName);

            if (store == null) {
                return ResponseEntity.notFound().build();
            }

            Object value = store.get(id);
            return Optional.ofNullable(value)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            log.error("Error retrieving value for store: {} and id: {}", storeName, id, e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/{storeName}/count")
    public ResponseEntity<Object> getStoreCount(@PathVariable String storeName) {
        try {
            ReadOnlyKeyValueStore<String, Object> store = getStore(storeName);

            if (store == null) {
                return ResponseEntity.notFound().build();
            }

            long count = store.approximateNumEntries();
            return ResponseEntity.ok(Map.of("approximateCount", count));
        } catch (Exception e) {
            log.error("Error retrieving count for store: {}", storeName, e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @SuppressWarnings("unchecked")
    private ReadOnlyKeyValueStore<String, Object> getStore(String storeName) {
        GlobalKTable<String, Object> globalKTable = (GlobalKTable<String, Object>) globalKTableLookup.get(storeName);

        if (globalKTable == null) {
            log.warn("Store {} is not found", storeName);
            return null;
        }

        return kafkaStateStoreService.getStoreFor(globalKTable);
    }

}