package com.example.controller;

import com.example.service.GlobalKTableRegistry;
import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;
import java.util.Set;

@RestController
@RequestMapping(value = "/api/stores", produces = "application/json")
@RequiredArgsConstructor
@Slf4j
public class StoreController {

    private final KafkaStateStoreService kafkaStateStoreService;
    private final GlobalKTableRegistry globalKTableRegistry;

    @GetMapping
    public ResponseEntity<Set<String>> getAllStores() {
        try {
            Set<String> storeNames = kafkaStateStoreService.getAllStoreNames();
            log.info("Retrieved {} store names", storeNames.size());
            return ResponseEntity.ok(storeNames);
        } catch (Exception e) {
            log.error("Error retrieving store names", e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/{storeName}/{id}")
    public ResponseEntity<Object> getStoreNameById(@PathVariable String storeName, @PathVariable String id) {
        try {
            // Check if store is registered
            if (!globalKTableRegistry.isRegistered(storeName)) {
                log.warn("Store {} is not registered", storeName);
                return ResponseEntity.notFound().build();
            }

            ReadOnlyKeyValueStore<String, Object> store = kafkaStateStoreService.getStore(storeName);
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
            if (!globalKTableRegistry.isRegistered(storeName)) {
                log.warn("Store {} is not registered", storeName);
                return ResponseEntity.notFound().build();
            }

            ReadOnlyKeyValueStore<String, Object> store = kafkaStateStoreService.getStore(storeName);
            long count = store.approximateNumEntries();

            return ResponseEntity.ok(count);
        } catch (Exception e) {
            log.error("Error retrieving count for store: {}", storeName, e);
            return ResponseEntity.status(500).body(null);
        }
    }
}