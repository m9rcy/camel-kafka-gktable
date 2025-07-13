package com.example.controller;

import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;
import java.util.Set;

@RestController
@RequestMapping("/api/stores")
@RequiredArgsConstructor
public class StoreController {

    private final KafkaStateStoreService kafkaStateStoreService;

    @GetMapping
    public ResponseEntity<Set<String>> getAllStores() {
        try {
            return ResponseEntity.ok(kafkaStateStoreService.getAllStoreNames());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/{storeName}/{id}")
    public ResponseEntity<Object> getStoreNameById(@PathVariable String storeName, @PathVariable String id) {
        try {
            ReadOnlyKeyValueStore<String, Object> store = kafkaStateStoreService.getStore(storeName);
            Object value = store.get(id);
            
            return Optional.ofNullable(value)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }

    @GetMapping("/{storeName}/count")
    public ResponseEntity<Long> getStoreCount(@PathVariable String storeName) {
        try {
            ReadOnlyKeyValueStore<String, Object> store = kafkaStateStoreService.getStore(storeName);
            long count = store.approximateNumEntries();
            
            return ResponseEntity.ok(count);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(null);
        }
    }
}