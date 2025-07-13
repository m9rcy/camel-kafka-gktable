package com.example.controller;

import com.example.service.EKafkaStateStoreService;
import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class KafkaStateStoreController {

    private final EKafkaStateStoreService kafkaStateStoreService;

    @GetMapping("/stores")
    public ResponseEntity<Set<String>> getAvailableStores() {
        try {
            Set<String> storeNames = kafkaStateStoreService.getAvailableStoreNames();
            return ResponseEntity.ok(storeNames);
        } catch (Exception e) {
            log.error("Error retrieving available stores", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/{storeName}/{id}")
    public ResponseEntity<Object> getStoreValue(
            @PathVariable String storeName,
            @PathVariable String id) {
        try {
            Object value = kafkaStateStoreService.getStoreValue(storeName, id);
            if (value == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(value);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid store name: {}", storeName);
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving value from store {} for id {}", storeName, id, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/{storeName}/status")
    public ResponseEntity<Map<String, Object>> getStoreStatus(
            @PathVariable String storeName) {
        try {
            Map<String, Object> status = kafkaStateStoreService.getStoreStatus(storeName);
            return ResponseEntity.ok(status);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid store name: {}", storeName);
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving status for store {}", storeName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}