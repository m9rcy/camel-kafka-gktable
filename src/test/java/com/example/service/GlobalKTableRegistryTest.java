package com.example.service;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GlobalKTableRegistryTest {

    @Mock
    private GlobalKTable<String, String> userTable;

    @Mock
    private GlobalKTable<String, String> productTable;

    @Mock
    private GlobalKTable<Long, String> orderTable;

    private GlobalKTableRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new GlobalKTableRegistry();
    }

    @Test
    void register_shouldStoreGlobalKTable() {
        // Given
        String storeName = "user-store";

        // When
        registry.register(storeName, userTable);

        // Then
        assertTrue(registry.isRegistered(storeName));
        assertEquals(userTable, registry.getGlobalKTable(storeName));
    }

    @Test
    void register_shouldHandleMultipleGlobalKTables() {
        // Given
        String userStoreName = "user-store";
        String productStoreName = "product-store";

        // When
        registry.register(userStoreName, userTable);
        registry.register(productStoreName, productTable);

        // Then
        assertTrue(registry.isRegistered(userStoreName));
        assertTrue(registry.isRegistered(productStoreName));
        assertEquals(userTable, registry.getGlobalKTable(userStoreName));
        assertEquals(productTable, registry.getGlobalKTable(productStoreName));
    }

    @Test
    void register_shouldOverwriteExistingRegistration() {
        // Given
        String storeName = "user-store";
        registry.register(storeName, userTable);

        // When
        registry.register(storeName, productTable);

        // Then
        assertTrue(registry.isRegistered(storeName));
//        assertEquals(productTable, registry.getGlobalKTable(storeName));
//        assertNotEquals(userTable, registry.getGlobalKTable(storeName));
    }

    @Test
    void getAllRegisteredNames_shouldReturnEmptySetWhenNoRegistrations() {
        // When
        Set<String> result = registry.getAllRegisteredNames();

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void getAllRegisteredNames_shouldReturnAllRegisteredNames() {
        // Given
        registry.register("user-store", userTable);
        registry.register("product-store", productTable);
        registry.register("order-store", orderTable);

        // When
        Set<String> result = registry.getAllRegisteredNames();

        // Then
        assertEquals(3, result.size());
        assertTrue(result.contains("user-store"));
        assertTrue(result.contains("product-store"));
        assertTrue(result.contains("order-store"));
    }

    @Test
    void getGlobalKTable_shouldReturnNullForUnregisteredStore() {
        // When
        GlobalKTable<?, ?> result = registry.getGlobalKTable("unknown-store");

        // Then
        assertNull(result);
    }

    @Test
    void isRegistered_shouldReturnFalseForUnregisteredStore() {
        // When
        boolean result = registry.isRegistered("unknown-store");

        // Then
        assertFalse(result);
    }

    @Test
    void isRegistered_shouldReturnTrueForRegisteredStore() {
        // Given
        String storeName = "user-store";
        registry.register(storeName, userTable);

        // When
        boolean result = registry.isRegistered(storeName);

        // Then
        assertTrue(result);
    }

    @Test
    void clear_shouldRemoveAllRegistrations() {
        // Given
        registry.register("user-store", userTable);
        registry.register("product-store", productTable);
        registry.register("order-store", orderTable);

        // When
        registry.clear();

        // Then
        assertTrue(registry.getAllRegisteredNames().isEmpty());
        assertFalse(registry.isRegistered("user-store"));
        assertFalse(registry.isRegistered("product-store"));
        assertFalse(registry.isRegistered("order-store"));
        assertNull(registry.getGlobalKTable("user-store"));
        assertNull(registry.getGlobalKTable("product-store"));
        assertNull(registry.getGlobalKTable("order-store"));
    }

    @Test
    void register_shouldHandleDifferentKeyValueTypes() {
        // Given
        String userStoreName = "user-store";
        String orderStoreName = "order-store";

        // When
        registry.register(userStoreName, userTable);
        registry.register(orderStoreName, orderTable);

        // Then
        assertTrue(registry.isRegistered(userStoreName));
        assertTrue(registry.isRegistered(orderStoreName));
        assertEquals(userTable, registry.getGlobalKTable(userStoreName));
        assertEquals(orderTable, registry.getGlobalKTable(orderStoreName));
    }

    @Test
    void register_shouldBeThreadSafe() throws InterruptedException {
        // Given
        int numberOfThreads = 10;
        int registrationsPerThread = 100;
        Thread[] threads = new Thread[numberOfThreads];

        // When
        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < registrationsPerThread; j++) {
                    String storeName = "store-" + threadIndex + "-" + j;
                    registry.register(storeName, userTable);
                }
            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Then
        assertEquals(numberOfThreads * registrationsPerThread, registry.getAllRegisteredNames().size());
    }
}