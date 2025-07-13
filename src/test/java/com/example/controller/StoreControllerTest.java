package com.example.controller;

import com.example.service.KafkaStateStoreService;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StoreControllerTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private ReadOnlyKeyValueStore<String, Object> keyValueStore;

    @InjectMocks
    private StoreController storeController;

    @Test
    @SuppressWarnings("unchecked")
    void getAllStores_shouldReturnStoreNames() {
        Set<String> expectedStores = Collections.singleton("test-store");
        when(kafkaStateStoreService.getAllStoreNames()).thenReturn(expectedStores);

        ResponseEntity<Set<String>> response = storeController.getAllStores();

        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedStores, response.getBody());
    }

    @Test
    void getAllStores_shouldReturn500OnError() {
        when(kafkaStateStoreService.getAllStoreNames()).thenThrow(new RuntimeException());

        ResponseEntity<Set<String>> response = storeController.getAllStores();

        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getStoreNameById_shouldReturnValueWhenFound() {
        String storeName = "test-store";
        String id = "123";
        Object expectedValue = "test-value";
        when(kafkaStateStoreService.<Object, String>getStore(anyString())).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(expectedValue);

        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedValue, response.getBody());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getStoreNameById_shouldReturn404WhenNotFound() {
        String storeName = "test-store";
        String id = "123";
        when(kafkaStateStoreService.<Object, String>getStore(anyString())).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(null);

        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        assertEquals(404, response.getStatusCodeValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getStoreNameById_shouldReturn500OnError() {
        String storeName = "test-store";
        String id = "123";
        when(kafkaStateStoreService.<Object, String>getStore(anyString())).thenThrow(new RuntimeException());

        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getStoreCount_shouldReturnCount() {
        String storeName = "test-store";
        long expectedCount = 42L;
        when(kafkaStateStoreService.<Object, String>getStore(anyString())).thenReturn(keyValueStore);
        when(keyValueStore.approximateNumEntries()).thenReturn(expectedCount);

        ResponseEntity<Long> response = storeController.getStoreCount(storeName);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedCount, response.getBody());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getStoreCount_shouldReturn500OnError() {
        String storeName = "test-store";
        when(kafkaStateStoreService.<Object, String>getStore(anyString())).thenThrow(new RuntimeException());

        ResponseEntity<Long> response = storeController.getStoreCount(storeName);

        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
    }
}