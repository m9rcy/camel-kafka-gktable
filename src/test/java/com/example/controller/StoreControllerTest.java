package com.example.controller;

import com.example.service.GlobalKTableRegistry;
import com.example.service.KafkaStateStoreService;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StoreControllerTest {

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTableRegistry globalKTableRegistry;

    @Mock
    private ReadOnlyKeyValueStore<String, Object> keyValueStore;

    @InjectMocks
    private StoreController storeController;

    @Test
    void getAllStores_shouldReturnStoreNames() {
        // Given
        Set<String> expectedStores = Set.of("user-store", "product-store");
        when(kafkaStateStoreService.getAllStoreNames()).thenReturn(expectedStores);

        // When
        ResponseEntity<Set<String>> response = storeController.getAllStores();

        // Then
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedStores, response.getBody());
        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getAllStores_shouldReturn500OnError() {
        // Given
        when(kafkaStateStoreService.getAllStoreNames()).thenThrow(new RuntimeException("Registry error"));

        // When
        ResponseEntity<Set<String>> response = storeController.getAllStores();

        // Then
        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getStoreNameById_shouldReturnValueWhenFound() {
        // Given
        String storeName = "user-store";
        String id = "123";
        Object expectedValue = "test-user";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(expectedValue);

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedValue, response.getBody());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn404WhenStoreNotRegistered() {
        // Given
        String storeName = "unknown-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(false);

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(404, response.getStatusCodeValue());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService, never()).getStore(anyString());
        verify(keyValueStore, never()).get(anyString());
    }

    @Test
    void getStoreNameById_shouldReturn404WhenValueNotFound() {
        // Given
        String storeName = "user-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(null);

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(404, response.getStatusCodeValue());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn500OnError() {
        // Given
        String storeName = "user-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenThrow(new RuntimeException("Store error"));

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).get(anyString());
    }

    @Test
    void getStoreCount_shouldReturnCount() {
        // Given
        String storeName = "user-store";
        long expectedCount = 42L;
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.approximateNumEntries()).thenReturn(expectedCount);

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(expectedCount, response.getBody());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturn404WhenStoreNotRegistered() {
        // Given
        String storeName = "unknown-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(false);

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(404, response.getStatusCodeValue());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService, never()).getStore(anyString());
        verify(keyValueStore, never()).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturn500OnError() {
        // Given
        String storeName = "user-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenThrow(new RuntimeException("Store error"));

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturnZeroWhenStoreEmpty() {
        // Given
        String storeName = "user-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.<Object, String>getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.approximateNumEntries()).thenReturn(0L);

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(0L, response.getBody());
        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).approximateNumEntries();
    }
}