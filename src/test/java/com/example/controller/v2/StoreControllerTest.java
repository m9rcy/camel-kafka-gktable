package com.example.controller.v2;

import com.example.service.v2.KafkaStateStoreServiceV2;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StoreControllerTest {

    @Mock
    private KafkaStateStoreServiceV2 kafkaStateStoreService;

    @Mock
    private GlobalKTable<String, Object> mockGlobalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, Object> mockStore;

    private StoreController storeController;
    private Map<String, GlobalKTable<?, ?>> globalKTableLookup;

    @BeforeEach
    void setUp() {
        globalKTableLookup = new HashMap<>();
        storeController = new StoreController(kafkaStateStoreService, globalKTableLookup);
    }

    @Test
    void getAllStores_shouldReturnStoreNames_whenStoresExist() {
        // Given
        globalKTableLookup.put("store1", mockGlobalKTable);
        globalKTableLookup.put("store2", mockGlobalKTable);

        // When
        ResponseEntity<Object> response = storeController.getAllStores();

        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Set<String>> responseBody = (Map<String, Set<String>>) response.getBody();
        assertNotNull(responseBody);
        assertTrue(responseBody.containsKey("stores"));
        assertEquals(2, responseBody.get("stores").size());
        assertTrue(responseBody.get("stores").contains("store1"));
        assertTrue(responseBody.get("stores").contains("store2"));
    }

    @Test
    void getAllStores_shouldReturnEmptySet_whenNoStoresExist() {
        // When
        ResponseEntity<Object> response = storeController.getAllStores();

        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Set<String>> responseBody = (Map<String, Set<String>>) response.getBody();
        assertNotNull(responseBody);
        assertTrue(responseBody.containsKey("stores"));
        assertTrue(responseBody.get("stores").isEmpty());
    }

    @Test
    void getAllStores_shouldReturn500_whenExceptionOccurs() {
        // Given
        Map<String, GlobalKTable<?, ?>> faultyMap = mock(Map.class);
        when(faultyMap.keySet()).thenThrow(new RuntimeException("Test exception"));
        
        StoreController faultyController = new StoreController(kafkaStateStoreService, faultyMap);

        // When
        ResponseEntity<Object> response = faultyController.getAllStores();

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStoreNameById_shouldReturnValue_whenStoreAndIdExist() {
        // Given
        String storeName = "testStore";
        String id = "testId";
        String expectedValue = "testValue";
        
        globalKTableLookup.put(storeName, mockGlobalKTable);
        when(kafkaStateStoreService.getStoreFor(any(GlobalKTable.class))).thenReturn(mockStore);
        when(mockStore.get(id)).thenReturn(expectedValue);

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(expectedValue, response.getBody());
    }

    @Test
    void getStoreNameById_shouldReturn404_whenStoreDoesNotExist() {
        // Given
        String storeName = "nonExistentStore";
        String id = "testId";

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStoreNameById_shouldReturn404_whenValueNotFound() {
        // Given
        String storeName = "testStore";
        String id = "nonExistentId";
        
        globalKTableLookup.put(storeName, mockGlobalKTable);
        when(kafkaStateStoreService.getStoreFor(any(GlobalKTable.class))).thenReturn(mockStore);
        when(mockStore.get(id)).thenReturn(null);

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStoreNameById_shouldReturn500_whenExceptionOccurs() {
        // Given
        String storeName = "testStore";
        String id = "testId";
        
        globalKTableLookup.put(storeName, mockGlobalKTable);
        when(kafkaStateStoreService.getStoreFor(any(GlobalKTable.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, id);

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStoreCount_shouldReturnCount_whenStoreExists() {
        // Given
        String storeName = "testStore";
        long expectedCount = 42L;
        
        globalKTableLookup.put(storeName, mockGlobalKTable);
        when(kafkaStateStoreService.getStoreFor(any(GlobalKTable.class))).thenReturn(mockStore);
        when(mockStore.approximateNumEntries()).thenReturn(expectedCount);

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Long> responseBody = (Map<String, Long>) response.getBody();
        assertNotNull(responseBody);
        assertTrue(responseBody.containsKey("approximateCount"));
        assertEquals(expectedCount, responseBody.get("approximateCount"));
    }

    @Test
    void getStoreCount_shouldReturn404_whenStoreDoesNotExist() {
        // Given
        String storeName = "nonExistentStore";

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStoreCount_shouldReturn500_whenExceptionOccurs() {
        // Given
        String storeName = "testStore";
        
        globalKTableLookup.put(storeName, mockGlobalKTable);
        when(kafkaStateStoreService.getStoreFor(any(GlobalKTable.class)))
            .thenThrow(new RuntimeException("Test exception"));

        // When
        ResponseEntity<Object> response = storeController.getStoreCount(storeName);

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void getStore_shouldReturnNull_whenGlobalKTableIsNull() {
        // Given
        String storeName = "testStore";
        // globalKTableLookup is empty, so get() will return null

        // When
        ResponseEntity<Object> response = storeController.getStoreNameById(storeName, "testId");

        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(kafkaStateStoreService, never()).getStoreFor(any());
    }
}