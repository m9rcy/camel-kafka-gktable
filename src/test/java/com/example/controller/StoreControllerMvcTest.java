package com.example.controller;

import com.example.service.KafkaStateStoreService;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Collections;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class StoreControllerMvcTest {

    private MockMvc mockMvc;

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private ReadOnlyKeyValueStore<Object, Object> keyValueStore;

    @InjectMocks
    private StoreController storeController;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(storeController)
                .build();
    }

    @Test
    void getAllStores_shouldReturnStoreNames() throws Exception {
        // Given
        Set<String> expectedStores = Set.of("store1", "store2");
        when(kafkaStateStoreService.getAllStoreNames()).thenReturn(expectedStores);

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0]").value("store2"))
                .andExpect(jsonPath("$[1]").value("store1"));

        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getAllStores_shouldReturn500OnError() throws Exception {
        // Given
        when(kafkaStateStoreService.getAllStoreNames()).thenThrow(new RuntimeException("Test error"));

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isInternalServerError());

        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getStoreNameById_shouldReturnValueWhenFound() throws Exception {
        // Given
        String storeName = "test-store";
        String id = "123";
        Object value = "test-value";
        when(kafkaStateStoreService.getStore(anyString())).thenReturn(keyValueStore);

        when(keyValueStore.get(id)).thenReturn(value);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isOk())
                .andExpect(content().string("test-value"));

        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn404WhenNotFound() throws Exception {
        // Given
        String storeName = "test-store";
        String id = "123";
        when(kafkaStateStoreService.getStore(anyString())).thenReturn(keyValueStore);

        when(keyValueStore.get(id)).thenReturn(null);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isNotFound());

        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn500OnError() throws Exception {
        // Given
        String storeName = "test-store";
        String id = "123";
        when(kafkaStateStoreService.getStore(storeName)).thenThrow(new RuntimeException("Test error"));

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isInternalServerError());

        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).get(anyString());
    }

    @Test
    void getStoreCount_shouldReturnCount() throws Exception {
        // Given
        String storeName = "test-store";
        long count = 42L;
        when(kafkaStateStoreService.getStore(anyString())).thenReturn(keyValueStore);

        when(keyValueStore.approximateNumEntries()).thenReturn(count);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isOk())
                .andExpect(content().string("42"));

        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturn500OnError() throws Exception {
        // Given
        String storeName = "test-store";
        when(kafkaStateStoreService.getStore(storeName)).thenThrow(new RuntimeException("Test error"));

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isInternalServerError());

        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturnZeroWhenStoreEmpty() throws Exception {
        // Given
        String storeName = "test-store";
        when(kafkaStateStoreService.getStore(anyString())).thenReturn(keyValueStore);

        when(keyValueStore.approximateNumEntries()).thenReturn(0L);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isOk())
                .andExpect(content().string("0"));
    }
}