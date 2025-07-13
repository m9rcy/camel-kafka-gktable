package com.example.controller;

import com.example.service.GlobalKTableRegistry;
import com.example.service.KafkaStateStoreService;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class StoreControllerMvcTest {

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @Mock
    private KafkaStateStoreService kafkaStateStoreService;

    @Mock
    private GlobalKTableRegistry globalKTableRegistry;

    @Mock
    private ReadOnlyKeyValueStore<Object, Object> keyValueStore;

    @InjectMocks
    private StoreController storeController;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(storeController).build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void getAllStores_shouldReturnStoreNames() throws Exception {
        // Given
        Set<String> expectedStores = Set.of("user-store", "product-store");
        when(kafkaStateStoreService.getAllStoreNames()).thenReturn(expectedStores);

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[?(@=='user-store')]").exists())
                .andExpect(jsonPath("$[?(@=='product-store')]").exists());

        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getAllStores_shouldReturn500OnError() throws Exception {
        // Given
        when(kafkaStateStoreService.getAllStoreNames()).thenThrow(new RuntimeException("Registry error"));

        // When & Then
        mockMvc.perform(get("/api/stores"))
                .andExpect(status().isInternalServerError());

        verify(kafkaStateStoreService).getAllStoreNames();
    }

    @Test
    void getStoreNameById_shouldReturnValueWhenFound() throws Exception {
        // Given
        String storeName = "user-store";
        String id = "123";
        Object value = "test-user";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(value);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isOk())
                .andExpect(content().string("test-user"));

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn404WhenStoreNotRegistered() throws Exception {
        // Given
        String storeName = "unknown-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(false);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isNotFound());

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService, never()).getStore(anyString());
        verify(keyValueStore, never()).get(anyString());
    }

    @Test
    void getStoreNameById_shouldReturn404WhenValueNotFound() throws Exception {
        // Given
        String storeName = "user-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.get(id)).thenReturn(null);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isNotFound());

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).get(id);
    }

    @Test
    void getStoreNameById_shouldReturn500OnError() throws Exception {
        // Given
        String storeName = "user-store";
        String id = "123";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenThrow(new RuntimeException("Store error"));

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/{id}", storeName, id))
                .andExpect(status().isInternalServerError());

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).get(anyString());
    }

    @Test
    void getStoreCount_shouldReturnCount() throws Exception {
        // Given
        String storeName = "user-store";
        long count = 42L;
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.approximateNumEntries()).thenReturn(count);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isOk())
                .andExpect(content().string("42"));

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturn404WhenStoreNotRegistered() throws Exception {
        // Given
        String storeName = "unknown-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(false);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isNotFound());

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService, never()).getStore(anyString());
        verify(keyValueStore, never()).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturn500OnError() throws Exception {
        // Given
        String storeName = "user-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenThrow(new RuntimeException("Store error"));

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isInternalServerError());

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore, never()).approximateNumEntries();
    }

    @Test
    void getStoreCount_shouldReturnZeroWhenStoreEmpty() throws Exception {
        // Given
        String storeName = "user-store";
        when(globalKTableRegistry.isRegistered(storeName)).thenReturn(true);
        when(kafkaStateStoreService.getStore(storeName)).thenReturn(keyValueStore);
        when(keyValueStore.approximateNumEntries()).thenReturn(0L);

        // When & Then
        mockMvc.perform(get("/api/stores/{storeName}/count", storeName))
                .andExpect(status().isOk())
                .andExpect(content().string("0"));

        verify(globalKTableRegistry).isRegistered(storeName);
        verify(kafkaStateStoreService).getStore(storeName);
        verify(keyValueStore).approximateNumEntries();
    }
}