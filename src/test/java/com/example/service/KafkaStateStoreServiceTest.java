package com.example.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaStateStoreServiceTest {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private GlobalKTable<String, String> globalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, String> store;

    @Mock
    private StreamsMetadata streamsMetadata;

    private KafkaStateStoreService service;

    @BeforeEach
    void setUp() {
        service = new KafkaStateStoreService(streamsBuilderFactoryBean);
    }

    @Test
    void testGetStoreForGlobalKTable() {
        // Given
        String storeName = "test-store";
        when(globalKTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        // When
        ReadOnlyKeyValueStore<String, String> result = service.getStoreFor(globalKTable);

        // Then
        assertSame(store, result);

        // Capture the argument to verify its properties
        ArgumentCaptor<StoreQueryParameters> captor = ArgumentCaptor.forClass(StoreQueryParameters.class);
        verify(kafkaStreams).store(captor.capture());

        StoreQueryParameters captured = captor.getValue();
        assertEquals(storeName, captured.storeName());
        assertEquals(QueryableStoreTypes.keyValueStore().getClass(), captured.queryableStoreType().getClass());
    }

    @Test
    void testGetStoreByName() {
        // Given
        String storeName = "test-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        // When
        ReadOnlyKeyValueStore<String, String> result = service.getStore(storeName);

        // Then
        assertSame(store, result);

        // Capture the argument to verify its properties
        ArgumentCaptor<StoreQueryParameters> captor = ArgumentCaptor.forClass(StoreQueryParameters.class);
        verify(kafkaStreams).store(captor.capture());

        StoreQueryParameters captured = captor.getValue();
        assertEquals(storeName, captured.storeName());
        assertEquals(QueryableStoreTypes.keyValueStore().getClass(), captured.queryableStoreType().getClass());
    }

    @Test
    void testGetStoreWhenKafkaStreamsNotAvailable() {
        // Given
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);

        // When & Then
        assertThrows(IllegalStateException.class, () -> service.getStore("test-store"));
    }

    @Test
    void testGetAllStoreNames() {
        // Given
        String storeName = "test-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(streamsMetadata.stateStoreNames()).thenReturn(Collections.singleton(storeName));
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.singleton(streamsMetadata));

        // When
        Set<String> result = service.getAllStoreNames();

        // Then
        assertEquals(1, result.size());
        assertTrue(result.contains(storeName));
        verify(kafkaStreams).metadataForAllStreamsClients();
    }

    @Test
    void testGetAllStoreNamesWhenEmpty() {
        // Given
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.emptySet());

        // When
        Set<String> result = service.getAllStoreNames();

        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAllStoreNamesWhenKafkaStreamsNotAvailable() {
        // Given
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);

        // When & Then
        assertThrows(IllegalStateException.class, () -> service.getAllStoreNames());
    }

    @Test
    void testGetAllStoreNamesWithMultipleStores() {
        // Given
        Set<String> expectedStores = Set.of("store1", "store2", "store3");
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(streamsMetadata.stateStoreNames()).thenReturn(expectedStores);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.singleton(streamsMetadata));

        // When
        Set<String> result = service.getAllStoreNames();

        // Then
        assertEquals(3, result.size());
        assertTrue(result.containsAll(expectedStores));
    }
}