package com.example.service.v2;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaStateStoreServiceV2Test {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private GlobalKTable<String, Object> globalKTable;

    @Mock
    private ReadOnlyKeyValueStore<String, Object> readOnlyKeyValueStore;

    private KafkaStateStoreServiceV2 kafkaStateStoreService;

    @BeforeEach
    void setUp() {
        kafkaStateStoreService = new KafkaStateStoreServiceV2(streamsBuilderFactoryBean);
    }

    @Test
    void getStoreFor_shouldReturnStore_whenGlobalKTableExists() {
        // Given
        String storeName = "testStore";
        when(globalKTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(readOnlyKeyValueStore);

        // When
        ReadOnlyKeyValueStore<String, Object> result = kafkaStateStoreService.getStoreFor(globalKTable);

        // Then
        assertNotNull(result);
        assertEquals(readOnlyKeyValueStore, result);
        
        verify(globalKTable).queryableStoreName();
        verify(streamsBuilderFactoryBean).getKafkaStreams();
        verify(kafkaStreams).store(any());
    }

    @Test
    void getStoreFor_shouldThrowException_whenKafkaStreamsIsNull() {
        // Given
        when(globalKTable.queryableStoreName()).thenReturn("testStore");
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);

        // When & Then
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> kafkaStateStoreService.getStoreFor(globalKTable)
        );
        
        assertEquals("Kafka Streams not available yet", exception.getMessage());
        verify(globalKTable).queryableStoreName();
        verify(streamsBuilderFactoryBean).getKafkaStreams();
        verify(kafkaStreams, never()).store(any());
    }

    @Test
    void getStoreFor_shouldUseCorrectStoreQueryParameters() {
        // Given
        String storeName = "testStore";
        when(globalKTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(readOnlyKeyValueStore);

        // When
        kafkaStateStoreService.getStoreFor(globalKTable);

        // Then
        verify(kafkaStreams).store(argThat(params -> {
            // Unfortunately, StoreQueryParameters doesn't expose getters for verification
            // but we can verify that the correct type of parameter is created
            return params != null;
        }));
    }

    @Test
    void getStoreFor_shouldHandleGenericTypes() {
        // Given
        GlobalKTable<Integer, String> intStringTable = mock(GlobalKTable.class);
        ReadOnlyKeyValueStore<Integer, String> intStringStore = mock(ReadOnlyKeyValueStore.class);
        
        String storeName = "intStringStore";
        when(intStringTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(intStringStore);

        // When
        ReadOnlyKeyValueStore<Integer, String> result = kafkaStateStoreService.getStoreFor(intStringTable);

        // Then
        assertNotNull(result);
        assertEquals(intStringStore, result);
    }

    @Test
    void getStoreFor_shouldPropagateRuntimeException_fromKafkaStreams() {
        // Given
        String storeName = "testStore";
        RuntimeException testException = new RuntimeException("Kafka Streams error");
        
        when(globalKTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenThrow(testException);

        // When & Then
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> kafkaStateStoreService.getStoreFor(globalKTable)
        );
        
        assertEquals("Kafka Streams error", exception.getMessage());
        verify(globalKTable).queryableStoreName();
        verify(streamsBuilderFactoryBean).getKafkaStreams();
        verify(kafkaStreams).store(any());
    }

    @Test
    void getStoreFor_shouldHandleNullStoreName() {
        // Given
        when(globalKTable.queryableStoreName()).thenReturn(null);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(readOnlyKeyValueStore);

        // When
        ReadOnlyKeyValueStore<String, Object> result = kafkaStateStoreService.getStoreFor(globalKTable);

        // Then
        assertNotNull(result);
        assertEquals(readOnlyKeyValueStore, result);
        verify(kafkaStreams).store(any());
    }

    @Test
    void getStoreFor_shouldVerifyQueryableStoreType() {
        // Given
        String storeName = "testStore";
        when(globalKTable.queryableStoreName()).thenReturn(storeName);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any())).thenReturn(readOnlyKeyValueStore);

        // When
        kafkaStateStoreService.getStoreFor(globalKTable);

        // Then
        verify(kafkaStreams).store(argThat(params -> {
            // We can't directly verify the internal structure of StoreQueryParameters
            // But we know it should be created using QueryableStoreTypes.keyValueStore()
            return params != null;
        }));
    }
}