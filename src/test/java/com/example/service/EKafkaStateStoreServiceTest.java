package com.example.service;

import com.example.model.Code;
import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EKafkaStateStoreServiceTest {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, Object> mockStore;

    @Mock
    private KeyValueIterator<String, Object> mockIterator;

    private EKafkaStateStoreService kafkaStateStoreService;

    @BeforeEach
    void setUp() {
        kafkaStateStoreService = new EKafkaStateStoreService(streamsBuilderFactoryBean);
        kafkaStateStoreService.registerKnownStores(); // Initialize known stores
    }

    @Test
    void getAvailableStoreNames_shouldReturnRegisteredStores() {
        // When
        Set<String> storeNames = kafkaStateStoreService.getAvailableStoreNames();

        // Then
        assertThat(storeNames).containsExactlyInAnyOrder(
                "order-window-global-store",
                "code-global-store"
        );
    }

    @Test
    void registerStore_shouldAddNewStore() {
        // Given
        String newStoreName = "new-store";
        String storeType = "NewType";

        // When
        kafkaStateStoreService.registerStore(newStoreName, storeType);
        Set<String> storeNames = kafkaStateStoreService.getAvailableStoreNames();

        // Then
        assertThat(storeNames).contains(newStoreName);
    }

    @Test
    void getStore_shouldReturnStore_whenStoreExists() {
        // Given
        String storeName = "order-window-global-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);

        // When
        ReadOnlyKeyValueStore<String, Object> result = kafkaStateStoreService.getStore(storeName);

        // Then
        assertThat(result).isEqualTo(mockStore);
//        verify(kafkaStreams).store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

        // Capture the argument to verify its properties
        ArgumentCaptor<StoreQueryParameters> captor = ArgumentCaptor.forClass(StoreQueryParameters.class);
        verify(kafkaStreams).store(captor.capture());

        StoreQueryParameters captured = captor.getValue();
        assertEquals(storeName, captured.storeName());
        assertEquals(QueryableStoreTypes.keyValueStore().getClass(), captured.queryableStoreType().getClass());
    }

    @Test
    void getStore_shouldThrowException_whenStoreNotRegistered() {
        // Given
        String invalidStoreName = "invalid-store";

        // When & Then
        assertThatThrownBy(() -> kafkaStateStoreService.getStore(invalidStoreName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Store not found: invalid-store");
    }

    @Test
    void getStore_shouldThrowException_whenKafkaStreamsNotAvailable() {
        // Given
        String storeName = "order-window-global-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(null);

        // When & Then
        assertThatThrownBy(() -> kafkaStateStoreService.getStore(storeName))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Kafka Streams not available yet");
    }

    @Test
    void getStoreValue_shouldReturnValue_whenExists() {
        // Given
        String storeName = "order-window-global-store";
        String id = "order-123";
        OrderWindow orderWindow = OrderWindow.builder()
                .id(id)
                .name("Test Order")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(30))
                .version(1)
                .build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.get(id)).thenReturn(orderWindow);

        // When
        Object result = kafkaStateStoreService.getStoreValue(storeName, id);

        // Then
        assertThat(result).isEqualTo(orderWindow);
    }

    @Test
    void getStoreValue_shouldReturnNull_whenNotExists() {
        // Given
        String storeName = "order-window-global-store";
        String id = "non-existent-id";

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.get(id)).thenReturn(null);

        // When
        Object result = kafkaStateStoreService.getStoreValue(storeName, id);

        // Then
        assertThat(result).isNull();
    }

    @Test
    void getStoreStatus_shouldReturnBasicStatus() {
        // Given
        String storeName = "code-global-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.approximateNumEntries()).thenReturn(50L);
        when(mockStore.all()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(false);

        // When
        Map<String, Object> status = kafkaStateStoreService.getStoreStatus(storeName);

        // Then
        assertThat(status).containsEntry("approximateCount", 50L);
        assertThat(status).containsEntry("storeName", storeName);
        assertThat(status).containsEntry("storeType", "Code");
    }

    @Test
    void getStoreStatus_shouldReturnOrderWindowStatistics() {
        // Given
        String storeName = "order-window-global-store";
        
        OrderWindow order1 = OrderWindow.builder()
                .id("order-1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(30))
                .version(1)
                .build();
        
        OrderWindow order2 = OrderWindow.builder()
                .id("order-2")
                .name("Order 2")
                .status(OrderStatus.DRAFT)
                .planStartDate(OffsetDateTime.now().minusDays(1))
                .planEndDate(OffsetDateTime.now().plusDays(30))
                .version(1)
                .build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.approximateNumEntries()).thenReturn(2L);
        when(mockStore.all()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, true, false);
        when(mockIterator.next())
                .thenReturn(new org.apache.kafka.streams.KeyValue<>("order-1", order1))
                .thenReturn(new org.apache.kafka.streams.KeyValue<>("order-2", order2));

        // When
        Map<String, Object> status = kafkaStateStoreService.getStoreStatus(storeName);

        // Then
        assertThat(status).containsEntry("approximateCount", 2L);
        assertThat(status).containsEntry("storeName", storeName);
        assertThat(status).containsEntry("storeType", "OrderWindow");
        assertThat(status).containsKey("statusBreakdown");
        assertThat(status).containsEntry("activeOrdersCount", 1L); // Only APPROVED is active

        @SuppressWarnings("unchecked")
        Map<OrderStatus, Long> statusBreakdown = (Map<OrderStatus, Long>) status.get("statusBreakdown");
        assertThat(statusBreakdown.get(OrderStatus.APPROVED)).isEqualTo(1L);
        assertThat(statusBreakdown.get(OrderStatus.DRAFT)).isEqualTo(1L);
        assertThat(statusBreakdown.get(OrderStatus.LODGED)).isEqualTo(0L);
    }

    @Test
    void getStoreStatus_shouldReturnCodeStatistics() {
        // Given
        String storeName = "code-global-store";
        
        Code code1 = Code.builder()
                .id("code-1")
                .name("Code 1")
                .description("Description 1")
                .build();
        
        Code code2 = Code.builder()
                .id("code-2")
                .name("Code 2")
                .description("")
                .build();
        
        Code code3 = Code.builder()
                .id("code-3")
                .name("Code 3")
                .description(null)
                .build();

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.approximateNumEntries()).thenReturn(3L);
        when(mockStore.all()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        when(mockIterator.next())
                .thenReturn(new org.apache.kafka.streams.KeyValue<>("code-1", code1))
                .thenReturn(new org.apache.kafka.streams.KeyValue<>("code-2", code2))
                .thenReturn(new org.apache.kafka.streams.KeyValue<>("code-3", code3));

        // When
        Map<String, Object> status = kafkaStateStoreService.getStoreStatus(storeName);

        // Then
        assertThat(status).containsEntry("approximateCount", 3L);
        assertThat(status).containsEntry("storeName", storeName);
        assertThat(status).containsEntry("storeType", "Code");
        assertThat(status).containsEntry("codesWithDescription", 1L); // Only code1 has non-empty description
        assertThat(status).containsEntry("codesWithoutDescription", 2L); // code2 and code3
    }

    @Test
    void getStoreStatus_shouldHandleExceptionDuringStatistics() {
        // Given
        String storeName = "order-window-global-store";
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(mockStore);
        when(mockStore.approximateNumEntries()).thenReturn(100L);
        when(mockStore.all()).thenThrow(new RuntimeException("Iterator error"));

        // When
        Map<String, Object> status = kafkaStateStoreService.getStoreStatus(storeName);

        // Then
        assertThat(status).containsEntry("approximateCount", 100L);
        assertThat(status).containsEntry("storeName", storeName);
        assertThat(status).containsEntry("storeType", "OrderWindow");
        assertThat(status).containsEntry("statisticsError", "Unable to calculate detailed statistics");
    }

    @Test
    void getStoreStatus_shouldThrowException_whenStoreNotRegistered() {
        // Given
        String invalidStoreName = "invalid-store";

        // When & Then
        assertThatThrownBy(() -> kafkaStateStoreService.getStoreStatus(invalidStoreName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Store not found: invalid-store");
    }
}