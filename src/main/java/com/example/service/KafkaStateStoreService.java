package com.example.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class KafkaStateStoreService {
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public<K,V>ReadOnlyKeyValueStore<K,V> getStoreFor(GlobalKTable<K,V> globalKTable) {
        return getStore(globalKTable.queryableStoreName());
    }

    public<V, K> ReadOnlyKeyValueStore<K,V> getStore(String storeName) {
        QueryableStoreType<ReadOnlyKeyValueStore<K,V>> storeType = QueryableStoreTypes.keyValueStore();
        return getKafkaStream().store(StoreQueryParameters.fromNameAndType(storeName, storeType));
    }

    private KafkaStreams getKafkaStream() {
        return Optional.ofNullable(streamsBuilderFactoryBean.getKafkaStreams()).orElseThrow(() -> new IllegalStateException("Kafka Streams not available yet"));
    }
}
