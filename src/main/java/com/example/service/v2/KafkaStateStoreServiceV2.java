package com.example.service.v2;

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
import java.util.Set;

@Service
@RequiredArgsConstructor
public class KafkaStateStoreServiceV2 {
    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public<K,V>ReadOnlyKeyValueStore<K,V> getStoreFor(GlobalKTable<K,V> globalKTable) {
        return getStore(globalKTable.queryableStoreName());
    }

    private<V, K> ReadOnlyKeyValueStore<K,V> getStore(String storeName) {
        QueryableStoreType<ReadOnlyKeyValueStore<K,V>> storeType = QueryableStoreTypes.keyValueStore();
        return getKafkaStream().store(StoreQueryParameters.fromNameAndType(storeName, storeType));
    }

    private KafkaStreams getKafkaStream() {
        return Optional.ofNullable(streamsBuilderFactoryBean.getKafkaStreams()).orElseThrow(() -> new IllegalStateException("Kafka Streams not available yet"));
    }
}
