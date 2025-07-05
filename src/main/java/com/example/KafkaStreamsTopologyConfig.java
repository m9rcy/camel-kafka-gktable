package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.OffsetDateTime;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsTopologyConfig {

    private final KafkaTopicConfig kafkaTopicConfig;
    private final StreamsBuilder streamsBuilder;

    @Bean
    public GlobalKTable<String, OrderWindow> orderWindowGlobalKTable(KafkaProperties kafkaProperties, ObjectMapper objectMapper) {
        JsonSerde<OrderWindow> orderWindowSerde = createValueSerde(kafkaProperties, OrderWindow.class, objectMapper);

        // Consume Order Window Topic
        KStream<String, OrderWindow> inputStream = streamsBuilder.stream(
                kafkaTopicConfig.getOrderWindowTopic(),
                Consumed.with(Serdes.String(), orderWindowSerde)
        );
        // Filter Order Window Topic for GlobalKTable
        OffsetDateTime twelveDaysAgo = OffsetDateTime.now().minusDays(12);
        inputStream.filter((k, v) -> v == null ||
                (v.getStatus() == OrderStatus.APPROVED ||
                        (v.getStatus() == OrderStatus.RELEASED &&
                                (v.getPlanEndDate().isAfter(twelveDaysAgo) || v.getPlanEndDate().equals(twelveDaysAgo)))))
                .to(kafkaTopicConfig.getOrderWindowFilteredTopic(), Produced.with(Serdes.String(), orderWindowSerde));

        return streamsBuilder.globalTable(
                kafkaTopicConfig.getOrderWindowFilteredTopic(),
                Consumed.with(Serdes.String(),  orderWindowSerde),
                Materialized.as("order-window-global-store")
        );
    }

    private <T> JsonSerde<T> createValueSerde(KafkaProperties kafkaProperties, Class<T> targetType, ObjectMapper objectMapper) {
        JavaType javaType = objectMapper.constructType(targetType);

        JsonSerializer<T> jsonSerializer = new JsonSerializer<>(javaType, objectMapper);
        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(javaType, objectMapper, false);

        jsonSerializer.configure(kafkaProperties.buildProducerProperties(), false);
        jsonDeserializer.configure(kafkaProperties.buildConsumerProperties(), false);

        return new JsonSerde<>(jsonSerializer, jsonDeserializer);

    }
}
