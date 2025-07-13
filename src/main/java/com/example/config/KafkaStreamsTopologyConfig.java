package com.example.config;

import com.example.KafkaTopicConfig;
import com.example.model.Code;
import com.example.model.OrderWindow;
import com.example.service.GlobalKTableRegistry;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

//@Configuration
//@EnableKafkaStreams
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsTopologyConfig {

    private final KafkaTopicConfig kafkaTopicConfig;
    private final StreamsBuilder streamsBuilder;
    private final GlobalKTableRegistry globalKTableRegistry;

    @Bean
    public GlobalKTable<String, OrderWindow> orderWindowGlobalKTable(
            KafkaProperties kafkaProperties, 
            ObjectMapper objectMapper) {
        
        JsonSerde<OrderWindow> orderWindowSerde = createValueSerde(kafkaProperties, OrderWindow.class, objectMapper);

        GlobalKTable<String, OrderWindow> orderWindowGlobalKTable = streamsBuilder.globalTable(
                kafkaTopicConfig.getOrderWindowTopic(),
                Consumed.with(Serdes.String(), orderWindowSerde),
                Materialized.as("order-window-global-store"));

        globalKTableRegistry.register("order-window-global-store", orderWindowGlobalKTable);

        return orderWindowGlobalKTable;
    }

    @Bean
    public GlobalKTable<String, Code> codeGlobalKTable(
            KafkaProperties kafkaProperties,
            ObjectMapper objectMapper) {

        JsonSerde<Code> codeJsonSerde = createValueSerde(kafkaProperties, Code.class, objectMapper);

        GlobalKTable<String, Code> codeGlobalKTable = streamsBuilder.globalTable(
                kafkaTopicConfig.getCodeLookupTopic(),
                Consumed.with(Serdes.String(), codeJsonSerde),
                Materialized.as("code-global-store")
        );

        globalKTableRegistry.register("code-global-store", codeGlobalKTable);

        return codeGlobalKTable;
    }

    /**
     * Creates a JsonSerde for the specified type using Spring Boot Kafka properties.
     * This method is made public to allow reuse in tests.
     */
    public <T> JsonSerde<T> createValueSerde(KafkaProperties kafkaProperties, Class<T> targetType, ObjectMapper objectMapper) {
        JavaType javaType = objectMapper.constructType(targetType);

        JsonSerializer<T> jsonSerializer = new JsonSerializer<>(javaType, objectMapper);
        JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(javaType, objectMapper, false);

        jsonSerializer.configure(kafkaProperties.buildProducerProperties(), false);
        jsonDeserializer.configure(kafkaProperties.buildConsumerProperties(), false);

        return new JsonSerde<>(jsonSerializer, jsonDeserializer);
    }
}