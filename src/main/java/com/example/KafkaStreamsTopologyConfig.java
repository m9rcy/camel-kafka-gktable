package com.example;

import com.example.model.OrderWindow;
import com.example.service.OrderWindowPredicateService;
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

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.function.Predicate;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsTopologyConfig {

    private final KafkaTopicConfig kafkaTopicConfig;
    private final StreamsBuilder streamsBuilder;
    private final OrderWindowPredicateService predicateService;

    private final Validator validator;

    @Bean
    public GlobalKTable<String, OrderWindow> orderWindowGlobalKTable(
            KafkaProperties kafkaProperties, 
            ObjectMapper objectMapper) {
        
        JsonSerde<OrderWindow> orderWindowSerde = createValueSerde(kafkaProperties, OrderWindow.class, objectMapper);

        // Consume Order Window Topic
        KStream<String, OrderWindow> inputStream = streamsBuilder.stream(
                kafkaTopicConfig.getOrderWindowTopic(),
                Consumed.with(Serdes.String(), orderWindowSerde)
        );

        // Apply validation and filtering using reusable predicates
        inputStream
                .filter(this::isValidRecord)
                .filter(this::isEligibleForGlobalKTable)
                .to(kafkaTopicConfig.getOrderWindowFilteredTopic(), 
                    Produced.with(Serdes.String(), orderWindowSerde));

        return streamsBuilder.globalTable(
                kafkaTopicConfig.getOrderWindowFilteredTopic(),
                Consumed.with(Serdes.String(), orderWindowSerde),
                Materialized.as("order-window-global-store")
        );
    }

    /**
     * Validates the record using both custom validator and Spring validation
     */
    private boolean isValidRecord(String key, OrderWindow orderWindow) {
        // Handle tombstone messages
        if (orderWindow == null) {
            log.debug("Allowing tombstone message for key: {}", key);
            return true;
        }

        // Spring validation
        Set<ConstraintViolation<OrderWindow>> violations = validator.validate(orderWindow);
        if (!violations.isEmpty()) {
            log.warn("OrderWindow failed Spring validation for key: {}, id: {}, violations: {}", 
                    key, orderWindow.getId(), violations);
            return false;
        }

        return true;
    }

    /**
     * Uses reusable predicates to determine if record should be in GlobalKTable
     */
    private boolean isEligibleForGlobalKTable(String key, OrderWindow orderWindow) {
        // Handle tombstone messages
        if (orderWindow == null) {
            log.debug("Allowing tombstone message for key: {}", key);
            return true;
        }

        // Use reusable predicates from service
        Predicate<OrderWindow> eligibilityPredicate = predicateService.createGlobalKTableEligibilityPredicate();
        
        boolean isEligible = eligibilityPredicate.test(orderWindow);
        
        if (!isEligible) {
            log.debug("OrderWindow not eligible for GlobalKTable - key: {}, id: {}, status: {}, planEndDate: {}", 
                    key, orderWindow.getId(), orderWindow.getStatus(), orderWindow.getPlanEndDate());
        }
        
        return isEligible;
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