package com.example;

import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorOldProcessor;
import com.example.processor.OrderWindowTombstoneOldProcessor;
import com.example.service.GlobalKTableMetricsService;
import com.example.service.KafkaStateStoreService;
import org.apache.camel.spring.boot.CamelAutoConfiguration;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.mockito.Mockito.mock;


@TestConfiguration
@EnableAutoConfiguration
@Import(CamelAutoConfiguration.class)
public class IntegrationTestConfiguration {

    @Bean
    @Primary
    public KafkaStateStoreService kafkaStateStoreService() {
        return mock(KafkaStateStoreService.class);
    }

    @Bean
    @Primary
    public GlobalKTableMetricsService globalKTableMetricsService() {
        return mock(GlobalKTableMetricsService.class);
    }

    @Bean
    @Primary
    public KafkaStreamsTopologyConfig kafkaStreamsTopologyConfig() {
        return mock(KafkaStreamsTopologyConfig.class);
    }

    @Bean
    @Primary
    public KafkaTopicConfig kafkaTopicConfig() {
        return mock(KafkaTopicConfig.class);
    }

    @Bean
    @Primary
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean() {
        return mock(StreamsBuilderFactoryBean.class);
    }

    @Bean
    @Primary
    public GlobalKTable<String, Object> globalKTable() {
        return mock(GlobalKTable.class);
    }

    @Bean("orderWindowDataExtractorProcessor")
    @Primary
    public OrderWindowDataExtractorOldProcessor orderWindowDataExtractorProcessor() {
        return mock(OrderWindowDataExtractorOldProcessor.class);
    }

    @Bean("xmlProcessor")
    @Primary
    public OrderWindowConversionProcessor xmlProcessor() {
        return mock(OrderWindowConversionProcessor.class);
    }

    @Bean("orderWindowTombstoneProcessor")
    @Primary
    public OrderWindowTombstoneOldProcessor orderWindowTombstoneProcessor() {
        return mock(OrderWindowTombstoneOldProcessor.class);
    }
}