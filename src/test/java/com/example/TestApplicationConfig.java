package com.example;

import com.example.KafkaTopicConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@TestConfiguration
public class TestApplicationConfig {

    @Bean
    @Primary
    public KafkaTopicConfig testKafkaTopicConfig() {
        KafkaTopicConfig config = new KafkaTopicConfig();
        config.setOrderWindowTopic("test-order-window-topic");
        config.setOrderWindowFilteredTopic("test-order-window-filtered-topic");
        return config;
    }

    @Bean
    @Primary
    public ObjectMapper testObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        return mapper;
    }

    @Bean
    public CamelContextConfiguration camelContextConfiguration() {
        return new CamelContextConfiguration() {
            @Override
            public void beforeApplicationStart(CamelContext camelContext) {
                // Configure Camel context for testing
                camelContext.setUseMDCLogging(true);
                camelContext.setLoadTypeConverters(true);
            }

            @Override
            public void afterApplicationStart(CamelContext camelContext) {
                // Any post-startup configuration
            }
        };
    }
}