package com.example.config;

import com.example.model.Code;
import com.example.model.OrderWindow;
import com.example.service.CodeQueryService;
import com.example.service.KafkaStateStoreService;
import com.example.service.OrderWindowQueryService;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryServiceConfiguration {

    @Bean
    public OrderWindowQueryService orderWindowQueryService(
            KafkaStateStoreService kafkaStateStoreService,
            GlobalKTable<String, OrderWindow> orderWindowGlobalKTable) {
        return new OrderWindowQueryService(kafkaStateStoreService, orderWindowGlobalKTable);
    }

    @Bean
    public CodeQueryService codeQueryService(
            KafkaStateStoreService kafkaStateStoreService,
            GlobalKTable<String, Code> codeGlobalKTable) {
        return new CodeQueryService(kafkaStateStoreService, codeGlobalKTable);
    }
}