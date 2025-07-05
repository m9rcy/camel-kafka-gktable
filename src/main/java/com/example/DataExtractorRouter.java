package com.example;

import com.example.model.OrderWindow;
import com.example.processor.OrderWindowConversionProcessor;
import com.example.processor.OrderWindowDataExtractorProcessor;
import com.example.processor.OrderWindowTombstoneProcessor;
import com.example.service.KafkaStateStoreService;
import org.apache.camel.builder.RouteBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DataExtractorRouter extends RouteBuilder {
    @Autowired
    private KafkaStateStoreService kafkaStateStoreService;
    @Autowired
    GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;
    @Autowired
    OrderWindowConversionProcessor xmlProcessor;
    @Autowired
    OrderWindowTombstoneProcessor orderWindowTombstoneProcessor;

    @Autowired
    OrderWindowDataExtractorProcessor orderWindowDataExtractorProcessor;
    @Autowired
    private KafkaTopicConfig kafkaTopicConfig;
    
    @Override
    public void configure() {
        // Existing route for data extraction
        from("timer:orderWindowConsumer?period=3600000;repeat=8").autoStartup(true).routeId("orderWindowConsumer")
                // Find all data in GTK and filter by null and distinct
                .process(orderWindowDataExtractorProcessor)
                .choice()
                    .when(header("dataExtractCount").isGreaterThan(0))
                        .log("Found ${header.dataExtractCount} records to process")
                        .process(xmlProcessor)
                    .endChoice()
                    .otherwise()
                        .log("No records found to extract")
                .end()
                .log("Body received formatter ${body}");

        // New tombstone cleanup route - runs daily
        from("timer:orderWindowTombstoneCleanup?period=86400000").autoStartup(true).routeId("orderWindowTombstoneCleanup")
                .log("Starting tombstone cleanup process...")
                .process(orderWindowTombstoneProcessor)
                .choice()
                    .when(header("tombstoneCount").isGreaterThan(0))
                        .log("Found ${header.tombstoneCount} records to tombstone")
                        .split(body())
                        .process(exchange -> {
                            String keyToTombstone = exchange.getIn().getBody(String.class);
                            exchange.getMessage().setBody(null); // Tombstone message (null value)
                            exchange.getMessage().setHeader("CamelKafkaKey", keyToTombstone);
                        })
                        .to("kafka:" + kafkaTopicConfig.getOrderWindowFilteredTopic() +
                            "?brokers=localhost:9092" +
                            "&keySerializer=org.apache.kafka.common.serialization.StringSerializer" +
                            "&valueSerializer=org.apache.kafka.common.serialization.StringSerializer")
                        .log("Tombstoned key: ${header.CamelKafkaKey}")
                    .endChoice()
                    .otherwise()
                        .log("No records found for tombstone cleanup")
                .end();
    }
}