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

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

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
                        // Convert DOMSource to XML String with declaration
                        .process(exchange -> {
                            Object body = exchange.getIn().getBody();
                            if (body instanceof DOMSource) {
                                DOMSource domSource = (DOMSource) body;
                                String xmlString = convertDomSourceToXmlString(domSource);
                                exchange.getMessage().setBody(xmlString);
                            }
                        })
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

    private String convertDomSourceToXmlString(DOMSource domSource) {
        try {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();

            // Set output properties for XML declaration
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.VERSION, "1.0");
            transformer.setOutputProperty(OutputKeys.STANDALONE, "yes");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            StringWriter writer = new StringWriter();
            transformer.transform(domSource, new StreamResult(writer));

            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert DOMSource to XML String", e);
        }
    }
}