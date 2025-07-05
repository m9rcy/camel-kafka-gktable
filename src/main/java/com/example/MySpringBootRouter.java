package com.example;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.github.javafaker.Faker;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class MySpringBootRouter extends RouteBuilder {

    @Autowired
    private KafkaStateStoreService kafkaStateStoreService;

    @Autowired
    GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;



    @Override
    public void configure() {
        Faker faker = new Faker(new Locale("en-NZ"));
        from("timer:orderWindowProducer?period={{timer.period}}").autoStartup(false).routeId("orderWindowProducer_")
                .process(exchange -> {
                    String[] possibleStatus = {"APPROVED", "RELEASED", "DONE", "DRAFT"};

                    int randomCount = faker.random().nextInt(5000, 10000);
                    List<String> randomLengthList = new ArrayList<>();
                    for (int i = 0; i < randomCount; i++) {
                        randomLengthList.add(faker.idNumber().valid());
                    }
                    OrderWindow orderEvent = OrderWindow.builder()
                            .id(String.valueOf(faker.number().numberBetween(0, 5000)))
                            .version(faker.number().numberBetween(0, 5))
                            .name(faker.lorem().word())
                            .status(OrderStatus.valueOf(faker.options().option(possibleStatus)))
                            .planStartDate(OffsetDateTime.now().plusMinutes(faker.number().numberBetween(2, 58)))
                            .planEndDate(OffsetDateTime.now().plusHours(faker.number().numberBetween(1, 9)))
                            .build();
                    exchange.getMessage().setBody(orderEvent);
                    exchange.getMessage().setHeader(KafkaConstants.KEY, orderEvent.getId());
                })

                .marshal().json(JsonLibrary.Jackson)
                .log("Body before sending to Kafka ${body}")
                .to("kafka:_order-window-topic?"
                        + "brokers=localhost:9092"
                        + "&valueSerializer=org.apache.kafka.common.serialization.StringSerializer"
                        + "&keySerializer=org.apache.kafka.common.serialization.StringSerializer");

        from("timer:orderWindowConsumer?period=3600000;repeat=8").autoStartup(false).routeId("orderWindowConsumer_old")
                .process(exchange -> {
                    ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);

                    List<OrderWindow> orderWindowList = new ArrayList<>();

                    // Iterate through all key-value pairs in the store
                    try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
                        while (iterator.hasNext()) {
                            KeyValue<String, OrderWindow> entry = iterator.next();
                            if (entry.value != null) {
                                orderWindowList.add(entry.value);
                            }
                        }
                    }

                    // Group by id and select the record with the highest version for each id
                    Map<String, OrderWindow> latestVersions = orderWindowList.stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.toMap(
                                    OrderWindow::getId,
                                    Function.identity(),
                                    (existing, replacement) ->
                                            existing.getVersion() > replacement.getVersion() ? existing : replacement
                            ));

                    // Convert the map values back to a list
                    List<OrderWindow> result = new ArrayList<>(latestVersions.values());

                    exchange.getMessage().setBody(result);

                })
                .log("Body received from GKT ${body.size}")
                .process(exchange -> {
                    List<OrderWindow> orderWindowList = exchange.getIn().getBody(List.class);
                    DOMSource domSource = createDocument(orderWindowList);
                    exchange.getMessage().setBody(domSource);
                })
                .log("Body received formatter ${body}");
    }

    private DOMSource createDocument(List<OrderWindow> orderWindows) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.newDocument();

            // Create root element <poxp>
            Element poxpElement = document.createElement("poxp");
            document.appendChild(poxpElement);

            for (OrderWindow orderWindow : orderWindows) {
                // Create <out_update> element for each order
                Element outUpdateElement = document.createElement("out_update");
                poxpElement.appendChild(outUpdateElement);

                // Create <out> element
                Element outElement = document.createElement("out");
                outUpdateElement.appendChild(outElement);

                // Add order details
                addElement(document, outElement, "id", orderWindow.getId());
                addElement(document, outElement, "status", orderWindow.getStatus().toString());
                addElement(document, outElement, "notes", ""); // Empty notes
            }

            return new DOMSource(document);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create DOMSource", e);
        }
    }

    // Helper method to create child elements
    private void addElement(Document doc, Element parent, String tagName, String textContent) {
        Element element = doc.createElement(tagName);
        element.setTextContent(textContent);
        parent.appendChild(element);
    }

    public String generateXml(List<OrderWindow> orderWindows) throws Exception {
        XmlMapper xmlMapper = new XmlMapper();
        ObjectNode rootNode = xmlMapper.createObjectNode();
        ArrayNode outUpdates = rootNode.putArray("out_update");

        for (OrderWindow orderWindow : orderWindows) {
            ObjectNode outUpdateNode = outUpdates.addObject();
            // Create the "out" node inside each out_update
            ObjectNode outNode = outUpdateNode.putObject("out");

            outNode.put("id", orderWindow.getId());
            outNode.put("status", orderWindow.getStatus().toString());
            outNode.put("notes", "");
        }

        return xmlMapper.writer()
                .withRootName("poxp")
                .with(ToXmlGenerator.Feature.WRITE_XML_DECLARATION)
                .writeValueAsString(rootNode);
    }

}
