package com.example.processor;

import com.example.model.OrderWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMSource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Slf4j
public class OrderWindowConversionProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        List<?> rawList = exchange.getIn().getBody(List.class);

        // Handle null or empty list
        if (rawList == null || rawList.isEmpty()) {
            throw new IllegalArgumentException("Body must not be null or empty");
        }

        // Safe casting with element type checking
        List<OrderWindow> orderWindows = rawList.stream()
                .filter(Objects::nonNull)
                .filter(OrderWindow.class::isInstance)
                .map(OrderWindow.class::cast)
                .collect(Collectors.toList());

        log.info("Converting the Outages ({}) to DOMSource", orderWindows.size());

        exchange.getMessage().setBody(createDocument(orderWindows));
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
}
