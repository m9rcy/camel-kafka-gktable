package com.example.processor;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.transform.dom.DOMSource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OrderWindowConversionProcessorTest {

    private OrderWindowConversionProcessor processor;
    private Exchange exchange;
    
    @BeforeEach
    void setUp() {
        processor = new OrderWindowConversionProcessor();
        exchange = new DefaultExchange(new DefaultCamelContext());
    }
    
    @Test
    void testProcessWithValidOrderWindows() throws Exception {
        // Given
        OrderWindow order1 = OrderWindow.builder()
                .id("order1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .build();
                
        OrderWindow order2 = OrderWindow.builder()
                .id("order2")
                .name("Order 2")
                .status(OrderStatus.RELEASED)
                .build();
        
        List<OrderWindow> orderWindows = Arrays.asList(order1, order2);
        exchange.getIn().setBody(orderWindows);
        
        // When
        processor.process(exchange);
        
        // Then
        DOMSource domSource = exchange.getMessage().getBody(DOMSource.class);
        assertNotNull(domSource);
        
        Document document = (Document) domSource.getNode();
        Element rootElement = document.getDocumentElement();
        assertEquals("poxp", rootElement.getNodeName());
        
        NodeList outUpdates = rootElement.getElementsByTagName("out_update");
        assertEquals(2, outUpdates.getLength());
        
        // Verify first order
        Element firstOutUpdate = (Element) outUpdates.item(0);
        Element firstOut = (Element) firstOutUpdate.getElementsByTagName("out").item(0);
        assertEquals("order1", firstOut.getElementsByTagName("id").item(0).getTextContent());
        assertEquals("APPROVED", firstOut.getElementsByTagName("status").item(0).getTextContent());
        assertEquals("", firstOut.getElementsByTagName("notes").item(0).getTextContent());
    }
    
    @Test
    void testProcessWithNullBody() {
        // Given
        exchange.getIn().setBody(null);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
    }
    
    @Test
    void testProcessWithEmptyList() {
        // Given
        exchange.getIn().setBody(Collections.emptyList());
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> processor.process(exchange));
    }
    
    @Test
    void testProcessWithMixedNullValues() throws Exception {
        // Given
        OrderWindow order1 = OrderWindow.builder()
                .id("order1")
                .name("Order 1")
                .status(OrderStatus.APPROVED)
                .build();
        
        List<Object> mixedList = Arrays.asList(order1, null, "invalid");
        exchange.getIn().setBody(mixedList);
        
        // When
        processor.process(exchange);
        
        // Then
        DOMSource domSource = exchange.getMessage().getBody(DOMSource.class);
        assertNotNull(domSource);
        
        Document document = (Document) domSource.getNode();
        Element rootElement = document.getDocumentElement();
        
        NodeList outUpdates = rootElement.getElementsByTagName("out_update");
        assertEquals(1, outUpdates.getLength()); // Only valid OrderWindow should be processed
    }
}
