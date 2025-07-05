package com.example;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultMessage;

import java.util.concurrent.TimeUnit;

public class CamelTestUtils {

    public static Exchange createTestExchange(CamelContext camelContext) {
        Exchange exchange = new DefaultExchange(camelContext);
        Message message = new DefaultMessage(camelContext);
        exchange.setIn(message);
        return exchange;
    }

    public static Exchange createTestExchange(CamelContext camelContext, Object body) {
        Exchange exchange = createTestExchange(camelContext);
        exchange.getIn().setBody(body);
        return exchange;
    }

    public static Exchange createTestExchangeWithHeaders(CamelContext camelContext, Object body, String headerName, Object headerValue) {
        Exchange exchange = createTestExchange(camelContext, body);
        exchange.getIn().setHeader(headerName, headerValue);
        return exchange;
    }

    public static NotifyBuilder createRouteCompletedNotifier(CamelContext camelContext, String routeId) {
        return new NotifyBuilder(camelContext)
                .fromRoute(routeId)
                .whenCompleted(1)
                .create();
    }

    public static NotifyBuilder createRouteCompletedNotifier(CamelContext camelContext, String routeId, int expectedCount) {
        return new NotifyBuilder(camelContext)
                .fromRoute(routeId)
                .whenCompleted(expectedCount)
                .create();
    }

    public static boolean waitForRouteCompletion(NotifyBuilder notifyBuilder, long timeout, TimeUnit timeUnit) {
        return notifyBuilder.matches(timeout, timeUnit);
    }

    public static void resetMockEndpoint(CamelContext camelContext, String endpointUri) {
        MockEndpoint mockEndpoint = camelContext.getEndpoint(endpointUri, MockEndpoint.class);
        mockEndpoint.reset();
    }

    public static MockEndpoint getMockEndpoint(CamelContext camelContext, String endpointUri) {
        return camelContext.getEndpoint(endpointUri, MockEndpoint.class);
    }

    public static Processor createHeaderSettingProcessor(String headerName, Object headerValue) {
        return exchange -> exchange.getIn().setHeader(headerName, headerValue);
    }

    public static Processor createBodySettingProcessor(Object body) {
        return exchange -> exchange.getIn().setBody(body);
    }
}