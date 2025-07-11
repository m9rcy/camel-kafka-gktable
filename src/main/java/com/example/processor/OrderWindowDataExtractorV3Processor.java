package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.OrderWindowQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Updated processor using the specialized OrderWindowQueryService.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderWindowDataExtractorV3Processor implements Processor {

    private final OrderWindowQueryService orderWindowQueryService;

    @Override
    public void process(Exchange exchange) throws Exception {
        log.debug("Starting OrderWindowDataExtractorV3Processor process");
        
        // Get all deduplicated OrderWindow values using the query service
        List<OrderWindow> results = orderWindowQueryService.queryAllDeduplicatedValues();
        
        log.info("Found {} deduplicated records to process", results.size());
        
        exchange.getMessage().setBody(results);
        exchange.getMessage().setHeader("dataExtractCount", results.size());
    }
}