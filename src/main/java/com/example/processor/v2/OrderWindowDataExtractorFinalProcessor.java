package com.example.processor.v2;

import com.example.model.OrderWindow;
import com.example.service.GlobalKTableQueryService;
import com.example.service.OrderWindowQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Simplified processor that extracts data from GlobalKTable using the centralized query service.
 * Returns the latest version of each order (deduplication by ID, keeping highest version).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderWindowDataExtractorFinalProcessor implements Processor {

    private final OrderWindowQueryService globalKTableQueryService;

    @Override
    public void process(Exchange exchange) throws Exception {
        log.debug("Starting OrderWindowDataExtractorProcessor process");
        
        // Get all deduplicated OrderWindow values using the query service
        List<OrderWindow> results = globalKTableQueryService.queryAllDeduplicatedValues();
        
        log.info("Found {} deduplicated records to process", results.size());
        
        exchange.getMessage().setBody(results);
        exchange.getMessage().setHeader("dataExtractCount", results.size());
    }
}