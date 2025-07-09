package com.example.processor.v2;

import com.example.service.GlobalKTableQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Simplified processor that identifies keys eligible for tombstone cleanup using the centralized query service.
 * Finds RELEASED orders with planEndDate older than 13 days.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderWindowTombstoneFinalProcessor implements Processor {

    private static final int TOMBSTONE_THRESHOLD_DAYS = 13;

    private final GlobalKTableQueryService globalKTableQueryService;

    @Override
    public void process(Exchange exchange) throws Exception {
        log.debug("Starting OrderWindowTombstoneProcessor process");
        
        // Query for keys eligible for tombstone cleanup using predefined predicate
        List<String> tombstoneKeys = globalKTableQueryService.queryKeys(
            GlobalKTableQueryService.OrderWindowPredicates.isTombstoneEligible(TOMBSTONE_THRESHOLD_DAYS)
        );
        
        log.info("Found {} keys eligible for tombstone cleanup", tombstoneKeys.size());
        
        exchange.getMessage().setBody(tombstoneKeys);
        exchange.getMessage().setHeader("tombstoneCount", tombstoneKeys.size());
    }
}