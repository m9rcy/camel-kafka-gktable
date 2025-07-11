package com.example.processor;

import com.example.service.OrderWindowQueryService;
import com.example.service.predicate.OrderWindowPredicates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Updated tombstone processor using the specialized OrderWindowQueryService.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderWindowTombstoneV3Processor implements Processor {

    private static final int TOMBSTONE_THRESHOLD_DAYS = 13;

    private final OrderWindowQueryService orderWindowQueryService;

    @Override
    public void process(Exchange exchange) throws Exception {
        log.debug("Starting OrderWindowTombstoneV3Processor process");
        
        // Query for keys eligible for tombstone cleanup using predefined predicate
        List<String> tombstoneKeys = orderWindowQueryService.queryKeys(
            OrderWindowPredicates.isTombstoneEligible(TOMBSTONE_THRESHOLD_DAYS)
        );
        
        log.info("Found {} keys eligible for tombstone cleanup", tombstoneKeys.size());
        
        exchange.getMessage().setBody(tombstoneKeys);
        exchange.getMessage().setHeader("tombstoneCount", tombstoneKeys.size());
    }
}