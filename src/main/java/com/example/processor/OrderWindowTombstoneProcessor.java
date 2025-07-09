package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import com.example.service.OrderWindowPredicateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Component;

import java.util.function.Predicate;

/**
 * Processor that identifies keys eligible for tombstone cleanup.
 * Finds RELEASED orders with planEndDate older than 13 days.
 */
@Component
@Slf4j
public class OrderWindowTombstoneProcessor extends BaseGlobalKTableProcessor<String> {

    private static final int TOMBSTONE_THRESHOLD_DAYS = 13;

    public OrderWindowTombstoneProcessor(
            KafkaStateStoreService kafkaStateStoreService,
            GlobalKTable<String, OrderWindow> orderWindowGlobalKTable,
            OrderWindowPredicateService predicateService) {
        super(kafkaStateStoreService, orderWindowGlobalKTable, predicateService);
    }

    @Override
    protected Predicate<OrderWindow> getFilterPredicate() {
        // Create predicate for tombstone eligibility using the predicate service
        return predicateService.isReleased()
                .and(predicateService.isOlderThan(TOMBSTONE_THRESHOLD_DAYS));
    }

    @Override
    protected String transformEntry(KeyValue<String, OrderWindow> entry) {
        // Return the key for tombstone cleanup
        return entry.key;
    }

    @Override
    protected String getCountHeaderName() {
        return "tombstoneCount";
    }
}