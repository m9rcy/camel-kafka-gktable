package com.example.processor;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
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
            GlobalKTable<String, OrderWindow> orderWindowGlobalKTable) {
        super(kafkaStateStoreService, orderWindowGlobalKTable);
    }

    @Override
    protected boolean shouldIncludeEntry(KeyValue<String, OrderWindow> entry) {
        OffsetDateTime cutoffDate = OffsetDateTime.now().minusDays(TOMBSTONE_THRESHOLD_DAYS);
        
        // Create predicate for tombstone eligibility
        Predicate<OrderWindow> isTombstoneEligible = 
                OrderWindowPredicates.isReleased()
                .and(OrderWindowPredicates.endDateBefore(cutoffDate));
        
        boolean eligible = isTombstoneEligible.test(entry.value);
        
        if (eligible) {
            log.debug("Marking key {} for tombstone - Status: {}, EndDate: {}", 
                    entry.key, entry.value.getStatus(), entry.value.getPlanEndDate());
        }
        
        return eligible;
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