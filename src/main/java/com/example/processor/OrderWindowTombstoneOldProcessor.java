package com.example.processor;

import com.example.model.OrderStatus;
import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@Component
@Slf4j
public class OrderWindowTombstoneOldProcessor implements Processor {

    private final KafkaStateStoreService kafkaStateStoreService;
    private final GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;

    @Override
    public void process(Exchange exchange) throws Exception {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        List<String> keysToTombstone = new ArrayList<>();
        OffsetDateTime thirteenDaysAgo = OffsetDateTime.now().minusDays(13);
        
        log.info("Starting tombstone cleanup process. Cutoff date: {}", thirteenDaysAgo);
        
        // Iterate through all key-value pairs in the store
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                OrderWindow orderWindow = entry.value;
                
                if (shouldTombstone(orderWindow, thirteenDaysAgo)) {
                    keysToTombstone.add(entry.key);
                    log.info("Marking key {} for tombstone - Status: {}, EndDate: {}", 
                            entry.key, orderWindow.getStatus(), orderWindow.getPlanEndDate());
                }
            }
        }
        
        log.info("Found {} records to tombstone", keysToTombstone.size());
        exchange.getMessage().setBody(keysToTombstone);
        exchange.getMessage().setHeader("tombstoneCount", keysToTombstone.size());
    }
    
    private boolean shouldTombstone(OrderWindow orderWindow, OffsetDateTime thirteenDaysAgo) {
        return orderWindow != null && 
               orderWindow.getStatus() == OrderStatus.RELEASED &&
               orderWindow.getPlanEndDate().isBefore(thirteenDaysAgo);
    }
}