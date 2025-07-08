package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Processor that extracts data from GlobalKTable and returns the latest version
 * of each order (deduplication by ID, keeping highest version).
 */
@Component
@Slf4j
public class OrderWindowDataExtractorProcessor extends BaseGlobalKTableProcessor<OrderWindow> {

    public OrderWindowDataExtractorProcessor(
            KafkaStateStoreService kafkaStateStoreService,
            GlobalKTable<String, OrderWindow> orderWindowGlobalKTable) {
        super(kafkaStateStoreService, orderWindowGlobalKTable);
    }

    @Override
    protected boolean shouldIncludeEntry(KeyValue<String, OrderWindow> entry) {
        // Include all non-null OrderWindow entries
        return entry.value != null;
    }

    @Override
    protected OrderWindow transformEntry(KeyValue<String, OrderWindow> entry) {
        // Return the OrderWindow as-is, deduplication happens in post-processing
        return entry.value;
    }

    @Override
    protected List<OrderWindow> postProcess(List<OrderWindow> results) {
        // Group by ID and select the record with the highest version for each ID
        Map<String, OrderWindow> latestVersions = results.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        OrderWindow::getId,
                        Function.identity(),
                        (existing, replacement) ->
                                existing.getVersion() > replacement.getVersion() ? existing : replacement
                ));

        List<OrderWindow> result = new ArrayList<>(latestVersions.values());

        log.debug("Deduplicated {} raw records to {} unique orders", 
                results.size(), result.size());
        
        return result;
    }

    @Override
    protected String getCountHeaderName() {
        return "dataExtractCount";
    }
}