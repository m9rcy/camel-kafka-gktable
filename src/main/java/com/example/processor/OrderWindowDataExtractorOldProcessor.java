package com.example.processor;

import com.example.model.OrderWindow;
import com.example.service.KafkaStateStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
@Slf4j
public class OrderWindowDataExtractorOldProcessor implements Processor {

    private final KafkaStateStoreService kafkaStateStoreService;
    private final GlobalKTable<String, OrderWindow> orderWindowGlobalKTable;
    @Override
    public void process(Exchange exchange) throws Exception {
        ReadOnlyKeyValueStore<String, OrderWindow> store = kafkaStateStoreService.getStoreFor(orderWindowGlobalKTable);
        List<OrderWindow> orderWindowList = new ArrayList<>();

        // Iterate through all key-value pairs in the store
        try (KeyValueIterator<String, OrderWindow> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, OrderWindow> entry = iterator.next();
                if (entry.value != null) {
                    orderWindowList.add(entry.value);
                }
            }
        }

        // Group by id and select the record with the highest version for each id
        Map<String, OrderWindow> latestVersions = orderWindowList.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        OrderWindow::getId,
                        Function.identity(),
                        (existing, replacement) ->
                                existing.getVersion() > replacement.getVersion() ? existing : replacement
                ));

        // Convert the map values back to a list
        List<OrderWindow> result = new ArrayList<>(latestVersions.values());

        log.info("Found {} records to extract", result.size());
        exchange.getMessage().setBody(result);
        exchange.getMessage().setHeader("dataExtractCount", result.size());
    }
}
