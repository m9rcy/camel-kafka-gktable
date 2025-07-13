package com.example.service;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class GlobalKTableRegistry {
    private final Map<String, GlobalKTable<?, ?>> globalKTables = new ConcurrentHashMap<>();
    
    public <K, V> void register(String name, GlobalKTable<K, V> globalKTable) {
        globalKTables.putIfAbsent(name, globalKTable);
    }
    
    public Set<String> getAllRegisteredNames() {
        return globalKTables.keySet();
    }
    
    public <K, V> GlobalKTable<K, V> getGlobalKTable(String name) {
        return (GlobalKTable<K, V>) globalKTables.get(name);
    }
    
    public boolean isRegistered(String name) {
        return globalKTables.containsKey(name);
    }
    
    public void clear() {
        globalKTables.clear();
    }
}