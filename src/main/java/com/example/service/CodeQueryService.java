package com.example.service;

import  com.example.model.Code;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Specialized Code query service.
 */
@Service
@Slf4j
public class CodeQueryService extends GlobalKTableQueryService<String, Code> {

    public CodeQueryService(KafkaStateStoreService kafkaStateStoreService, 
                           GlobalKTable<String, Code> codeGlobalKTable) {
        super(kafkaStateStoreService, codeGlobalKTable);
    }

    /**
     * Find codes by name pattern.
     * 
     * @param namePattern Pattern to match in code names
     * @return List of matching codes
     */
    public List<Code> findByNamePattern(String namePattern) {
        return queryValues(code -> code.getName() != null && 
                          code.getName().toLowerCase().contains(namePattern.toLowerCase()));
    }

    /**
     * Find codes by description pattern.
     * 
     * @param descriptionPattern Pattern to match in code descriptions
     * @return List of matching codes
     */
    public List<Code> findByDescriptionPattern(String descriptionPattern) {
        return queryValues(code -> code.getDescription() != null && 
                          code.getDescription().toLowerCase().contains(descriptionPattern.toLowerCase()));
    }

    /**
     * Check if a code exists by ID.
     * 
     * @param codeId The code ID to check
     * @return true if the code exists, false otherwise
     */
    public boolean codeExists(String codeId) {
        return getByKey(codeId) != null;
    }
}