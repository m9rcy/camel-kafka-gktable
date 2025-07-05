package com.example.model;

import lombok.Builder;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
@Builder
public class GlobalKTableMetrics {
    private long totalRecords;
    private long duplicateKeys;
    private long tombstoneEligible;
    private long approvedStatus;
    private long releasedStatus;
    private OffsetDateTime lastUpdated;
}