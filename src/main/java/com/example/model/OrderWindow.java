package com.example.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.time.OffsetDateTime;

@Jacksonized
@Builder
@Data
public class OrderWindow {
    private String id;
    private String name;
    private OrderStatus status;
    private OffsetDateTime planStartDate;
    private OffsetDateTime planEndDate;
    private Integer version;
}
