package com.example.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import javax.validation.constraints.*;
import java.time.OffsetDateTime;

@Jacksonized
@Builder
@Data
public class OrderWindow {
    
    @NotNull(message = "ID cannot be null")
    @NotBlank(message = "ID cannot be blank")
    @Size(max = 50, message = "ID must be at most 50 characters")
    @Pattern(regexp = "^[a-zA-Z0-9_-]+$", message = "ID can only contain alphanumeric characters, hyphens, and underscores")
    private String id;
    
    @NotNull(message = "Name cannot be null")
    @NotBlank(message = "Name cannot be blank")
    @Size(max = 255, message = "Name must be at most 255 characters")
    private String name;
    
    @NotNull(message = "Status cannot be null")
    private OrderStatus status;
    
    @NotNull(message = "Plan start date cannot be null")
    @PastOrPresent(message = "Plan start date cannot be in the future")
    private OffsetDateTime planStartDate;
    
    @NotNull(message = "Plan end date cannot be null")
    private OffsetDateTime planEndDate;
    
    @NotNull(message = "Version cannot be null")
    @Min(value = 1, message = "Version must be at least 1")
    @Max(value = 1000, message = "Version must be at most 1000")
    private Integer version;
    
    // Custom validation method that can be called by @AssertTrue
//    @AssertTrue(message = "Plan end date must be after plan start date")
//    public boolean isValidDateRange() {
//        if (planStartDate == null || planEndDate == null) {
//            return true; // Let @NotNull handle null validation
//        }
//        return planEndDate.isAfter(planStartDate);
//    }
    
    // Business rule validation
    //@AssertTrue(message = "Released orders cannot have plan end date more than 1 year in the future")
//    public boolean isValidReleasedDateRange() {
//        if (status != OrderStatus.RELEASED || planEndDate == null) {
//            return true;
//        }
//        return planEndDate.isBefore(OffsetDateTime.now().plusYears(1));
//    }
}