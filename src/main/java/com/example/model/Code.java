package com.example.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Jacksonized
@Builder
@Data
public class Code {

    private String id;

    private String name;

    private String description;
}
