package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImportSummary {

    public static final String RESPONSE_SUCCESS = "SUCCESS";
    public static final String RESPONSE_ERROR = "ERROR";

    private String responseType;
    private String status;
    private ImportCount importCount;
    private String description;
    private List<Conflict> conflicts = new ArrayList<>();
    private String reference;
}
