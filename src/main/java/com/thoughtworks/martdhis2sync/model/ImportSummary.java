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

    String responseType;
    String status;
    ImportCount importCount;
    List<Conflict> conflicts = new ArrayList<>();
    String reference;
}
