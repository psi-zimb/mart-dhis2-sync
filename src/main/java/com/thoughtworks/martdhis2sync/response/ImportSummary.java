package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ImportSummary {

    public static final String RESPONSE_SUCCESS = "SUCCESS";

    String responseType;
    String status;
    ImportCount importCount;
    List<Conflict> conflicts = new ArrayList<>();
    String reference;
}
