package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

import java.util.List;

@Data
public class ImportSummary {

    public static final String RESPONSE_SUCCESS = "SUCCESS";
    public static final String RESPONSE_CONFLICT = "CONFLICT";

    String responseType;
    String status;
    ImportCount importCount;
    List<Conflict> conflicts;
    String reference;
}
