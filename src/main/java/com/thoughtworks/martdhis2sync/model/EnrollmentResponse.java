package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

import java.util.List;

@Data
public class EnrollmentResponse {
    private String responseType;
    private String status;
    private int imported;
    private int updated;
    private int deleted;
    private int ignored;
    private List<EnrollmentImportSummary> importSummaries;
    private int total;
}
