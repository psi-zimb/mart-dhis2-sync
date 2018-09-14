package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

import java.util.List;

@Data
public class Response {
    String responseType;
    String status;
    int imported;
    int updated;
    int deleted;
    int ignored;
    List<ImportSummary> importSummaries;
    int total;
}
