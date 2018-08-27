package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

@Data
public class Response {
    String responseType;
    String status;
    int imported;
    int updated;
    int deleted;
    int ignored;
    Object importSummaries;
    int total;
}
