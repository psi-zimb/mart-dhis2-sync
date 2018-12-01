package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

@Data
public class DHISEnrollmentSyncResponse {
    String httpStatus;
    int httpStatusCode;
    String status;
    String message;
    private EnrollmentResponse response;
}
