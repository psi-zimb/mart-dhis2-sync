package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

@Data
public class TrackedEntityResponse {
    String httpStatus;
    int httpStatusCode;
    String status;
    String message;
    private Response response;
}
