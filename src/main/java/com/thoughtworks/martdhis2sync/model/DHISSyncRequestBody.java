package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

@Data
public class DHISSyncRequestBody {
    private String service;
    private String user;
    private String comment;

}
