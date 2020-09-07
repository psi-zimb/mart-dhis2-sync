package com.thoughtworks.martdhis2sync.model;

import lombok.Data;

import java.util.Date;

@Data
public class DHISSyncRequestBody {
    private String service;
    private String user;
    private String comment;
    private Date startDate;
    private Date endDate;
}
