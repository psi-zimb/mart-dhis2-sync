package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class EventTracker {
    private String eventId;
    private String instanceId;
    private String program;
    private int programUniqueId;
    private Date programStartDate;
}