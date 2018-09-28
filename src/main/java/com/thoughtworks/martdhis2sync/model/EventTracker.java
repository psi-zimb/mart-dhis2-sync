package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventTracker {
    private String eventId;
    private String instanceId;
    private String program;
    private String eventUniqueId;
}