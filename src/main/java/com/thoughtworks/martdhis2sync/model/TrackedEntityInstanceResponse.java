package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrackedEntityInstanceResponse {
    private List<TrackedEntityInstanceInfo> trackedEntityInstances;
    private String message;
    private int httpStatusCode;
}
