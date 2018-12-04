package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrackedEntityInstance {
    private String created;
    private String orgUnit;
    private String createdAtClient;
    private String trackedEntityInstance;
    private String lastUpdated;
    private String trackedEntityType;
    private String lastUpdatedAtClient;
    private boolean inactive;
    private boolean deleted;
    private String featureType;
    private List<Object> programOwners;
    private List<Object> enrollments;
    private List<Object> relationships;
    private List<Attribute> attributes;
}
