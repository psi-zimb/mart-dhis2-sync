package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrackedEntityInstanceInfo {
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
    private List<EnrollmentDetails> enrollments;
    private List<Object> relationships;
    private List<Attribute> attributes;

    public boolean hasAttribute(String attributeId) {
        if (attributes != null) {
            return attributes.stream().anyMatch(attribute ->
                    attribute.getAttribute().equals(attributeId));
        }
        return false;
    }

    public String getAttributeValue(String attributeId) {
        if (attributes != null) {
            List<Attribute> b = attributes.stream().filter(attribute ->
                    attribute.getAttribute().equals(attributeId)).collect(Collectors.toList());
            return b.get(0).getValue();
        }
        return "";
    }
}
