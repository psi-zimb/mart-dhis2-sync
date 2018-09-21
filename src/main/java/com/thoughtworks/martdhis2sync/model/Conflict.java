package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Conflict {

    public static final String CONFLICT_OBJ_ORG_UNIT = "OrganisationUnit";
    public static final String CONFLICT_VAL_ORG_UNIT = "No org unit ID in tracked entity instance object";
    public static final String CONFLICT_OBJ_TEI_TYPE = "TrackedEntityInstance.trackedEntityType";
    public static final String CONFLICT_OBJ_ATTRIBUTE = "Attribute.attribute";
    public static final String CONFLICT_OBJ_ATTRIBUTE_VALUE = "Attribute.value";
    public static final String CONFLICT_OBJ_ENROLLMENT_DATE = "Enrollment.date";
    public static final String CONFLICT_OBJ_ENROLLMENT_INCIDENT_DATE = "Enrollment.incidentDate";

    private String object;
    private String value;
}
