package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrollmentAPIPayLoadTemp {
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_CANCELLED = "CANCELLED";

    private String enrollment;
    private String instanceId;
    private String program;
    private String OrgUnit;
    private String programStartDate;
    private String incidentDate;
    private String status;
    private String programUniqueId;
    private List<EventTemp> events;
}
