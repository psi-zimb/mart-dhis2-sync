package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Enrollment {
    private String enrollment_id;
    private String instance_id;
    private String program;
    private String OrgUnit;
    private String program_start_date;
    private String incident_date;
    private String status;
    private String program_unique_id;
}
