package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrollmentDetails {
    private String program;
    private String trackedEntityInstance;
    private String enrollment;
    private String enrollmentDate;
    private String completedDate;
    private String status;
}
