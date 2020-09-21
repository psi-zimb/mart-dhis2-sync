package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrollmentDetails {
    private String program;
    private String enrollment;
    private String enrollmentDate;
    private String completedDate;
    private String status;
    private List<EventTemp> events;
}
