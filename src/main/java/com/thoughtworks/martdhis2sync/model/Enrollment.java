package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class Enrollment {
    private String enrollment_id;
    private String instance_id;
    private String program_name;
    private Date program_start_date;
    private String status;
    private int program_unique_id;
}
