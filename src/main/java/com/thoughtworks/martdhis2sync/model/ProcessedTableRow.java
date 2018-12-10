package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProcessedTableRow {
    private String programUniqueId;
    private EnrollmentAPIPayLoad payLoad;
}
