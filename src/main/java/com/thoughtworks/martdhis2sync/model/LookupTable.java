package com.thoughtworks.martdhis2sync.model;


import lombok.Data;

@Data
public class LookupTable {

    private String instance;
    private String enrollments;
    private String event;
}
