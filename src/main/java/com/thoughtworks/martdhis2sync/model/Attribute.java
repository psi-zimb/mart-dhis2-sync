package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Attribute {
    private String lastUpdated;
    private String storedBy;
    private String code;
    private String displayName;
    private String created;
    private String valueType;
    private String attribute;
    private String value;
}
