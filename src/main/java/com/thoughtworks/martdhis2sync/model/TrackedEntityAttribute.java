package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrackedEntityAttribute {
    private String id;
    private String displayName;
}
