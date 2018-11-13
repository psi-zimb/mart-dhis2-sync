package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TrackedEntityAttributeResponse {
    private Pager pager;
    private List<TrackedEntityAttribute> trackedEntityAttributes;
}
