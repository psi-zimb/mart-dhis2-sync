package com.thoughtworks.martdhis2sync.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrgUnit {
    private String id;
    private String displayName;
}
