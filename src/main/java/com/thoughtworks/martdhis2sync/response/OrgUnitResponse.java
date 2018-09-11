package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

import java.util.List;

@Data
public class OrgUnitResponse {
    private Pager pager;
    private List<OrgUnit> organisationUnits;
}
