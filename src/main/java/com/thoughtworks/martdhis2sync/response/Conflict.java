package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

@Data
public class Conflict {

    private String object;
    private String value;
}
