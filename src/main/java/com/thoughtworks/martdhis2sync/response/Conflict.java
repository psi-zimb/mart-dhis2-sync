package com.thoughtworks.martdhis2sync.response;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Conflict {

    private String object;
    private String value;
}
