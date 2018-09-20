package com.thoughtworks.martdhis2sync.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImportCount {
    private int imported;
    private int updated;
    private int ignored;
    private int deleted;
}
