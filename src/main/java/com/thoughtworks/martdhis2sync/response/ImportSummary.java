package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

@Data
public class ImportSummary {
    String responseType;
    String status;
    ImportCount importCount;
    String reference;
}
