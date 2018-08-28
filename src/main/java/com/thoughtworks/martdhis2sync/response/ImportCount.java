package com.thoughtworks.martdhis2sync.response;

import lombok.Data;

@Data
public class ImportCount {
    int imported;
    int updated;
    int ignored;
    int deleted;
}
