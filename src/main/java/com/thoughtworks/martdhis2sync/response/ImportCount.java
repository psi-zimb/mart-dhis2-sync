package com.thoughtworks.martdhis2sync.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ImportCount {
    int imported;
    int updated;
    int ignored;
    int deleted;
}
