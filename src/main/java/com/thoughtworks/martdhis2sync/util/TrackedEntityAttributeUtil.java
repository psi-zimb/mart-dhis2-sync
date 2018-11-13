package com.thoughtworks.martdhis2sync.util;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class TrackedEntityAttributeUtil {
    @Getter
    @Setter
    private static List<String> dateTimeAttributes;
}
