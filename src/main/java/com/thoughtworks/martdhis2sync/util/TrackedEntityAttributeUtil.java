package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.TrackedEntityAttribute;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class TrackedEntityAttributeUtil {
    @Getter
    @Setter
    private static List<TrackedEntityAttribute> dateTimeAttributes;
}
