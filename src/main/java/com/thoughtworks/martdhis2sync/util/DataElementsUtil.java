package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.DataElement;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class DataElementsUtil {

    @Getter
    @Setter
    private static List<DataElement> dateTimeElements;
}
