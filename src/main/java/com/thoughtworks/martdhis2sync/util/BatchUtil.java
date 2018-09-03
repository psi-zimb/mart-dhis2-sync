package com.thoughtworks.martdhis2sync.util;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;

import java.io.IOException;

public class BatchUtil {

    public static String convertResourceOutputToString(Resource resource) throws IOException {
        return IOUtils.toString(resource.getInputStream());
    }

}
