package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EnrollmentUtil {

    public static Date date = new Date(Long.MIN_VALUE);
    public static List<EnrollmentAPIPayLoad> enrollmentsToSaveInTracker = new ArrayList<>();
}
