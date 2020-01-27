package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;

import java.util.*;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;

public class EnrollmentUtil {

    public static Date date = new Date(Long.MIN_VALUE);
    public static List<EnrollmentAPIPayLoad> enrollmentsToSaveInTracker = new ArrayList<>();

    // Map contains instance IDn Enrollment ID in current sync process
    public static Map<String, String> instanceIDEnrollmentIDMap = new HashMap<>();

    public static void updateLatestEnrollmentDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (date.compareTo(bahmniDateCreated) < 1) {
            date = bahmniDateCreated;
        }
    }
}
