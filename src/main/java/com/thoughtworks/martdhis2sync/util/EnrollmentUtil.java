package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import org.slf4j.Logger;

import java.util.*;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;

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

    public static void updateMarker(MarkerUtil markerUtil, String programName, Logger logger) {
        String enrollmentDate = BatchUtil.getStringFromDate(date, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        logger.info("Updating Enrollment marker with date : " + enrollmentDate);
        markerUtil.updateMarkerEntry(programName, CATEGORY_ENROLLMENT, enrollmentDate);
    }
}
