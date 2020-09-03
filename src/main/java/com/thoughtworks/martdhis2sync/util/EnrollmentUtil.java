package com.thoughtworks.martdhis2sync.util;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;

public class EnrollmentUtil {

    public static Date newCompletedDate = new Date(Long.MIN_VALUE);
    public static Date newCancelledDate = new Date(Long.MIN_VALUE);
    public static Date newActiveDate = new Date(Long.MIN_VALUE);

    public static Date updatedCompletedDate = new Date(Long.MIN_VALUE);
    public static Date updatedActiveDate = new Date(Long.MIN_VALUE);

    public static List<EnrollmentAPIPayLoad> enrollmentsToSaveInTracker = new ArrayList<>();

    // Map contains instance IDn Enrollment ID in current sync process
    public static Map<String, String> instanceIDEnrollmentIDMap = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(EnrollmentUtil.class);

    public static void updateLatestEnrollmentDateCreated(String dateCreated, String enrollmentType) throws Exception {
        logger.info("Enrollment type: " + enrollmentType);
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        switch(enrollmentType) {
            case MarkerUtil.CATEGORY_NEW_ACTIVE_ENROLLMENT :
                if (newActiveDate.compareTo(bahmniDateCreated) < 1) {
                    newActiveDate = bahmniDateCreated;
                }
                break;
            case MarkerUtil.CATEGORY_NEW_COMPLETED_ENROLLMENT :
                if (newCompletedDate.compareTo(bahmniDateCreated) < 1) {
                    newCompletedDate = bahmniDateCreated;
                }
                break;
            case MarkerUtil.CATEGORY_NEW_CANCELLED_ENROLLMENT :
                if (newCancelledDate.compareTo(bahmniDateCreated) < 1) {
                    newCancelledDate = bahmniDateCreated;
                }
                break;
            case MarkerUtil.CATEGORY_UPDATED_ACTIVE_ENROLLMENT :
                if (updatedActiveDate.compareTo(bahmniDateCreated) < 1) {
                    updatedActiveDate = bahmniDateCreated;
                }
                break;
            case MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT :
                if (updatedCompletedDate.compareTo(bahmniDateCreated) < 1) {
                    updatedCompletedDate = bahmniDateCreated;
                }
                break;
            default:
                throw new Exception("Enrollment type is invalid: " + enrollmentType);
        }
    }

    public static void updateMarker(MarkerUtil markerUtil, String programName, String enrollmentType) {
        String enrollmentDate = null;
        switch(enrollmentType) {
            case MarkerUtil.CATEGORY_NEW_ACTIVE_ENROLLMENT :
                enrollmentDate = BatchUtil.getStringFromDate(newActiveDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
                break;
            case MarkerUtil.CATEGORY_NEW_COMPLETED_ENROLLMENT :
                enrollmentDate = BatchUtil.getStringFromDate(newCompletedDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
                break;
            case MarkerUtil.CATEGORY_NEW_CANCELLED_ENROLLMENT :
                enrollmentDate = BatchUtil.getStringFromDate(newCancelledDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
                break;
            case MarkerUtil.CATEGORY_UPDATED_ACTIVE_ENROLLMENT :
                enrollmentDate = BatchUtil.getStringFromDate(updatedActiveDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
                break;
            case MarkerUtil.CATEGORY_UPDATED_COMPLETED_ENROLLMENT :
                enrollmentDate = BatchUtil.getStringFromDate(updatedCompletedDate, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
                break;
        }
        logger.info("Updating Enrollment marker with date : " + enrollmentDate + " Type : " + enrollmentType);
        markerUtil.updateMarkerEntry(programName, enrollmentType, enrollmentDate);
    }
}
