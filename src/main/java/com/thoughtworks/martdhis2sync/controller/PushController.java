package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.*;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import java.util.*;

import static com.thoughtworks.martdhis2sync.service.LoggerService.*;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.*;


@RestController
public class PushController {

    @Autowired
    private MappingService mappingService;

    @Autowired
    private TEIService teiService;

    @Autowired
    private LoggerService loggerService;

    @Autowired
    private DHISMetaDataService dhisMetaDataService;

    @Autowired
    private CompletedEnrollmentService completedEnrollmentService;

    @Autowired
    private CancelledEnrollmentService cancelledEnrollmentService;

    @Autowired
    private ActiveEnrollmentService activeEnrollmentService;

    @Autowired
    private MarkerUtil markerUtil;

    private List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();

    public static boolean IS_DELTA_EXISTS = false;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PutMapping(value = "/pushData")
    public void pushData(@RequestBody DHISSyncRequestBody requestBody) throws Exception {
        long timeInMillis = System.currentTimeMillis();
        IS_DELTA_EXISTS = false;
        loggerService.addLog(requestBody.getService(), requestBody.getUser(), requestBody.getComment());

        dhisMetaDataService.filterByTypeDateTime();

        Map<String, Object> mapping = mappingService.getMapping(requestBody.getService());

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);
        Config config = gson.fromJson(mapping.get("config").toString(), Config.class);
        EnrollmentUtil.newActiveDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_ACTIVE_ENROLLMENT);
        EnrollmentUtil.newCompletedDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_COMPLETED_ENROLLMENT);
        EnrollmentUtil.newCancelledDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_CANCELLED_ENROLLMENT);

        EnrollmentUtil.updatedActiveDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_ACTIVE_ENROLLMENT);
        EnrollmentUtil.updatedCompletedDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
        EnrollmentUtil.updatedCancelledDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_CANCELLED_ENROLLMENT);

        EventUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_EVENT);

        try {
            loggerService.clearLog();
            EnrollmentUtil.instanceIDEnrollmentIDMap.clear();
            Map<String,String> invalidPatients = teiService.verifyOrgUnitsForPatients(lookupTable, requestBody.getService());
            if(invalidPatients.size() > 0) {
                loggerService.collateLogMessage("Pre-validation for sync service failed. Invalid Org Unit specified for below patients. Update patient's clinical info in Bahmni, run Bahmni MART");
                invalidPatients.forEach((patientID,orgUnit)-> {
                    loggerService.collateLogMessage("[Patient ID (" + patientID + ") Org Unit ID (" + orgUnit + ")] ");
                });
                loggerService.updateLog(requestBody.getService(), FAILED);
                throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "Pre-validation for sync service failed. Invalid Org Unit specified for below patients. Update patient's clinical info in Bahmni, run Bahmni MART");
            }
            teiService.getTrackedEntityInstances(requestBody.getService(), mappingJson);
            teiService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getInstance(), mappingJson.getInstance(), config.getSearchable(), config.getComparable());
            triggerEnrollmentsSync(requestBody, lookupTable, mappingJson, config);

            if (!IS_DELTA_EXISTS) {
                loggerService.collateLogMessage(NO_DELTA_DATA);
                loggerService.updateLog(requestBody.getService(), SUCCESS);
                throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "NO DATA TO SYNC");
            } else {
                updateEventMarker(requestBody);
            }
            loggerService.updateLog(requestBody.getService(), SUCCESS);
        } catch (HttpServerErrorException e) {
            loggerService.updateLog(requestBody.getService(), FAILED);
            throw e;
        } catch (Exception e) {
            loggerService.updateLog(requestBody.getService(), FAILED);
        }
        logger.info("Push Controller completed and took: " + (System.currentTimeMillis() - timeInMillis)/1000 + " seconds");
    }

    private void updateEventMarker(DHISSyncRequestBody requestBody) {
        markerUtil.updateMarkerEntry(requestBody.getService(), CATEGORY_EVENT,
                getStringFromDate(EventUtil.date, DATEFORMAT_WITH_24HR_TIME));
    }

    private void triggerEnrollmentsSync(DHISSyncRequestBody requestBody, LookupTable lookupTable, MappingJson mappingJson, Config config) throws Exception {
        TrackersHandler.clearTrackerLists();

        logger.info("=========================TEI sync Success=========================\n\n" +
                "=========================Getting enrollments for TEI=========================\n");

        teiService.getEnrollmentsForInstances(lookupTable.getEnrollments(), lookupTable.getEvent(), requestBody.getService());

        logger.info("=========================Got enrollments for TEI=========================\n\n" +
                "=========================New Completed Enrollment Sync Started=========================\n");

        completedEnrollmentService.triggerJobForNewCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment());

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================New Completed Enrollment Sync Success=========================\n\n");
        logger.info("=========================New Cancelled Enrollment Sync Started=========================\n\n");

        cancelledEnrollmentService.triggerJobForNewCancelledEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment());

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================New Cancelled Enrollment Sync Success=========================\n\n");

        logger.info("=========================Update Cancelled Enrollment Sync Started=========================\n\n");

        cancelledEnrollmentService.triggerJobForUpdatedCancelledEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment());

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================Update Cancelled Enrollment Sync Success=========================\n\n");

        logger.info("=========================Update Complete Enrollment Sync Started=========================\n");

        completedEnrollmentService.triggerJobForUpdatedCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment());

        TrackersHandler.clearTrackerLists();

        logger.info("=========================Update Complete Enrollment Sync Success=========================\n\n" +
                "=========================New Active Enrollment Sync Started=========================\n");

        activeEnrollmentService.triggerJobForNewActiveEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment());

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();


        logger.info("=========================New Active Enrollment Sync Success=========================\n\n" +
                "=========================Update Active Enrollment Sync Started");

        activeEnrollmentService.triggerJobForUpdatedActiveEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment());
    }
}
