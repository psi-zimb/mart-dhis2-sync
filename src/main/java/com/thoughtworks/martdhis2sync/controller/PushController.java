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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.service.LoggerService.*;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
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
    public static boolean IS_DATE_RANGE_SYNC = false;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String dateFormat = "yyyy-MM-dd";

    @PutMapping(value = "/pushData")
    public void pushData(@RequestBody DHISSyncRequestBody requestBody) throws Exception {
        long timeInMillis = System.currentTimeMillis();
        IS_DELTA_EXISTS = false;
        loggerService.addLog(requestBody.getService(), requestBody.getUser(), requestBody.getComment(), requestBody.getStartDate(), requestBody.getEndDate());
        String startDate = requestBody.getStartDate() != null ? getStringFromDate(requestBody.getStartDate(),dateFormat) : "";
        String endDate = requestBody.getEndDate() != null ? getStringFromDate(requestBody.getEndDate(),dateFormat) : "";
        IS_DATE_RANGE_SYNC = checkDates(startDate, endDate) ? true : false;
        dhisMetaDataService.filterByTypeDateTime();
        Map<String, Object> mapping;
        LookupTable lookupTable;
        MappingJson mappingJson;
        Config config;
        try {
            loggerService.clearLog();
            mapping = mappingService.getMapping(requestBody.getService());

           Gson gson = new Gson();
           lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
           mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);
           config = gson.fromJson(mapping.get("config").toString(), Config.class);
        }
        catch(Exception e)
        {
            loggerService.collateLogMessage("There is An Error While Getting Mapping Information . Please Check Corresponding Program Mapping and Sync Again");
            loggerService.updateLog(requestBody.getService(), FAILED);
            throw e;
        }

        try {
            EnrollmentUtil.newActiveDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_ACTIVE_ENROLLMENT);
            EnrollmentUtil.newCompletedDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_COMPLETED_ENROLLMENT);
            EnrollmentUtil.newCancelledDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_NEW_CANCELLED_ENROLLMENT);

            EnrollmentUtil.updatedActiveDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_ACTIVE_ENROLLMENT);
            EnrollmentUtil.updatedCompletedDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_COMPLETED_ENROLLMENT);
            EnrollmentUtil.updatedCancelledDate = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_UPDATED_CANCELLED_ENROLLMENT);

            EventUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_EVENT);
        }
        catch(Exception e)
        {
            loggerService.collateLogMessage("There is An Error While Getting LastSynced Date.");
            loggerService.updateLog(requestBody.getService(), FAILED);
            throw e;
        }
        try {
            EnrollmentUtil.instanceIDEnrollmentIDMap.clear();
            Map<String,String> invalidPatients = teiService.verifyOrgUnitsForPatients(lookupTable, requestBody.getService());
            if(invalidPatients.size() > 0) {
                StringBuffer sbf = new StringBuffer();
                sbf.append("Pre-validation for sync service failed.");
                sbf.append("\nInvalid Org Unit specified for below patients.");
                invalidPatients.forEach((patientID,orgUnit)-> {
                    sbf.append("\n[Patient ID (" + patientID + ") Org Unit ID (" + orgUnit + ")] ");
                });
                sbf.append("\nUpdate patient's clinical info in Bahmni, run Bahmni MART.");
                loggerService.collateLogMessage(sbf.toString());
                loggerService.updateLog(requestBody.getService(), FAILED);
                throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "Pre-validation for sync service failed. Invalid Org Unit specified for below patients. Update patient's clinical info in Bahmni, run Bahmni MART");
            }
            teiService.getTrackedEntityInstances(requestBody.getService(), mappingJson);
            teiService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getInstance(), mappingJson.getInstance(), config.getSearchable(), config.getComparable(), startDate , endDate);
            triggerEnrollmentsSync(requestBody, lookupTable, mappingJson, config, startDate, endDate);

            if (!IS_DELTA_EXISTS) {
                loggerService.collateLogMessage(NO_DELTA_DATA);
                loggerService.updateLog(requestBody.getService(), SUCCESS);
                throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "NO DATA TO SYNC");
            } else {
                if(!checkDates(startDate,endDate))
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

    private void triggerEnrollmentsSync(DHISSyncRequestBody requestBody, LookupTable lookupTable, MappingJson mappingJson, Config config, String startDate, String endDate) throws Exception {
        TrackersHandler.clearTrackerLists();

        logger.info("=========================TEI sync Success=========================\n\n" +
                "=========================Getting enrollments for TEI=========================\n");

        teiService.getEnrollmentsForInstances(lookupTable.getEnrollments(), lookupTable.getEvent(), requestBody.getService(), startDate, endDate);

        logger.info("=========================Got enrollments for TEI=========================\n\n" +
                "=========================New Completed Enrollment Sync Started=========================\n");


        completedEnrollmentService.triggerJobForNewCompletedEnrollments(requestBody.getService(), requestBody.getUser(),lookupTable.getInstance(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment(),startDate, endDate);

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================New Completed Enrollment Sync Success=========================\n\n");
        logger.info("=========================New Cancelled Enrollment Sync Started=========================\n\n");

        cancelledEnrollmentService.triggerJobForNewCancelledEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment(), startDate, endDate);

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================New Cancelled Enrollment Sync Success=========================\n\n");

        logger.info("=========================Update Cancelled Enrollment Sync Started=========================\n\n");

        cancelledEnrollmentService.triggerJobForUpdatedCancelledEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment(), startDate, endDate);

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();

        logger.info("=========================Update Cancelled Enrollment Sync Success=========================\n\n");

        logger.info("=========================Update Complete Enrollment Sync Started=========================\n");

        completedEnrollmentService.triggerJobForUpdatedCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment(), startDate, endDate);

        TrackersHandler.clearTrackerLists();

        logger.info("=========================Update Complete Enrollment Sync Success=========================\n\n" +
                "=========================New Active Enrollment Sync Started=========================\n");

        activeEnrollmentService.triggerJobForNewActiveEnrollments(requestBody.getService(), requestBody.getUser(),lookupTable.getInstance(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment(), startDate, endDate);

        enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        TrackersHandler.clearTrackerLists();


        logger.info("=========================New Active Enrollment Sync Success=========================\n\n" +
                "=========================Update Active Enrollment Sync Started");

        activeEnrollmentService.triggerJobForUpdatedActiveEnrollments(requestBody.getService(), requestBody.getUser(),
                lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                config.getOpenLatestCompletedEnrollment(), startDate, endDate);
    }
}
