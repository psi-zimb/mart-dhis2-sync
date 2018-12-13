package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.Config;
import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import com.thoughtworks.martdhis2sync.service.ActiveEnrollmentService;
import com.thoughtworks.martdhis2sync.service.CompletedEnrollmentService;
import com.thoughtworks.martdhis2sync.service.DHISMetaDataService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.service.LoggerService.FAILED;
import static com.thoughtworks.martdhis2sync.service.LoggerService.NO_DELTA_DATA;
import static com.thoughtworks.martdhis2sync.service.LoggerService.SUCCESS;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;


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
    private ActiveEnrollmentService activeEnrollmentService;

    @Autowired
    private MarkerUtil markerUtil;

    private List<EnrollmentAPIPayLoad> enrollmentsToIgnore = new ArrayList<>();

    public static boolean IS_DELTA_EXISTS = false;

    @PutMapping(value = "/pushData")
    public void pushData(@RequestBody DHISSyncRequestBody requestBody)
            throws Exception {
        IS_DELTA_EXISTS = false;
        loggerService.addLog(requestBody.getService(), requestBody.getUser(), requestBody.getComment());

        dhisMetaDataService.getTrackedEntityInstances(requestBody.getService());
        dhisMetaDataService.filterByTypeDateTime();

        Map<String, Object> mapping = mappingService.getMapping(requestBody.getService());

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);
        Config config = gson.fromJson(mapping.get("config").toString(), Config.class);
        teiService.setSearchableAttributes(config.getSearchable());
        EnrollmentUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_ENROLLMENT);
        EventUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_EVENT);

        try {
            teiService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getInstance(), mappingJson.getInstance());

            TrackersHandler.clearTrackerLists();
            teiService.getEnrollmentsForInstances(lookupTable.getEnrollments(), lookupTable.getEvent(), requestBody.getService());
            completedEnrollmentService.triggerJobForNewCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment());

            enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
            TrackersHandler.clearTrackerLists();
            completedEnrollmentService.triggerJobForUpdatedCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                    config.getOpenLatestCompletedEnrollment());

            TrackersHandler.clearTrackerLists();
            activeEnrollmentService.triggerJobForNewActiveEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), config.getOpenLatestCompletedEnrollment());

            enrollmentsToIgnore = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
            TrackersHandler.clearTrackerLists();
            activeEnrollmentService.triggerJobForUpdatedActiveEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent(), enrollmentsToIgnore,
                    config.getOpenLatestCompletedEnrollment());

            if (!IS_DELTA_EXISTS) {
                loggerService.collateLogMessage(NO_DELTA_DATA);
                loggerService.updateLog(requestBody.getService(), SUCCESS);
                throw new Exception("NO DATA TO SYNC");
            } else {
                markerUtil.updateMarkerEntry(requestBody.getService(), CATEGORY_ENROLLMENT,
                        getStringFromDate(EnrollmentUtil.date, DATEFORMAT_WITH_24HR_TIME));
                markerUtil.updateMarkerEntry(requestBody.getService(), CATEGORY_EVENT,
                        getStringFromDate(EventUtil.date, DATEFORMAT_WITH_24HR_TIME));
            }
            loggerService.updateLog(requestBody.getService(), SUCCESS);
        } catch (SyncFailedException e) {
            loggerService.updateLog(requestBody.getService(), FAILED);
        }
    }
}
