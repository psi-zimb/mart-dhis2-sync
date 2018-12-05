package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.Config;
import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import com.thoughtworks.martdhis2sync.service.*;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.SyncFailedException;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.service.LoggerService.*;
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
    private MarkerUtil markerUtil;

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
        teiService.setSearchableAttributes(gson.fromJson(mapping.get("config").toString(), Config.class).getSearchable());
        EnrollmentUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_ENROLLMENT);
        EventUtil.date = markerUtil.getLastSyncedDate(requestBody.getService(), CATEGORY_EVENT);

        try {
            teiService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getInstance(), mappingJson.getInstance());
            completedEnrollmentService.triggerJobForNewCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent());
            completedEnrollmentService.triggerJobForUpdatedCompletedEnrollments(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEnrollments(), lookupTable.getEvent(), mappingJson.getEvent());
            if (!IS_DELTA_EXISTS) {
                loggerService.collateLogMessage(NO_DELTA_DATA);
                loggerService.updateLog(requestBody.getService(), SUCCESS);
                throw new Exception("NO DATA TO SYNC");
            }
            loggerService.updateLog(requestBody.getService(), SUCCESS);
        } catch (SyncFailedException e) {
            loggerService.updateLog(requestBody.getService(), FAILED);
        }
    }
}
