package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.DHISSyncRequestBody;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import com.thoughtworks.martdhis2sync.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.SyncFailedException;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeChars;

@RestController
public class PushController {

    @Autowired
    private MappingService mappingService;

    @Autowired
    private TEIService teiService;

    @Autowired
    private ProgramEnrollmentService programEnrollmentService;

    @Autowired
    private EventService eventService;

    @Autowired
    private LoggerService loggerService;

    public static boolean IS_DELTA_EXISTS = false;
    public static StringBuilder failedReason = new StringBuilder();
    private static final String SUCCESS = "success";
    private static final String FAILED = "failed";
    private static final int COMMA_AND_SPACE_SIZE = 2;

    @PutMapping(value = "/pushData")
    public void pushData(@RequestBody DHISSyncRequestBody requestBody)
            throws Exception {
        IS_DELTA_EXISTS = false;
        failedReason = new StringBuilder();
        loggerService.addLog(requestBody.getService(), requestBody.getUser(), requestBody.getComment());

        Map<String, Object> mapping = mappingService.getMapping(requestBody.getService());

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        try {
            teiService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getInstance(), mappingJson.getInstance());
            programEnrollmentService.triggerJob(requestBody.getService(),
                    requestBody.getUser(), lookupTable.getEnrollments());
            eventService.triggerJob(requestBody.getService(), requestBody.getUser(),
                    lookupTable.getEvent(), mappingJson.getEvent(), lookupTable.getEnrollments());
            if (!IS_DELTA_EXISTS) {
                loggerService.updateLog(requestBody.getService(), FAILED, "No data to sync");
                throw new Exception("NO DATA TO SYNC");
            }
            loggerService.updateLog(requestBody.getService(), SUCCESS, "");
        } catch (SyncFailedException e) {
            loggerService.updateLog(requestBody.getService(), FAILED, removeChars(failedReason, COMMA_AND_SPACE_SIZE));
        }
    }
}
