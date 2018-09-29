package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import com.thoughtworks.martdhis2sync.service.EventService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.ProgramEnrollmentService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    public void pushData(@RequestParam String service, @RequestParam String user)
            throws Exception {
        IS_DELTA_EXISTS = false;
        failedReason = new StringBuilder();
        loggerService.addLog(service, user, "Comments");

        Map<String, Object> mapping = mappingService.getMapping(service);

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        try {
            teiService.triggerJob(service, user, lookupTable.getInstance(), mappingJson.getInstance());
            programEnrollmentService.triggerJob(service, user, lookupTable.getEnrollments());
            eventService.triggerJob(service, user, lookupTable.getEvent(), mappingJson.getEvent(), lookupTable.getEnrollments());
            loggerService.updateLog(service, SUCCESS, "");
            if(!IS_DELTA_EXISTS) {
                throw new Exception("NO DATA TO SYNC");
            }
        } catch (SyncFailedException e) {
            loggerService.updateLog(service, FAILED, removeChars(failedReason, COMMA_AND_SPACE_SIZE));
        }
    }
}
