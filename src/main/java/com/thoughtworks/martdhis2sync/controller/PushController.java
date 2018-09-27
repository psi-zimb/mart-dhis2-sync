package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.service.EventService;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.ProgramEnrollmentService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.model.LookupTable;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.SyncFailedException;
import java.util.HashMap;
import java.util.Map;

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

    @PutMapping(value = "/pushData")
    public Map<String, String> pushData(@RequestParam String service, @RequestParam String user)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException {

        Map<String, Object> mapping = mappingService.getMapping(service);

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        try {
            teiService.triggerJob(service, user, lookupTable.getInstance(), mappingJson.getInstance());
            programEnrollmentService.triggerJob(service, user, lookupTable.getEnrollments());
            eventService.triggerJob(service, user, lookupTable.getEvent(), mappingJson.getEvent(), lookupTable.getEnrollments());
        } catch (SyncFailedException ignored) {
        }

        Map<String, String> result = new HashMap<>();
        result.put("Job Status", "Executed");
        return result;
    }
}
