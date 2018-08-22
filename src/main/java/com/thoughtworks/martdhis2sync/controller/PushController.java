package com.thoughtworks.martdhis2sync.controller;

import com.google.gson.Gson;
import com.thoughtworks.martdhis2sync.service.MappingService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.util.LookupTable;
import com.thoughtworks.martdhis2sync.util.MappingJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class PushController {

    @Autowired
    private MappingService mappingService;

    @Autowired
    private TEIService teiService;

    @GetMapping(value = "/pushData")
    public Map<String, String> pushData(@RequestParam String service) {

        Map<String, Object> mapping = mappingService.getMapping(service);

        Gson gson = new Gson();
        LookupTable lookupTable = gson.fromJson(mapping.get("lookup_table").toString(), LookupTable.class);
        MappingJson mappingJson = gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class);

        teiService.triggerJob(service, lookupTable.getInstance(), mappingJson.getInstance());

        Map<String, String> result = new HashMap<>();
        result.put("Job Status", "Executed");
        return result;
    }
}
