package com.thoughtworks.martdhis2sync.service;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import com.thoughtworks.martdhis2sync.dao.PatientDAO;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.MappingJson;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceInfo;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.SyncFailedException;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class TEIService {
    private final String TEI_ENROLLMENTS_URI = "/api/trackedEntityInstances?" +
            "fields=trackedEntityInstance,enrollments[program,enrollment,enrollmentDate,completedDate,status]&" +
            "program=%s&trackedEntityInstance=%s";

    @Value("${dhis2.url}")
    private String dhis2Url;

    @Value("${country.org.unit.id}")
    private String orgUnitID;

    @Autowired
    private MappingDAO mappingDAO;

    private static final String TEI_URI = "/api/trackedEntityInstances?pageSize=10000";

    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;

    @Autowired
    private JobService jobService;

    @Autowired
    private PatientDAO patientDAO;

    @Autowired
    private SyncRepository syncRepository;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "TEI Service: ";
    private static final String TEI_JOB_NAME = "Sync Tracked Entity Instance";

    public void triggerJob(String service, String user, String lookupTable, Object mappingObj, List<String> searchableAttributes, List<String> comparableAttributes)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        try {
            LinkedList<Step> steps = new LinkedList<>();
            steps.add(trackedEntityInstanceStep.get(lookupTable, service, mappingObj, searchableAttributes, comparableAttributes));
            jobService.triggerJob(service, user, TEI_JOB_NAME, steps, "");
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }

    public void getTrackedEntityInstances(String mappingName, MappingJson mappingJson) throws IOException {
        StringBuilder url = new StringBuilder();

        url.append(TEI_URI);
        url.append("&ou=");
        url.append(orgUnitID);
        url.append("&ouMode=DESCENDANTS");

        Gson gson = new Gson();
        LinkedTreeMap instanceMapping = gson.fromJson(mappingJson.getInstance().toString(), LinkedTreeMap.class);

        List<Map<String, Object>> searchableFields = mappingDAO.getSearchableFields(mappingName);

        if (searchableFields.isEmpty()) {
            TEIUtil.setTrackedEntityInstanceInfos(Collections.emptyList());
            return;
        }

        searchableFields.get(0).keySet().forEach(filter -> {
            url.append("&filter=");
            url.append(instanceMapping.get(filter));
            url.append(":IN:");

            searchableFields.forEach(searchableField -> {
                url.append(searchableField.get(filter));
                url.append(";");
            });
        });

        TEIUtil.setTrackedEntityInstanceInfos(
                syncRepository.getTrackedEntityInstances(url.toString()).getBody().getTrackedEntityInstances()
        );
    }

    public void getEnrollmentsForInstances(String enrollmentTable, String eventTable, String programName) throws Exception {
        TEIUtil.setInstancesWithEnrollments(new HashMap<>());
        List<Map<String, Object>> deltaInstanceIds = patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);
        if (!deltaInstanceIds.isEmpty()) {
            List<String> instanceIdsList = getInstanceIds(deltaInstanceIds);
            String program = deltaInstanceIds.get(0).get("program").toString();
            String instanceIds = String.join(";", instanceIdsList);
            String url = String.format(TEI_ENROLLMENTS_URI, program, instanceIds);

            ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstances = syncRepository.getTrackedEntityInstances(url);
            TEIUtil.setInstancesWithEnrollments(getMap(trackedEntityInstances.getBody().getTrackedEntityInstances()));
        }
    }

    private Map<String, List<EnrollmentDetails>> getMap(List<TrackedEntityInstanceInfo> trackedEntityInstances) {
        Map<String, List<EnrollmentDetails>> instancesMap = new HashMap<>();
        trackedEntityInstances.forEach(trackedEntityInstance -> {
            if (!trackedEntityInstance.getEnrollments().isEmpty()) {
                instancesMap.put(trackedEntityInstance.getTrackedEntityInstance(), trackedEntityInstance.getEnrollments());
            }
        });

        return instancesMap;
    }

    private List<String> getInstanceIds(List<Map<String, Object>> newEnrollmentInstances) {
        return newEnrollmentInstances
                .stream()
                .map(instanceObj -> instanceObj.get("instance_id").toString())
                .collect(Collectors.toList());
    }
}
