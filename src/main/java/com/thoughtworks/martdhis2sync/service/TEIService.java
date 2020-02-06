package com.thoughtworks.martdhis2sync.service;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import com.thoughtworks.martdhis2sync.dao.PatientDAO;
import com.thoughtworks.martdhis2sync.model.*;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
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

    private final String PATIENTS_WITH_INVALID_ORG_UNIT_QUERY = "select \"Patient_Identifier\",\"OrgUnit\" from %s it " +
            "where \"OrgUnit\" is null or " +
            "\"OrgUnit\" not in (select orgunit from  orgunit_tracker ot)";

    @Value("${country.org.unit.id.for.patient.data.duplication.check}")
    private String orgUnitID;

    @Value("${tracked.entity.filter.uri.limit}")
    private int TEI_FILTER_URI_LIMIT;

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

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;


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
        List<TrackedEntityInstanceInfo> allTEIInfos = new ArrayList<>();
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

        separateSearchFieldsBasedOnFilterLimit(searchableFields).forEach(searchableFieldGroup -> {
            StringBuilder uri = new StringBuilder();
            searchableFieldGroup.get(0).keySet().forEach(filter -> {
                uri.append("&filter=");
                uri.append(instanceMapping.get(filter));
                uri.append(":IN:");

                searchableFieldGroup.forEach(searchableField -> {
                    uri.append(searchableField.get(filter));
                    uri.append(";");
                });
            });
            uri.append("&includeAllAttributes=true");
            ResponseEntity<TrackedEntityInstanceResponse> response = syncRepository.getTrackedEntityInstances(url.toString() + uri);
            if(response != null && response.getBody() != null) {
                allTEIInfos.addAll(response.getBody().getTrackedEntityInstances());
            }
        });

        TEIUtil.setTrackedEntityInstanceInfos(allTEIInfos);
        logger.info("TEIUtil.getTrackedEntityInstanceInfos().size(): " + TEIUtil.getTrackedEntityInstanceInfos().size());
    }

    public void getEnrollmentsForInstances(String enrollmentTable, String eventTable, String programName) throws Exception {
        logger.info("Enrollment Table is " + enrollmentTable);
        logger.info("Event Table is " + eventTable);
        logger.info("Program name is " + programName);

        TEIUtil.setInstancesWithEnrollments(new HashMap<>());
        List<Map<String, Object>> deltaInstanceIds = patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);
        if (!deltaInstanceIds.isEmpty()) {
            List<String> instanceIdsList = getInstanceIds(deltaInstanceIds);
            String program = deltaInstanceIds.get(0).get("program").toString();
            logger.info("instanceIdsList : " + instanceIdsList.size());
            int lowerLimit = 0;
            int upperLimit = TEI_FILTER_URI_LIMIT;
            List<TrackedEntityInstanceInfo> result = new ArrayList<>();
            while(lowerLimit < instanceIdsList.size()) {
                if(upperLimit > instanceIdsList.size()) {
                    upperLimit = instanceIdsList.size();
                }
                logger.info("Lower : " + lowerLimit + " Upper " + upperLimit);
                List<String> subInstanceIds  =  instanceIdsList.subList(lowerLimit , upperLimit);
                lowerLimit = upperLimit;
                upperLimit += TEI_FILTER_URI_LIMIT;

                String instanceIds = String.join(";", subInstanceIds);
                String url = String.format(TEI_ENROLLMENTS_URI, program, instanceIds);

                ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstances = syncRepository.getTrackedEntityInstances(url);
                result.addAll(trackedEntityInstances.getBody().getTrackedEntityInstances());

            }
            TEIUtil.setInstancesWithEnrollments(getMap(result, program));
            logger.info("Results Size " + result.size());
        }
    }

    private Map<String, List<EnrollmentDetails>> getMap(List<TrackedEntityInstanceInfo> trackedEntityInstances, String currentProgram) {
        Map<String, List<EnrollmentDetails>> instancesMap = new HashMap<>();
        trackedEntityInstances.forEach(trackedEntityInstance -> {
            if (!trackedEntityInstance.getEnrollments().isEmpty()) {
                instancesMap.put(trackedEntityInstance.getTrackedEntityInstance(), filterProgramsBy(currentProgram, trackedEntityInstance.getEnrollments()));
            }
        });
        logger.info("instancesMap " + instancesMap);
        return instancesMap;
    }

    private List<EnrollmentDetails> filterProgramsBy(String program, List<EnrollmentDetails> allEnrollments) {
        return allEnrollments.stream().filter(enrollment -> program.equals(enrollment.getProgram())).collect(Collectors.toList());
    }

    private List<String> getInstanceIds(List<Map<String, Object>> newEnrollmentInstances) {
        return newEnrollmentInstances
                .stream()
                .map(instanceObj -> instanceObj.get("instance_id").toString())
                .collect(Collectors.toList());
    }

    private List<List<Map<String, Object>>> separateSearchFieldsBasedOnFilterLimit(List<Map<String, Object>> searchableFields) {
        List<List<Map<String, Object>>> result = new ArrayList<>();
        List<Map<String, Object>> searchableGroup = new ArrayList<>();
        searchableFields.forEach(searchableField -> {
            if (searchableGroup.size() >= TEI_FILTER_URI_LIMIT) {
                result.add(new ArrayList<>(searchableGroup));
                searchableGroup.clear();
            }
            searchableGroup.add(searchableField);
        });

        if (!searchableGroup.isEmpty()) {
            result.add(searchableGroup);
        }

        return result;
    }

    public Map<String,String> verifyOrgUnitsForPatients(String instanceTable) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(String.format(PATIENTS_WITH_INVALID_ORG_UNIT_QUERY, instanceTable));
        Map<String,String> invalidPatients = new HashMap<>();
        rows.forEach(row -> {
            String patientID = (String)row.get("Patient_Identifier");
            String orgUnit = (String)row.get("OrgUnit");
            invalidPatients.put(patientID,orgUnit);
        });
        return invalidPatients;
    }
}
