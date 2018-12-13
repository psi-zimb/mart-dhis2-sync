package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.PatientDAO;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstance;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.step.TrackedEntityInstanceStep;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.SyncFailedException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class TEIService {
    private final String TEI_ENROLLMENTS_URI = "/api/trackedEntityInstances?" +
            "fields=trackedEntityInstance,enrollments[program,enrollment,enrollmentDate,completedDate,status]&" +
            "program=%s&trackedEntityInstance=%s";

    @Autowired
    private TrackedEntityInstanceStep trackedEntityInstanceStep;

    @Autowired
    private JobService jobService;

    @Autowired
    private PatientDAO patientDAO;

    @Autowired
    private SyncRepository syncRepository;

    @Setter
    private List<String> searchableAttributes;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "TEI Service: ";
    private static final String TEI_JOB_NAME = "Sync Tracked Entity Instance";

    public void triggerJob(String service, String user, String lookupTable, Object mappingObj)
            throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, SyncFailedException {

        trackedEntityInstanceStep.setSearchableAttributes(searchableAttributes);

        try {
            LinkedList<Step> steps = new LinkedList<>();
            steps.add(trackedEntityInstanceStep.get(lookupTable, service, mappingObj));
            jobService.triggerJob(service, user, TEI_JOB_NAME, steps, "");
        } catch (Exception e) {
            logger.error(LOG_PREFIX + e.getMessage());
            throw e;
        }
    }

    public void getEnrollmentsForInstances(String enrollmentTable, String eventTable, String programName) throws Exception {
        TEIUtil.setInstancesWithEnrollments(new HashMap<>());
        List<Map<String, Object>> deltaInstanceIds = patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);
        if (deltaInstanceIds.size() > 0) {
            List<String> instanceIdsList = getInstanceIds(deltaInstanceIds);
            String program = deltaInstanceIds.get(0).get("program").toString();
            String instanceIds = String.join(";", instanceIdsList);
            String url = String.format(TEI_ENROLLMENTS_URI, program, instanceIds);

            ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstances = syncRepository.getTrackedEntityInstances(url);
            TEIUtil.setInstancesWithEnrollments(getMap(trackedEntityInstances.getBody().getTrackedEntityInstances()));
        }
    }

    private Map<String, List<EnrollmentDetails>> getMap(List<TrackedEntityInstance> trackedEntityInstances) {
        Map<String, List<EnrollmentDetails>> instancesMap = new HashMap<>();
        trackedEntityInstances.forEach(trackedEntityInstance -> {
            if (trackedEntityInstance.getEnrollments().size() > 0) {
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
