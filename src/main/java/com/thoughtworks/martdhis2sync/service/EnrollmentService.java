package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.thoughtworks.martdhis2sync.controller.PushController.suggestedRemovableDuplicatesSet;

@Component
public class EnrollmentService {
    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";
    private static final String TEI_URI = "/api/trackedEntityInstances?pageSize=10000";
    private static final String ENROLLMENT_API_FORMAT = "{" +
            "\"enrollment\":\"%s\", " +
            "\"trackedEntityInstance\":\"%s\", " +
            "\"orgUnit\":\"%s\", " +
            "\"program\":\"%s\", " +
            "\"enrollmentDate\":\"%s\", " +
            "\"incidentDate\":\"%s\", " +
            "\"status\":\"%s\", " +
            "\"events\":[%s]" +
            "}";
    @Autowired
    protected SyncRepository syncRepository;
    @Autowired
    protected EnrollmentResponseHandler enrollmentResponseHandler;
    @Autowired
    protected EventResponseHandler eventResponseHandler;
    @Value("${tracked.entity.preferred.program}")
    private String preferredProgramToAutoEnroll;
    @Value("${country.org.unit.id.for.patient.data.duplication.check}")
    private String orgUnitID;
    @Value("${tracked.entity.attribute.uic}")
    private String uicAttributeId;

    public void enrollSingleClientInstanceToPreferredProgram(TrackedEntityInstanceInfo trackedEntityInstanceInfo) throws Exception {
        String apiBody = getAPIBody(trackedEntityInstanceInfo);
        ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse;
        try {
            enrollmentResponse = syncRepository.sendEnrollmentData(URI, apiBody);
            if (HttpStatus.OK.equals(enrollmentResponse.getStatusCode())) {
                StringBuilder url = new StringBuilder();
                url.append(TEI_URI).append("&fields=trackedEntityInstance,enrollments[program,enrollment,en" +
                        "rollmentDate,completedDate,status]").append("&ou=").append(orgUnitID).append("&ouMode=DESCENDANTS").append("&program=").append(preferredProgramToAutoEnroll).append("&trackedEntityInstance=").append(trackedEntityInstanceInfo.getTrackedEntityInstance());
                ResponseEntity<TrackedEntityInstanceResponse> response = syncRepository.getTrackedEntityInstances(url.toString());
                TEIUtil.setInstancesWithEnrollments(getMap(Collections.singletonList(response.getBody().getTrackedEntityInstances().get(0)), preferredProgramToAutoEnroll));
            }else if(enrollmentResponse.getStatusCode().equals(HttpStatus.CONFLICT)){
                suggestedRemovableDuplicatesSet.add(trackedEntityInstanceInfo.getAttributeValue(uicAttributeId));
            }
        } catch (Exception e) {
            JobService.setIS_JOB_FAILED(true);
            throw new Exception();
        }
    }

    public boolean isTrackedEntityInstanceEnrolledToPreferredProgrsamForInstanceId(String instanceId) throws Exception {
        StringBuilder url = new StringBuilder();
        url.append(TEI_URI).append("&fields=trackedEntityInstance,enrollments[program,enrollment,enrollmentDate,completedDate,status]").append("&program=").append(preferredProgramToAutoEnroll).append("&trackedEntityInstance=").append(instanceId);

        ResponseEntity<TrackedEntityInstanceResponse> response = syncRepository.getTrackedEntityInstances(url.toString());

        if (response.getStatusCode().is2xxSuccessful()) {
            List<EnrollmentDetails> enrollmentDetails = response.getBody().getTrackedEntityInstances().get(0).getEnrollments().stream().filter(enrollmentInfo -> enrollmentInfo.getProgram().equals(preferredProgramToAutoEnroll)).collect(Collectors.toList());
            return !enrollmentDetails.isEmpty();
        }
        return false;
    }

    private Map<String, List<EnrollmentDetails>> getMap(List<TrackedEntityInstanceInfo> trackedEntityInstances, String currentProgram) {
        Map<String, List<EnrollmentDetails>> instancesMap = new HashMap<>();
        trackedEntityInstances.forEach(trackedEntityInstance -> {
            if (!trackedEntityInstance.getEnrollments().isEmpty()) {
                instancesMap.put(trackedEntityInstance.getTrackedEntityInstance(), filterProgramsBy(currentProgram, trackedEntityInstance.getEnrollments()));
            }
        });
        return instancesMap;
    }

    private List<EnrollmentDetails> filterProgramsBy(String program, List<EnrollmentDetails> allEnrollments) {
        return allEnrollments.stream().filter(enrollment -> program.equals(enrollment.getProgram())).collect(Collectors.toList());
    }

    private String getAPIBody(TrackedEntityInstanceInfo trackedEntityInstanceInfo) {
        StringBuilder body = new StringBuilder("");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        simpleDateFormat.format(Date.from(Instant.now()));
        body.append(String.format(
                ENROLLMENT_API_FORMAT,
                "",
                trackedEntityInstanceInfo.getTrackedEntityInstance(),
                trackedEntityInstanceInfo.getOrgUnit(),
                preferredProgramToAutoEnroll,
                simpleDateFormat.format(Date.from(Instant.now())),
                simpleDateFormat.format(Date.from(Instant.now())),
                EnrollmentAPIPayLoad.STATUS_ACTIVE,
                ""
        ));


        return String.format("{\"enrollments\":[%s]}", body);
    }
}
