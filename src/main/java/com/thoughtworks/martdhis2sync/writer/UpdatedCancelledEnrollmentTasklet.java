package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.service.JobService;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;

@Component
public class UpdatedCancelledEnrollmentTasklet implements Tasklet {
    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private TrackersHandler trackersHandler;

    @Autowired
    private EnrollmentResponseHandler enrollmentResponseHandler;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "UPDATE CANCELLED ENROLLMENT SYNC: ";

    private static final String ENROLLMENT_API_FORMAT = "{" +
            "\"enrollment\":\"%s\", " +
            "\"trackedEntityInstance\":\"%s\", " +
            "\"orgUnit\":\"%s\", " +
            "\"program\":\"%s\", " +
            "\"enrollmentDate\":\"%s\", " +
            "\"incidentDate\":\"%s\", " +
            "\"status\":\"%s\"" +
            "}";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String user = chunkContext.getStepContext().getJobParameters().get("user").toString();
        if (EnrollmentUtil.enrollmentsToSaveInTracker.isEmpty()) {
            return RepeatStatus.FINISHED;
        }
        List<EnrollmentAPIPayLoad> completedEnrollments = new ArrayList<>(EnrollmentUtil.enrollmentsToSaveInTracker);
        for(EnrollmentAPIPayLoad enrollment : completedEnrollments) {
            String apiBody = getApiBody(enrollment);
            ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse = syncRepository.sendEnrollmentData(URI, apiBody);
            processResponseEntity(enrollmentResponse);
        }
        updateTrackers(user);
        return RepeatStatus.FINISHED;
    }

    private void processResponseEntity(ResponseEntity<DHISEnrollmentSyncResponse> responseEntity) {
        Iterator<EnrollmentAPIPayLoad> iterator = EnrollmentUtil.enrollmentsToSaveInTracker.iterator();
        List<EnrollmentImportSummary> enrollmentImportSummaries = responseEntity.getBody().getResponse().getImportSummaries();
        if (!HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            JobService.setIS_JOB_FAILED(true);
            enrollmentResponseHandler.processCompletedSecondStepResponse(enrollmentImportSummaries, iterator, logger, LOG_PREFIX);
        }
    }

    private String getApiBody(EnrollmentAPIPayLoad enrollment) {
        StringBuilder body = new StringBuilder();
        body.append(String.format(
                ENROLLMENT_API_FORMAT,
                enrollment.getEnrollmentId(),
                enrollment.getInstanceId(),
                enrollment.getOrgUnit(),
                enrollment.getProgram(),
                enrollment.getProgramStartDate(),
                enrollment.getIncidentDate(),
                enrollment.getStatus().toUpperCase()
        ))
                .append(",");
        return String.format("{\"enrollments\":[%s]}", removeLastChar(body));
    }

    private void updateTrackers(String user) {
        trackersHandler.updateInEnrollmentTracker(user, LOG_PREFIX, logger);
        if (!EventUtil.eventsToSaveInTracker.isEmpty()) {
            trackersHandler.insertInEventTracker(user, LOG_PREFIX, logger);
        }
    }
}
