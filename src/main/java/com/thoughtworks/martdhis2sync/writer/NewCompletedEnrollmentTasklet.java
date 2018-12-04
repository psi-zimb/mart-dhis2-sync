package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
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
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.GetUTCDateTimeAsString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;

@Component
public class NewCompletedEnrollmentTasklet implements Tasklet {
    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private LoggerService loggerService;

    @Autowired
    private DataSource dataSource;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "NEW COMPLETED ENROLLMENT SYNC: ";

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
        String apiBody = getApiBody();
        ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse = syncRepository.sendEnrollmentData(URI, apiBody);
        processResponseEntity(enrollmentResponse);
        updateTrackers(user);
        return RepeatStatus.FINISHED;
    }

    private void processResponseEntity(ResponseEntity<DHISEnrollmentSyncResponse> responseEntity) {
        Iterator<EnrollmentAPIPayLoad> iterator = EnrollmentUtil.enrollmentsToSaveInTracker.iterator();
        List<EnrollmentImportSummary> enrollmentImportSummaries = responseEntity.getBody().getResponse().getImportSummaries();
        if (!HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            processEnrollmentErrorResponse(enrollmentImportSummaries, iterator);
        }
    }

    private void processEnrollmentErrorResponse(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator) {
        for (EnrollmentImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                EnrollmentAPIPayLoad payLoad = payLoadIterator.next();
                payLoad.setStatus(Enrollment.STATUS_ACTIVE);
                logger.error(LOG_PREFIX + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                EnrollmentAPIPayLoad payLoad = payLoadIterator.next();
                payLoad.setStatus(Enrollment.STATUS_ACTIVE);
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
            } else {
                payLoadIterator.next();
            }
        }
    }


    private boolean isIgnored(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isConflicted(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !importSummary.getConflicts().isEmpty();
    }

    private String getApiBody() {
        StringBuilder body = new StringBuilder();
        EnrollmentUtil.enrollmentsToSaveInTracker.forEach(enrollment -> {
            body
                    .append(String.format(
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
        });

        return String.format("{\"enrollments\":[%s]}", removeLastChar(body));
    }

    private void updateTrackers(String user) {
        int recordsCreated;
        try {
            recordsCreated = insertInEnrollmentTracker(user);
            logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Enrollment UIDs.");
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting Program Enrollment UIDs:" + e.getMessage());
            e.printStackTrace();
        }

        try {
            recordsCreated = insertInEventTracker(user);
            logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Event UIDs.");
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting Event UIDs:" + e.getMessage());
            e.printStackTrace();
        }
    }

    private int insertInEventTracker(String user) throws SQLException {
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                updateCount = 0;
                for (EventTracker eventTracker : EventUtil.eventsToSaveInTracker) {
                    ps.setString(1, eventTracker.getEventId());
                    ps.setString(2, eventTracker.getInstanceId());
                    ps.setString(3, eventTracker.getProgram());
                    ps.setString(4, eventTracker.getProgramStage());
                    ps.setString(5, eventTracker.getEventUniqueId());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    private int insertInEnrollmentTracker(String user) throws SQLException {
        String sql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                updateCount = 0;
                for (EnrollmentAPIPayLoad enrollment : EnrollmentUtil.enrollmentsToSaveInTracker) {
                    ps.setString(1, enrollment.getEnrollmentId());
                    ps.setString(2, enrollment.getInstanceId());
                    ps.setString(3, enrollment.getProgram());
                    ps.setString(4, enrollment.getStatus());
                    ps.setString(5, enrollment.getProgramUniqueId());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }
}
