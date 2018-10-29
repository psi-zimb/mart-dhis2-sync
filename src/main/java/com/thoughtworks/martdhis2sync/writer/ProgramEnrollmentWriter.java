package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.Enrollment.STATUS_ACTIVE;
import static com.thoughtworks.martdhis2sync.model.Enrollment.STATUS_CANCELLED;
import static com.thoughtworks.martdhis2sync.model.Enrollment.STATUS_COMPLETED;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_ENROLLMENT;

@Component
@StepScope
public class ProgramEnrollmentWriter implements ItemWriter<Enrollment> {

    @Autowired
    private SyncRepository syncRepository;

    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Value("#{jobParameters['user']}")
    private String user;

    @Value("#{jobParameters['service']}")
    private String programName;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private MarkerUtil markerUtil;

    @Autowired
    private LoggerService loggerService;

    private Iterator<Enrollment> mapIterator;

    private List<Enrollment> enrollmentsToSaveInTrackerTable = new ArrayList<>();

    private List<Enrollment> newOrUpdatedEnrollmentsList = new ArrayList<>();

    private List<Enrollment> completedOrCancelledEnrollmentsList = new ArrayList<>();

    private List<Enrollment> createToCompleteOrCancelEnrollmentsList = new ArrayList<>();

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ResponseEntity<DHISSyncResponse> responseEntity;

    private static final String EMPTY_STRING = "\"\"";

    private static final String LOG_PREFIX = "ENROLLMENT SYNC: ";

    private static final String ENROLLMENT_API_FORMAT = "{\"enrollment\": \"%s\", " +
            "\"trackedEntityInstance\": \"%s\", " +
            "\"orgUnit\":\"%s\"," +
            "\"program\":\"%s\"," +
            "\"enrollmentDate\":\"%s\"," +
            "\"incidentDate\":\"%s\"," +
            "\"status\": \"%s\"}";

    private static final String UPDATE_QUERY = "UPDATE public.enrollment_tracker " +
            "SET status = ?, created_by = ?, date_created = ? " +
            "WHERE enrollment_id = ?";

    private static final String INSERT_QUERY = "INSERT INTO public.enrollment_tracker(" +
            "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
            "values (?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void write(List<? extends Enrollment> enrollments) throws Exception {
        PushController.IS_DELTA_EXISTS = true;
        resetLists();
        enrollments.forEach(enrollment -> {
            switch (enrollment.getStatus()) {
                case STATUS_ACTIVE:
                    newOrUpdatedEnrollmentsList.add(enrollment);
                    break;
                case STATUS_COMPLETED:
                case STATUS_CANCELLED: {
                    if (StringUtils.isEmpty(enrollment.getEnrollment_id())) {
                        createToCompleteOrCancelEnrollmentsList.add(enrollment);
                    } else {
                        completedOrCancelledEnrollmentsList.add(enrollment);
                    }
                    break;
                }
                default:
                    break;
            }
        });

        if (!createToCompleteOrCancelEnrollmentsList.isEmpty()) {
            syncNewEnrollmentsToBeMarkedCompleteOrCancel();
        }
        if (!(enrollmentsToSaveInTrackerTable.isEmpty() && completedOrCancelledEnrollmentsList.isEmpty())) {
            syncCompletedOrCancelledEnrollments();
        }
        if (!newOrUpdatedEnrollmentsList.isEmpty()) {
            syncNewOrUpdatedEnrollments();
        }
        updateMarker();
    }

    private void syncNewEnrollmentsToBeMarkedCompleteOrCancel() throws Exception {
        mapIterator = createToCompleteOrCancelEnrollmentsList.iterator();

        responseEntity = syncRepository.sendData(URI, getRequestBody(createToCompleteOrCancelEnrollmentsList));
        processResponseEntity(responseEntity);
    }

    private void syncCompletedOrCancelledEnrollments() throws Exception {
        completedOrCancelledEnrollmentsList.addAll(enrollmentsToSaveInTrackerTable);
        enrollmentsToSaveInTrackerTable.clear();
        mapIterator = completedOrCancelledEnrollmentsList.iterator();

        responseEntity = syncRepository.sendData(URI, getRequestBody(completedOrCancelledEnrollmentsList));
        processResponseEntity(responseEntity);
    }

    private void syncNewOrUpdatedEnrollments() throws Exception {
        enrollmentsToSaveInTrackerTable.clear();
        mapIterator = newOrUpdatedEnrollmentsList.iterator();

        responseEntity = syncRepository.sendData(URI, getRequestBody(newOrUpdatedEnrollmentsList));
        processResponseEntity(responseEntity);
    }

    private String getRequestBody(List<Enrollment> list) {
        StringBuilder body = new StringBuilder("{\"enrollments\":[");
        list.forEach(enrollment -> {
            String enr = String.format(
                    ENROLLMENT_API_FORMAT,
                    enrollment.getEnrollment_id(), enrollment.getInstance_id(),
                    enrollment.getOrgUnit(), enrollment.getProgram(),
                    enrollment.getProgram_start_date(), enrollment.getIncident_date(),
                    enrollment.getStatus().toUpperCase());
            body.append(enr).append(",");
        });
        body.replace(body.length() - 1, body.length(), "]}");
        return body.toString();
    }

    private void processResponseEntity(ResponseEntity<DHISSyncResponse> responseEntity) throws Exception {
        if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            processImportSummaries(responseEntity.getBody().getResponse().getImportSummaries());
            updateTracker();
        } else {
            processErrorResponse(responseEntity.getBody().getResponse().getImportSummaries());
            updateTracker();
            throw new Exception();
        }
    }

    private void processErrorResponse(List<ImportSummary> importSummaries) {
        for (ImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                if (mapIterator.hasNext()) mapIterator.next();
                logger.error(LOG_PREFIX + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                if (mapIterator.hasNext()) mapIterator.next();
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
            } else {
                processImportSummaries(Collections.singletonList(importSummary));
            }
        }
    }

    private void processImportSummaries(List<ImportSummary> importSummaries) {
        importSummaries.forEach(importSummary -> {
            if (isImported(importSummary)) {
                while (mapIterator.hasNext()) {
                    Enrollment enrollment = mapIterator.next();
                    if (StringUtils.isEmpty(enrollment.getEnrollment_id())) {
                        enrollment.setEnrollment_id(importSummary.getReference());
                        enrollmentsToSaveInTrackerTable.add(enrollment);
                        break;
                    }
                }
            }
        });
    }

    private boolean isIgnored(ImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isConflicted(ImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !importSummary.getConflicts().isEmpty();
    }

    private boolean isImported(ImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(importSummary.getStatus()) && importSummary.getImportCount().getImported() == 1;
    }

    private void updateTracker() {

        int recordsCreated;
        try {
            if (!enrollmentsToSaveInTrackerTable.isEmpty()) {
                recordsCreated = getInsertCount(INSERT_QUERY);
                logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Enrollment UIDs.");
            } else if (!completedOrCancelledEnrollmentsList.isEmpty()) {
                recordsCreated = getUpdateCount(UPDATE_QUERY);
                logger.info(LOG_PREFIX + "Successfully updated " + recordsCreated + " Enrollments' status.");
            }
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting Program Enrollment UIDs:" + e.getMessage());
            e.printStackTrace();
        }
    }

    private int getUpdateCount(String sqlQuery) throws SQLException {
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                updateCount = 0;
                for (Enrollment enrollment : completedOrCancelledEnrollmentsList) {
                    ps.setString(1, enrollment.getStatus());
                    ps.setString(2, user);
                    ps.setTimestamp(3, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    ps.setString(4, getUnquotedString(enrollment.getEnrollment_id()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        completedOrCancelledEnrollmentsList.clear();
        return updateCount;
    }

    private int getInsertCount(String sqlQuery) throws SQLException {
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                updateCount = 0;
                for (Enrollment enrollment : enrollmentsToSaveInTrackerTable) {
                    ps.setString(1, enrollment.getEnrollment_id());
                    ps.setString(2, enrollment.getInstance_id());
                    ps.setString(3, enrollment.getProgram());
                    ps.setString(4, enrollment.getStatus());
                    ps.setString(5, enrollment.getProgram_unique_id());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, CATEGORY_ENROLLMENT,
                getStringFromDate(EnrollmentUtil.date, DATEFORMAT_WITH_24HR_TIME));
    }

    private void resetLists() {
        createToCompleteOrCancelEnrollmentsList.clear();
        enrollmentsToSaveInTrackerTable.clear();
        newOrUpdatedEnrollmentsList.clear();
        enrollmentsToSaveInTrackerTable.clear();
    }
}
