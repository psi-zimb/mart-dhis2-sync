package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.Conflict;
import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({BatchUtil.class})
public class NewCompletedEnrollmentTaskletTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private DataSource dataSource;

    @Mock
    private LoggerService loggerService;

    @Mock
    private Logger logger;

    @Mock
    private ResponseEntity<DHISEnrollmentSyncResponse> responseEntity;

    @Mock
    private DHISEnrollmentSyncResponse syncResponse;

    @Mock
    private EnrollmentResponse response;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private StepContribution stepContribution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private StepContext stepContext;

    private NewCompletedEnrollmentTasklet tasklet;

    private String uri = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    private EnrollmentAPIPayLoad payLoad1;
    private EnrollmentAPIPayLoad payLoad2;
    private String enrId1 = "enrId1";
    private String enrId2 = "enrId2";
    private String instanceId1 = "instance1";
    private String instanceId2 = "instance2";
    private String enrDate = "2018-10-13";
    private String date = "2018-10-12 13:00:00";

    @Before
    public void setUp() throws Exception {
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, "1", enrId1);
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, "2", enrId2);
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("user", "superman");

        tasklet = new NewCompletedEnrollmentTasklet();

        setValuesForMemberFields(tasklet, "syncRepository", syncRepository);
        setValuesForMemberFields(tasklet, "loggerService", loggerService);
        setValuesForMemberFields(tasklet, "logger", logger);
        setValuesForMemberFields(tasklet, "dataSource", dataSource);

        mockStatic(BatchUtil.class);
        when(BatchUtil.GetUTCDateTimeAsString()).thenReturn(date);
        when(BatchUtil.removeLastChar(any())).thenReturn(getEnrollment(payLoad1));

        when(responseEntity.getBody()).thenReturn(syncResponse);
        when(syncResponse.getResponse()).thenReturn(response);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
        when(stepContext.getJobParameters()).thenReturn(jobParams);
    }

    @Test
    public void shouldUpdateTrackersAfterSync() throws Exception {
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        getEnrollment(payLoad1) +
                    "]" +
                "}";

        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );
        String enrSql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        String evnSql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(enrSql)).thenReturn(preparedStatement);
        when(connection.prepareStatement(evnSql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);
        EnrollmentUtil.enrollmentsToSaveInTracker = Collections.singletonList(payLoad1);
        EventUtil.eventsToSaveInTracker = getEventTrackers();

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(dataSource, times(2)).getConnection();
        verify(connection, times(1)).prepareStatement(enrSql);
        verify(preparedStatement, times(1)).setString(1, enrId1);
        verify(preparedStatement, times(3)).setString(2, instanceId1);
        verify(preparedStatement, times(3)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(2)).setString(5, "1");
        verify(preparedStatement, times(3)).setString(6, "superman");
        verify(preparedStatement, times(3)).executeUpdate();
        verify(connection, times(1)).prepareStatement(evnSql);
        verify(preparedStatement, times(1)).setString(1, "eventId1");
        verify(preparedStatement, times(2)).setString(4, "dkjfErjA");
        verify(preparedStatement, times(1)).setString(1, "eventId2");
        verify(preparedStatement, times(1)).setString(5, "2");
    }

    @Test
    public void shouldNotCallEventTrackerUpdateWhenEventsAreNotThere() throws Exception {
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        getEnrollment(payLoad1) +
                    "]" +
                "}";

        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );
        String enrSql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        String evnSql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(enrSql)).thenReturn(preparedStatement);
        when(connection.prepareStatement(evnSql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);
        EnrollmentUtil.enrollmentsToSaveInTracker = Collections.singletonList(payLoad1);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(dataSource, times(2)).getConnection();
        verify(connection, times(1)).prepareStatement(enrSql);
        verify(preparedStatement, times(1)).setString(1, enrId1);
        verify(preparedStatement, times(1)).setString(2, instanceId1);
        verify(preparedStatement, times(1)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(6, "superman");
        verify(preparedStatement, times(1)).executeUpdate();
        verify(connection, times(1)).prepareStatement(evnSql);
        verify(preparedStatement, times(0)).setString(1, "eventId1");
        verify(preparedStatement, times(0)).setString(4, "dkjfErjA");
        verify(preparedStatement, times(0)).setString(1, "eventId2");
        verify(preparedStatement, times(0)).setString(5, "2");
    }

    @Test
    public void shouldChangeTheStatusToActiveWhenThereIsErrorResponseForThatEnrollment() throws Exception {
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        getEnrollment(payLoad1) +
                        "," +
                        getEnrollment(payLoad2) +
                    "]" +
                "}";

        String description = "Event.programStage does not point to a valid programStage, and program is multi-stage: " +
                "wrong program stage";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        description, new ArrayList<>(), enrId2, null
                )
        );
        String enrSql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        String evnSql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(BatchUtil.removeLastChar(any())).thenReturn(getEnrollment(payLoad1) + "," + getEnrollment(payLoad2));
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(enrSql)).thenReturn(preparedStatement);
        when(connection.prepareStatement(evnSql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);
        EnrollmentUtil.enrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(dataSource, times(2)).getConnection();
        verify(connection, times(1)).prepareStatement(enrSql);
        verify(preparedStatement, times(1)).setString(1, enrId1);
        verify(preparedStatement, times(1)).setString(1, enrId2);
        verify(preparedStatement, times(1)).setString(2, instanceId1);
        verify(preparedStatement, times(1)).setString(2, instanceId2);
        verify(preparedStatement, times(2)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "ACTIVE");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(5, "2");
        verify(preparedStatement, times(2)).setString(6, "superman");
        verify(preparedStatement, times(2)).executeUpdate();
        verify(connection, times(1)).prepareStatement(evnSql);
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: " + description);
        verify(loggerService, times(1)).collateLogMessage(description);
    }

    @Test
    public void shouldCollateConflictMessageWhenThereIsConflict() throws Exception {
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        getEnrollment(payLoad1) +
                        "," +
                        getEnrollment(payLoad2) +
                    "]" +
                "}";

        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        null, Collections.singletonList(new Conflict(Conflict.CONFLICT_OBJ_ENROLLMENT_DATE, "Enrollment date should not be future date")),
                        enrId2, null
                )
        );
        String enrSql = "INSERT INTO public.enrollment_tracker(" +
                "enrollment_id, instance_id, program, status, program_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        String evnSql = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(BatchUtil.removeLastChar(any())).thenReturn(getEnrollment(payLoad1) + "," + getEnrollment(payLoad2));
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(enrSql)).thenReturn(preparedStatement);
        when(connection.prepareStatement(evnSql)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);
        EnrollmentUtil.enrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(dataSource, times(2)).getConnection();
        verify(connection, times(1)).prepareStatement(enrSql);
        verify(preparedStatement, times(1)).setString(1, enrId1);
        verify(preparedStatement, times(1)).setString(1, enrId2);
        verify(preparedStatement, times(1)).setString(2, instanceId1);
        verify(preparedStatement, times(1)).setString(2, instanceId2);
        verify(preparedStatement, times(2)).setString(3, "xhjKKwoq");
        verify(preparedStatement, times(1)).setString(4, "ACTIVE");
        verify(preparedStatement, times(1)).setString(4, "COMPLETED");
        verify(preparedStatement, times(1)).setString(5, "1");
        verify(preparedStatement, times(1)).setString(5, "2");
        verify(preparedStatement, times(2)).setString(6, "superman");
        verify(preparedStatement, times(2)).executeUpdate();
        verify(connection, times(1)).prepareStatement(evnSql);
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: " +
                Conflict.CONFLICT_OBJ_ENROLLMENT_DATE + ": " + "Enrollment date should not be future date");
        verify(loggerService, times(1)).collateLogMessage(Conflict.CONFLICT_OBJ_ENROLLMENT_DATE + ": " + "Enrollment date should not be future date");
    }

    @Test
    public void shouldLogMessageWhenFailedToInsertIntoTrackers() throws Exception {
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        getEnrollment(payLoad1) +
                    "]" +
                "}";

        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(dataSource.getConnection()).thenThrow(new SQLException("can't get database connection"));
        EnrollmentUtil.enrollmentsToSaveInTracker = Collections.singletonList(payLoad1);
        EventUtil.eventsToSaveInTracker = getEventTrackers();

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(dataSource, times(2)).getConnection();
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: Exception occurred " +
                "while inserting Program Enrollment UIDs:can't get database connection");
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: Exception occurred " +
                "while inserting Event UIDs:can't get database connection");
    }

    @After
    public void tearDown() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker = new ArrayList<>();
        EventUtil.eventsToSaveInTracker = new ArrayList<>();
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, String programUniqueId, String enrId) {
        return new EnrollmentAPIPayLoad(
                enrId,
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                "COMPLETED",
                programUniqueId,
                null
        );
    }

    private String getEnrollment(EnrollmentAPIPayLoad payLoad) {
        return String.format("{\"enrollment\":\"%s\", " +
                        "\"trackedEntityInstance\":\"%s\", " +
                        "\"orgUnit\":\"%s\", " +
                        "\"program\":\"%s\", " +
                        "\"enrollmentDate\":\"%s\", " +
                        "\"incidentDate\":\"%s\", " +
                        "\"status\":\"COMPLETED\"}",
                payLoad.getEnrollmentId(),
                payLoad.getInstanceId(),
                payLoad.getOrgUnit(),
                payLoad.getProgram(),
                payLoad.getProgramStartDate(),
                payLoad.getIncidentDate()
        );
    }

    private List<EventTracker> getEventTrackers() {
        EventTracker eventTracker1 = new EventTracker("eventId1", instanceId1, "xhjKKwoq", "1", "dkjfErjA");
        EventTracker eventTracker2 = new EventTracker("eventId2", instanceId1, "xhjKKwoq", "2", "dkjfErjA");

        return Arrays.asList(eventTracker1, eventTracker2);
    }
}
