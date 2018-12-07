package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.trackerHandler.TrackersHandler;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.sql.SQLException;
import java.util.ArrayList;
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
@PrepareForTest(BatchUtil.class)
public class UpdatedCompletedEnrollmentTaskletTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private Logger logger;

    @Mock
    private ResponseEntity<DHISEnrollmentSyncResponse> responseEntity;

    @Mock
    private DHISEnrollmentSyncResponse syncResponse;

    @Mock
    private EnrollmentResponse response;

    @Mock
    private StepContribution stepContribution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private StepContext stepContext;

    @Mock
    private TrackersHandler trackersHandler;

    @Mock
    private EnrollmentResponseHandler enrollmentResponseHandler;

    private UpdatedCompletedEnrollmentTasklet tasklet;

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

        tasklet = new UpdatedCompletedEnrollmentTasklet();

        setValuesForMemberFields(tasklet, "syncRepository", syncRepository);
        setValuesForMemberFields(tasklet, "logger", logger);
        setValuesForMemberFields(tasklet, "trackersHandler", trackersHandler);
        setValuesForMemberFields(tasklet, "enrollmentResponseHandler", enrollmentResponseHandler);

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

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(trackersHandler.updateInEnrollmentTracker("superman")).thenReturn(1);
        when(trackersHandler.insertInEventTracker("superman")).thenReturn(1);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman");
        verify(trackersHandler, times(1)).insertInEventTracker("superman");
    }

    @Test
    public void shouldCallResponseHandlerOnFail() throws Exception {
        String requestBody = "{" +
                "\"enrollments\":[" +
                getEnrollment(payLoad1) +
                "]" +
                "}";

        List<EnrollmentImportSummary> importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(trackersHandler.updateInEnrollmentTracker("superman")).thenReturn(1);
        when(trackersHandler.insertInEventTracker("superman")).thenReturn(1);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(enrollmentResponseHandler, times(1)).processCompletedSecondStepResponse(any(), any(), any(), any());
        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman");
        verify(trackersHandler, times(1)).insertInEventTracker("superman");
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
        when(trackersHandler.updateInEnrollmentTracker("superman")).thenReturn(1);
        when(trackersHandler.insertInEventTracker("superman")).thenThrow(new SQLException("can't get database connection"));

        try {
            tasklet.execute(stepContribution, chunkContext);
        } catch (SQLException e) {
            verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
            verify(responseEntity, times(1)).getBody();
            verify(syncResponse, times(1)).getResponse();
            verify(response, times(1)).getImportSummaries();
            verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: Exception occurred " +
                    "while inserting Event UIDs:can't get database connection");
        }
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
}
