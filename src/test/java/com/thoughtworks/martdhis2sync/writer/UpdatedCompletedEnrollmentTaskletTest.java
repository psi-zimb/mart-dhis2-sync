package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
    private String enrId1 = "enrId1";
    private String instanceId1 = "instance1";
    private String enrDate = "2018-10-13";
    private String date = "2018-10-12 13:00:00";
    private String logPrefix = "UPDATE COMPLETED ENROLLMENT SYNC: ";
    private String requestBody = "";
    List<EnrollmentImportSummary> importSummaries = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, "1", enrId1);
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("user", "superman");

        tasklet = new UpdatedCompletedEnrollmentTasklet();

        setValuesForMemberFields(tasklet, "syncRepository", syncRepository);
        setValuesForMemberFields(tasklet, "logger", logger);
        setValuesForMemberFields(tasklet, "trackersHandler", trackersHandler);
        setValuesForMemberFields(tasklet, "enrollmentResponseHandler", enrollmentResponseHandler);

        requestBody = "{" +
                "\"enrollments\":[" +
                getEnrollment(payLoad1) +
                "]" +
                "}";

        importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );

        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");
        Event event1 = getEvents(instanceId1, date, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);

        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1");
        EnrollmentUtil.enrollmentsToSaveInTracker.add(payLoad1);

        EventTracker eventTracker = new EventTracker("ieux8w6gn", instanceId1, "psuenc33", "11", "Uhyf56yg");
        EventUtil.eventsToSaveInTracker.add(eventTracker);

        mockStatic(BatchUtil.class);
        when(BatchUtil.GetUTCDateTimeAsString()).thenReturn(date);
        when(BatchUtil.removeLastChar(any())).thenReturn(getEnrollment(payLoad1));

        when(syncRepository.sendEnrollmentDataForUpdate(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(syncResponse);
        when(syncResponse.getResponse()).thenReturn(response);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
        when(stepContext.getJobParameters()).thenReturn(jobParams);
    }

    @Test
    public void shouldUpdateTrackersAfterSync() throws Exception {
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentDataForUpdate(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldCallResponseHandlerOnFail() throws Exception {
        importSummaries = Collections.singletonList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        null, new ArrayList<>(), enrId1, null
                )
        );

        when(response.getImportSummaries()).thenReturn(importSummaries);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentDataForUpdate(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(enrollmentResponseHandler, times(1)).processCompletedSecondStepResponse(any(), any(), any(), any());
        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(1)).insertInEventTracker("superman", logPrefix, logger);

        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldNotCallSyncRepositoryIfEnrollmentTrackerIsEmpty() throws Exception {
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(0)).sendEnrollmentDataForUpdate(anyString(), anyString());
        verify(trackersHandler, times(0)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
    }

    @Test
    public void shouldNotCallInsertIntoEventTrackerIdEventTrackerIsEmpty() throws Exception {
        EventUtil.eventsToSaveInTracker.clear();

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        tasklet.execute(stepContribution, chunkContext);

        verify(syncRepository, times(1)).sendEnrollmentDataForUpdate(anyString(), anyString());
        verify(trackersHandler, times(1)).updateInEnrollmentTracker("superman", logPrefix, logger);
        verify(trackersHandler, times(0)).insertInEventTracker("superman", logPrefix, logger);
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

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events, String programUniqueId) {
        return new EnrollmentAPIPayLoad(
                enrId1,
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                "ACTIVE",
                programUniqueId,
                events
        );
    }

    private Event getEvents(String instanceId, String eventDate, Map<String, String> dataValues, String eventUniqueId) {
        return new Event(
                "",
                instanceId,
                "",
                "xhjKKwoq",
                "FJTkwmaP",
                "jSsoNjesL",
                eventDate,
                "COMPLETED",
                eventUniqueId,
                dataValues
        );
    }
}
