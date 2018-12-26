package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.model.Response;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.service.JobService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(JobService.class)
public class UpdatedActiveAndCompletedEnrollmentWithEventsWriterTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<DHISEnrollmentSyncResponse> responseEntity;

    @Mock
    private Logger logger;

    @Mock
    private DHISEnrollmentSyncResponse syncResponse;

    @Mock
    private EnrollmentResponse response;

    @Mock
    private EnrollmentResponseHandler enrollmentResponseHandler;

    @Mock
    private EventResponseHandler eventResponseHandler;

    @Mock
    private LoggerService loggerService;

    private UpdatedActiveAndCompletedEnrollmentWithEventsWriter writer;

    private String uri = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    private Event event1;
    private Event event2;
    private Event event3;
    private EnrollmentAPIPayLoad payLoad1;
    private EnrollmentAPIPayLoad payLoad2;
    private EnrollmentAPIPayLoad payLoad3;
    List<ProcessedTableRow> processedTableRows;
    List<EnrollmentAPIPayLoad> enrollmentAPIPayLoads = new LinkedList<>();
    HashMap<String, List<EnrollmentDetails>> instancesWithEnrollments = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        String instanceId1 = "instance1";
        String instanceId2 = "instance2";
        String instanceId3 = "instance3";
        String enrollmentId1 = "enrollment1";
        String enrollmentId2 = "enrollment2";
        String enrollmentId3 = "enrollment3";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");
        Map<String, String> dataValues2 = new HashMap<>();
        dataValues2.put("gXNu7zJBTDN", "yes");
        dataValues2.put("jkEjtKqlJtN", "event value2");
        Map<String, String> dataValues3 = new HashMap<>();
        dataValues3.put("gXNu7zJBTDN", "yes");
        dataValues3.put("jkEjtKqlJtN", "event value3");

        event1 = getEvents(instanceId1, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1", enrollmentId1);
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        event2 = getEvents(instanceId2, eventDate, dataValues2, "2");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, events2, "2", enrollmentId2);
        ProcessedTableRow processedTableRow2 = getProcessedTableRow("2", payLoad2);

        event3 = getEvents(instanceId3, eventDate, dataValues3, "3");
        List<Event> events3 = new LinkedList<>();
        events3.add(event3);
        payLoad3 = getEnrollmentPayLoad(instanceId3, enrDate, events3, "3", enrollmentId3);
        ProcessedTableRow processedTableRow3 = getProcessedTableRow("3", payLoad3);

        enrollmentAPIPayLoads.add(payLoad1);
        enrollmentAPIPayLoads.add(payLoad2);
        enrollmentAPIPayLoads.add(payLoad3);
        processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2, processedTableRow3);
        writer = new UpdatedActiveAndCompletedEnrollmentWithEventsWriter();

        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "logger", logger);
        setValuesForMemberFields(writer, "enrollmentResponseHandler", enrollmentResponseHandler);
        setValuesForMemberFields(writer, "eventResponseHandler", eventResponseHandler);
        setValuesForMemberFields(writer, "loggerService", loggerService);

        when(responseEntity.getBody()).thenReturn(syncResponse);
        when(syncResponse.getResponse()).thenReturn(response);

        EnrollmentDetails enrollmentDetails1 = new EnrollmentDetails(payLoad1.getProgram(), payLoad1.getEnrollmentId(),
                payLoad1.getProgramStartDate(), "2018-10-12T12:00:00.234", payLoad1.getStatus());
        EnrollmentDetails enrollmentDetails2 = new EnrollmentDetails(payLoad2.getProgram(), payLoad2.getEnrollmentId(),
                payLoad2.getProgramStartDate(), "2018-10-12T12:00:00.234", payLoad2.getStatus());
        EnrollmentDetails enrollmentDetails3 = new EnrollmentDetails(payLoad3.getProgram(), payLoad3.getEnrollmentId(),
                payLoad3.getProgramStartDate(), "2018-10-12T12:00:00.234", payLoad3.getStatus());
        instancesWithEnrollments.put(instanceId1, Collections.singletonList(enrollmentDetails1));
        instancesWithEnrollments.put(instanceId2, Collections.singletonList(enrollmentDetails2));
        instancesWithEnrollments.put(instanceId3, Collections.singletonList(enrollmentDetails3));
        TEIUtil.setInstancesWithEnrollments(instancesWithEnrollments);

        mockStatic(JobService.class);
        when(JobService.isIS_JOB_FAILED()).thenReturn(false);
    }

    @Test
    public void shouldCallSyncRepoToSendData() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");
        dataValues.put("jkEjtKqlJtN", "text value");

        Event event = getEvents(instanceId, eventDate, dataValues, "1");
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event), "3", "1");
        ProcessedTableRow processedTableRow = getProcessedTableRow("3", payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);
        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad, payLoad.getEnrollmentId()) +
                ", " +
                "\"events\":[" +
                getEvent(event) +
                "]" +
                "}" +
                "]" +
                "}";
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    @Test
    public void shouldCollateTheRecordsWhenMultipleRecordsExistsForAPatient() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");
        Map<String, String> dataValues2 = new HashMap<>();
        dataValues2.put("gXNu7zJBTDN", "yes");
        dataValues2.put("jkEjtKqlJtN", "event value2");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "3", "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("3", payLoad1);

        Event event2 = getEvents(instanceId, eventDate, dataValues2, "2");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        EnrollmentAPIPayLoad payLoad2 = getEnrollmentPayLoad(instanceId, enrDate, events2, "3", "1");
        ProcessedTableRow processedTableRow2 = getProcessedTableRow("3", payLoad2);

        EnrollmentAPIPayLoad payLoad3 = getEnrollmentPayLoad(instanceId, enrDate, new LinkedList<>(), "3", "1");
        ProcessedTableRow processedTableRow3 = getProcessedTableRow("3", payLoad3);

        List<ProcessedTableRow> processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2, processedTableRow3);
        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad1, payLoad1.getEnrollmentId()) +
                ", " +
                "\"events\":[" +
                getEvent(event1) +
                "," +
                getEvent(event2) +
                "]" +
                "}" +
                "]" +
                "}";
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    @Test
    public void shouldCallSuccessResponseProcessorsOnSyncSuccess() throws Exception {
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, payLoad1.getEnrollmentId()) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2, payLoad2.getEnrollmentId()) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3, payLoad3.getEnrollmentId()) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event3) +
                        "]" +
                    "}" +
                "]" +
            "}";
        String enrReference1 = "enrReference1";
        String enrReference2 = "enrReference2";
        String enrReference3 = "enrReference3";
        String envReference1 = "envReference1";
        String envReference2 = "envReference2";
        String envReference3 = "envReference3";

        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference1)),
                        1
                )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference2)),
                        1
                )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference3)),
                        1
                )
                )
        );

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(enrollmentResponseHandler, times(1)).processImportSummaries(any(), any());
        verify(eventResponseHandler, times(1))
                .process(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldCallErrorResponseProcessorsOnSyncFail() throws Exception {
        event2.setProgramStage("wrong program stage");
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        "{" +
                            getEnrollment(payLoad1, payLoad1.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event1) +
                            "]" +
                        "}," +
                        "{" +
                            getEnrollment(payLoad2, payLoad2.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event2) +
                            "]" +
                        "}," +
                        "{" +
                            getEnrollment(payLoad3, payLoad3.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event3) +
                            "]" +
                        "}" +
                    "]" +
                "}";
        String enrReference1 = "enrReference1";
        String enrReference2 = "enrReference2";
        String enrReference3 = "enrReference3";
        String envReference1 = "envReference1";
        String envReference3 = "envReference3";

        String description = "Event.programStage does not point to a valid programStage, and program is multi-stage: " +
                "wrong program stage";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference1)),
                        1
                )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference2, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_ERROR, 0, 0, 0, 1,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                                        description, new ArrayList<>(), null)),
                        1
                )
                ),
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference3, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 2, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(2, 0, 0, 0),
                                        null, new ArrayList<>(), envReference3)),
                        1
                )
                )
        );

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(2)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(enrollmentResponseHandler, times(1)).processErrorResponse(any(), any(), any(), any());
        verify(eventResponseHandler, times(1)).process(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldHandleErrorWhenSamePatientHasUpdatesForMoreThanOneEnrollment() throws Exception {
        payLoad2.setInstanceId("instance1");
        String requestBody = "{" +
                    "\"enrollments\":[" +
                        "{" +
                            getEnrollment(payLoad1, payLoad1.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event1) +
                            "]" +
                        "}," +
                        "{" +
                            getEnrollment(payLoad2, payLoad2.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event2) +
                            "]" +
                        "}," +
                        "{" +
                            getEnrollment(payLoad3, payLoad3.getEnrollmentId()) +
                            ", " +
                            "\"events\":[" +
                                getEvent(event3) +
                            "]" +
                        "}" +
                    "]" +
                "}";

        String message = "Program has another active enrollment going on. Not possible to incomplete";

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(syncResponse.getMessage()).thenReturn(message);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(2)).getBody();
        verify(syncResponse, times(1)).getMessage();
        verify(logger, times(1)).error("UPDATE COMPLETED ENROLLMENT WITH EVENTS SYNC: " + message);
        verify(loggerService, times(1)).collateLogMessage(message);
    }

    @Test
    public void shouldThrowErrorWhenDHISHasDifferentActiveEnrollment() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");
        dataValues.put("jkEjtKqlJtN", "text value");

        Event event = getEvents(instanceId, eventDate, dataValues, "1");
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event), "3", "enrollment1");
        ProcessedTableRow processedTableRow = getProcessedTableRow("3", payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);

        EnrollmentDetails enrollmentDetails1 = new EnrollmentDetails(payLoad.getProgram(), "enrollment1",
                payLoad.getProgramStartDate(), "2018-10-12T12:00:00.234", EnrollmentAPIPayLoad.STATUS_COMPLETED);
        EnrollmentDetails enrollmentDetails2 = new EnrollmentDetails(payLoad.getProgram(), "enrollment2",
                "2018-11-12T19:00:00.345", null, EnrollmentAPIPayLoad.STATUS_ACTIVE);

        instancesWithEnrollments.clear();
        instancesWithEnrollments.put(instanceId, Arrays.asList(enrollmentDetails1, enrollmentDetails2));

        TEIUtil.setInstancesWithEnrollments(instancesWithEnrollments);

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad, "") +
                ", " +
                "\"events\":[" +
                getEvent(event) +
                "]" +
                "}" +
                "]" +
                "}";

        String message = "DHIS has another active enrollment going on. Can't complete this enrollment. " +
                "BAHMNI enrollment id: enrollment1 and DHIS enrollment id: enrollment2";
        when(JobService.isIS_JOB_FAILED()).thenReturn(true);
        doNothing().when(JobService.class);
        JobService.setIS_JOB_FAILED(true);
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(0)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(0)).getBody();
        verify(syncResponse, times(0)).getResponse();
        verify(response, times(0)).getImportSummaries();
        verifyStatic(times(1));
        JobService.setIS_JOB_FAILED(true);
        verifyStatic(times(1));
        JobService.isIS_JOB_FAILED();
        verify(logger, times(1)).error("UPDATE COMPLETED ENROLLMENT WITH EVENTS SYNC: " + message);
        verify(loggerService, times(1)).collateLogMessage(message);
    }

    @Test
    public void shouldSyncSuccessfullyWhenDHISHasNoActiveEnrollments() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");
        dataValues.put("jkEjtKqlJtN", "text value");

        Event event = getEvents(instanceId, eventDate, dataValues, "1");
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event), "3", "enrollment1");
        ProcessedTableRow processedTableRow = getProcessedTableRow("3", payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);

        EnrollmentDetails enrollmentDetails1 = new EnrollmentDetails(payLoad.getProgram(), "enrollment1",
                payLoad.getProgramStartDate(), "2018-10-12T12:00:00.234", EnrollmentAPIPayLoad.STATUS_COMPLETED);
        EnrollmentDetails enrollmentDetails2 = new EnrollmentDetails(payLoad.getProgram(), "enrollment2",
                "2018-11-12T19:00:00.345", null, EnrollmentAPIPayLoad.STATUS_COMPLETED);

        instancesWithEnrollments.clear();
        instancesWithEnrollments.put(instanceId, Arrays.asList(enrollmentDetails1, enrollmentDetails2));

        TEIUtil.setInstancesWithEnrollments(instancesWithEnrollments);

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad, "enrollment1") +
                ", " +
                "\"events\":[" +
                getEvent(event) +
                "]" +
                "}" +
                "]" +
                "}";

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    private ProcessedTableRow getProcessedTableRow(String enrollmentUniqueId, EnrollmentAPIPayLoad payLoad) {
        return new ProcessedTableRow(
                enrollmentUniqueId,
                payLoad
        );
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events, String programUniqueId, String enrollmentId) {
        return new EnrollmentAPIPayLoad(
                enrollmentId,
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                EnrollmentAPIPayLoad.STATUS_COMPLETED,
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

    private String getEvent(Event event) {
        return String.format("{" +
                        "\"event\":\"\", " +
                        "\"trackedEntityInstance\":\"%s\", " +
                        "\"enrollment\":\"\", " +
                        "\"program\":\"%s\", " +
                        "\"programStage\":\"%s\", " +
                        "\"orgUnit\":\"%s\", " +
                        "\"eventDate\":\"%s\", " +
                        "\"status\":\"COMPLETED\", " +
                        "\"dataValues\":[" +
                        "{" +
                        "\"dataElement\":\"gXNu7zJBTDN\", " +
                        "\"value\":\"%s\"" +
                        "}," +
                        "{" +
                        "\"dataElement\":\"jkEjtKqlJtN\", " +
                        "\"value\":\"%s\"" +
                        "}" +
                        "]" +
                        "}",
                event.getTrackedEntityInstance(),
                event.getProgram(),
                event.getProgramStage(),
                event.getOrgUnit(),
                event.getEventDate(),
                event.getDataValues().get("gXNu7zJBTDN"),
                event.getDataValues().get("jkEjtKqlJtN")
        );
    }

    private String getEnrollment(EnrollmentAPIPayLoad payLoad, String enrollmentId) {
        return String.format("\"enrollment\":\"%s\", " +
                        "\"trackedEntityInstance\":\"%s\", " +
                        "\"orgUnit\":\"%s\", " +
                        "\"program\":\"%s\", " +
                        "\"enrollmentDate\":\"%s\", " +
                        "\"incidentDate\":\"%s\", " +
                        "\"status\":\"ACTIVE\"",
                enrollmentId,
                payLoad.getInstanceId(),
                payLoad.getOrgUnit(),
                payLoad.getProgram(),
                payLoad.getProgramStartDate(),
                payLoad.getIncidentDate()
        );
    }
}
