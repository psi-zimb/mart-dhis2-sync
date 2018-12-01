package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.Conflict;
import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.model.Response;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.Conflict.CONFLICT_OBJ_ENROLLMENT_DATE;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_WARNING;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.enrollmentsToSaveInTracker;
import static com.thoughtworks.martdhis2sync.util.EventUtil.eventsToSaveInTracker;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getEventTrackers;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class NewCompletedEnrollmentWithEventsWriterTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<DHISEnrollmentSyncResponse> responseEntity;

    @Mock
    private LoggerService loggerService;

    @Mock
    private Logger logger;

    @Mock
    private DHISEnrollmentSyncResponse syncResponse;

    @Mock
    private EnrollmentResponse response;

    private NewCompletedEnrollmentWithEventsWriter writer;

    private String uri = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    private Event event1;
    private Event event2;
    private Event event3;
    private EnrollmentAPIPayLoad payLoad1;
    private EnrollmentAPIPayLoad payLoad2;
    private EnrollmentAPIPayLoad payLoad3;
    List<ProcessedTableRow> processedTableRows;

    @Before
    public void setUp() throws Exception {
        String patientIdentifier1 = "NAH00010";
        String patientIdentifier2 = "NAH00011";
        String patientIdentifier3 = "NAH00012";
        String instanceId1 = "instance1";
        String instanceId2 = "instance2";
        String instanceId3 = "instance3";
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
        payLoad1 = getEnrollmentPayLoad(instanceId1, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow(patientIdentifier1, payLoad1);

        event2 = getEvents(instanceId2, eventDate, dataValues2, "2");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        payLoad2 = getEnrollmentPayLoad(instanceId2, enrDate, events2, "2");
        ProcessedTableRow processedTableRow2 = getProcessedTableRow(patientIdentifier2, payLoad2);

        event3 = getEvents(instanceId3, eventDate, dataValues3, "3");
        List<Event> events3 = new LinkedList<>();
        events3.add(event3);
        payLoad3 = getEnrollmentPayLoad(instanceId3, enrDate, events3, "3");
        ProcessedTableRow processedTableRow3 = getProcessedTableRow(patientIdentifier3, payLoad3);

        processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2, processedTableRow3);
        writer = new NewCompletedEnrollmentWithEventsWriter();

        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "loggerService", loggerService);
        setValuesForMemberFields(writer, "logger", logger);

        when(responseEntity.getBody()).thenReturn(syncResponse);
        when(syncResponse.getResponse()).thenReturn(response);
    }

    @Test
    public void shouldCallSyncRepoToSendData() throws Exception {
        String patientIdentifier = "NAH00010";
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");
        dataValues.put("jkEjtKqlJtN", "text value");

        Event event = getEvents(instanceId, eventDate, dataValues, "1");
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event), "1");
        ProcessedTableRow processedTableRow = getProcessedTableRow(patientIdentifier, payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event) +
                        "]" +
                    "}" +
                "]" +
            "}";
        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    @Test
    public void shouldCollateTheRecordsWhenMultipleRecordsExistsForAPatient() throws Exception {
        String patientIdentifier = "NAH00010";
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
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow(patientIdentifier, payLoad1);

        Event event2 = getEvents(instanceId, eventDate, dataValues2, "2");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        EnrollmentAPIPayLoad payLoad2 = getEnrollmentPayLoad(instanceId, enrDate, events2, "1");
        ProcessedTableRow processedTableRow2 = getProcessedTableRow(patientIdentifier, payLoad2);

        List<ProcessedTableRow> processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
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
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    @Test
    public void shouldUpdateUtilTrackersWhenTheSyncIsSuccess() throws Exception {
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3) +
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
        payLoad1.setEnrollmentId(enrReference1);
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2, payLoad3);
        event1.setEvent(envReference1);
        event2.setEvent(envReference2);
        event3.setEvent(envReference3);
        List<EventTracker> expectedEventsToSaveInTracker = getEventTrackers(Arrays.asList(event1, event2, event3));

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        assertEquals(expectedEnrollmentsToSaveInTracker, enrollmentsToSaveInTracker);
        assertEquals(expectedEventsToSaveInTracker, eventsToSaveInTracker);
        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    @Test
    public void shouldNotReturnReferencesForEventsIfTheProgramStageIsInCorrect() throws Exception {
        event2.setProgramStage("wrong program stage");
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3) +
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
        payLoad1.setEnrollmentId(enrReference1);
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2, payLoad3);
        event1.setEvent(envReference1);
        event3.setEvent(envReference3);
        List<EventTracker> expectedEventsToSaveInTracker = getEventTrackers(Arrays.asList(event1, event3));


        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        assertEquals(expectedEnrollmentsToSaveInTracker, enrollmentsToSaveInTracker);
        assertEquals(expectedEventsToSaveInTracker, eventsToSaveInTracker);
        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: " + description);
        verify(loggerService, times(1)).collateLogMessage(description);
    }

    @Test
    public void shouldNotReturnReferencesForEventsIfTheDataTypeIsMisMatch() throws Exception {
        event3.getDataValues().put("gXNu7zJBTDN", "True");
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3) +
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
                                IMPORT_SUMMARY_RESPONSE_WARNING, 1, 0, 0, 1,
                                Collections.singletonList(
                                        new ImportSummary("", IMPORT_SUMMARY_RESPONSE_WARNING, new ImportCount(1, 0, 1, 0),
                                        null, Collections.singletonList(new Conflict("gXNu7zJBTDN", "value_not_true_only")),
                                        envReference3)),
                        1
                        )
                )
        );
        payLoad1.setEnrollmentId(enrReference1);
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad2, payLoad3);
        event1.setEvent(envReference1);
        event2.setEvent(envReference2);
        event3.setEvent(envReference3);
        List<EventTracker> expectedEventsToSaveInTracker = getEventTrackers(Arrays.asList(event1, event2, event3));


        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        assertEquals(expectedEnrollmentsToSaveInTracker, enrollmentsToSaveInTracker);
        assertEquals(expectedEventsToSaveInTracker, eventsToSaveInTracker);
        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: gXNu7zJBTDN: value_not_true_only");
        verify(loggerService, times(1)).collateLogMessage("gXNu7zJBTDN: value_not_true_only");
    }

    @Test
    public void shouldLogConflictsAndThrowExceptionOnEnrollmentSyncFailureWith409ConflictWhenEnrollmentDateIsFutureDate() throws Exception {
        payLoad2.setIncidentDate("2020-10-20");
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event3) +
                        "]" +
                    "}" +
                "]" +
            "}";
        String enrReference1 = "enrReference1";
        String enrReference3 = "enrReference3";
        String envReference1 = "envReference1";
        String envReference3 = "envReference3";

        String conflictMessage = "Enrollment Date can't be future date :Mon Oct 20 00:00:00 IST 2020";
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
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        null, Collections.singletonList(new Conflict(CONFLICT_OBJ_ENROLLMENT_DATE, conflictMessage)),
                        null, null
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
        payLoad1.setEnrollmentId(enrReference1);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad1, payLoad3);
        event1.setEvent(envReference1);
        event3.setEvent(envReference3);
        List<EventTracker> expectedEventsToSaveInTracker = getEventTrackers(Arrays.asList(event1, event3));


        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        assertEquals(expectedEnrollmentsToSaveInTracker, enrollmentsToSaveInTracker);
        assertEquals(expectedEventsToSaveInTracker, eventsToSaveInTracker);
        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: " +
                CONFLICT_OBJ_ENROLLMENT_DATE +": " + conflictMessage);
        verify(loggerService, times(1)).collateLogMessage(CONFLICT_OBJ_ENROLLMENT_DATE +
                ": " + conflictMessage);
    }

    @Test
    public void shouldLogDescriptionAndThrowExceptionOnEnrollmentSyncFailureWhenInstanceIsAlreadyEnrolled() throws Exception {
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3) +
                        ", " +
                        "\"events\":[" +
                            getEvent(event3) +
                        "]" +
                    "}" +
                "]" +
            "}";
        String enrReference2 = "enrReference2";
        String enrReference3 = "enrReference3";
        String envReference2 = "envReference2";
        String envReference3 = "envReference3";
        String description = "TrackedEntityInstance instance1 already has an active enrollment in program xhjKKwoq";
        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR, new ImportCount(0, 0, 1, 0),
                        description, new ArrayList<>(), null, null
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
        payLoad2.setEnrollmentId(enrReference2);
        payLoad3.setEnrollmentId(enrReference3);
        List<EnrollmentAPIPayLoad> expectedEnrollmentsToSaveInTracker = Arrays.asList(payLoad2, payLoad3);
        event2.setEvent(envReference2);
        event3.setEvent(envReference3);
        List<EventTracker> expectedEventsToSaveInTracker = getEventTrackers(Arrays.asList(event2, event3));


        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(processedTableRows);

        assertEquals(expectedEnrollmentsToSaveInTracker, enrollmentsToSaveInTracker);
        assertEquals(expectedEventsToSaveInTracker, eventsToSaveInTracker);
        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(logger, times(1)).error("NEW COMPLETED ENROLLMENT SYNC: " + description);
        verify(loggerService, times(1)).collateLogMessage(description);
    }

    private ProcessedTableRow getProcessedTableRow(String patientIdentifier, EnrollmentAPIPayLoad payLoad) {
        return new ProcessedTableRow(
                patientIdentifier,
                payLoad
        );
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events, String programUniqueId) {
        return new EnrollmentAPIPayLoad(
                "",
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

    private String getEnrollment(EnrollmentAPIPayLoad payLoad) {
        return String.format("\"enrollment\":\"\", " +
                        "\"trackedEntityInstance\":\"%s\", " +
                        "\"orgUnit\":\"%s\", " +
                        "\"program\":\"%s\", " +
                        "\"enrollmentDate\":\"%s\", " +
                        "\"incidentDate\":\"%s\", " +
                        "\"status\":\"ACTIVE\"",
                payLoad.getInstanceId(),
                payLoad.getOrgUnit(),
                payLoad.getProgram(),
                payLoad.getProgramStartDate(),
                payLoad.getIncidentDate()
        );
    }
}
