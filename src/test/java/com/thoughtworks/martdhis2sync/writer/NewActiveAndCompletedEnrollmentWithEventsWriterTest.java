package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForSuperClassMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class NewActiveAndCompletedEnrollmentWithEventsWriterTest {
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

    private NewCompletedEnrollmentWithEventsWriter writer;

    @Mock
    private MarkerUtil markerUtil;

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

        enrollmentAPIPayLoads.add(payLoad1);
        enrollmentAPIPayLoads.add(payLoad2);
        enrollmentAPIPayLoads.add(payLoad3);
        processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2, processedTableRow3);
        writer = new NewCompletedEnrollmentWithEventsWriter();

        setValuesForSuperClassMemberFields(writer, "syncRepository", syncRepository);
        setValuesForSuperClassMemberFields(writer, "logger", logger);
        setValuesForSuperClassMemberFields(writer, "enrollmentResponseHandler", enrollmentResponseHandler);
        setValuesForSuperClassMemberFields(writer, "eventResponseHandler", eventResponseHandler);
        setValuesForSuperClassMemberFields(writer, "openLatestCompletedEnrollment", "no");
        setValuesForSuperClassMemberFields(writer, "markerUtil", markerUtil);

        when(responseEntity.getBody()).thenReturn(syncResponse);
        when(syncResponse.getResponse()).thenReturn(response);

        instancesWithEnrollments.put(instanceId1, new ArrayList<>());
        instancesWithEnrollments.put(instanceId2, new ArrayList<>());
        instancesWithEnrollments.put(instanceId3, new ArrayList<>());
        TEIUtil.setInstancesWithEnrollments(instancesWithEnrollments);
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
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event), "1");
        ProcessedTableRow processedTableRow = getProcessedTableRow("1", payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);
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
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        Event event2 = getEvents(instanceId, eventDate, dataValues2, "2");
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        EnrollmentAPIPayLoad payLoad2 = getEnrollmentPayLoad(instanceId, enrDate, events2, "1");
        ProcessedTableRow processedTableRow2 = getProcessedTableRow("1", payLoad2);

        EnrollmentAPIPayLoad payLoad3 = getEnrollmentPayLoad(instanceId, enrDate, Collections.emptyList(), "2");
        ProcessedTableRow processedTableRow3 = getProcessedTableRow("2", payLoad3);

        EnrollmentAPIPayLoad payLoad4 = getEnrollmentPayLoad(instanceId, enrDate, new LinkedList<>(), "1");
        ProcessedTableRow processedTableRow4 = getProcessedTableRow("1", payLoad4);


        List<ProcessedTableRow> processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2,
                processedTableRow3, processedTableRow4);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                            "," +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[]" +
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
    public void shouldHaveEmptyEnrollmentIdWhenInstanceDoesNotHaveAnyEnrollments() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
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
    public void shouldHaveEmptyEnrollmentIdWhenInstanceDoesNotExistInTheList() throws Exception {
        String instanceId = "instance4";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
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
    public void shouldHaveActiveEnrollmentIdWhenInstanceAlreadyHasEnrollment() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_ACTIVE,new ArrayList<>());
        instancesWithEnrollments.put(instanceId, Collections.singletonList(enrDetails1));

        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "enrollmentId1") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
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
    public void shouldHaveEmptyEnrollmentIdWhenInstanceHasOnlyCompletedEnrollment() throws Exception {
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());
        instancesWithEnrollments.put(instanceId, Collections.singletonList(enrDetails1));

        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
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
    public void shouldHaveLatestCompletedEnrollmentIdWhenInstanceHasOnlyCompletedEnrollmentAndConfigIsYes() throws Exception {
        setValuesForSuperClassMemberFields(writer, "openLatestCompletedEnrollment", "yes");

        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());
        EnrollmentDetails enrDetails2 = new EnrollmentDetails("xhjKKwoq", "enrollmentId2", "2018-11-06T00:00:00.000",
                "2018-12-07T18:14:41.513", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());

        instancesWithEnrollments.put(instanceId, Arrays.asList(enrDetails1, enrDetails2));

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad1, "enrollmentId2") +
                ", " +
                "\"events\":[" +
                getEvent(event1) +
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
    public void shouldHaveLatestCompletedEnrollmentIdAndDoNotConsiderTheOrderWhenInstanceHasOnlyCompletedEnrollmentAndConfigIsYes() throws Exception {
        setValuesForSuperClassMemberFields(writer, "openLatestCompletedEnrollment", "yes");

        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-07T18:14:41.513", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());
        EnrollmentDetails enrDetails2 = new EnrollmentDetails("xhjKKwoq", "enrollmentId2", "2018-11-06T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());

        instancesWithEnrollments.put(instanceId, Arrays.asList(enrDetails1, enrDetails2));

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad1, "enrollmentId1") +
                ", " +
                "\"events\":[" +
                getEvent(event1) +
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
    public void shouldHaveActiveEnrollmentIdWhenInstanceHasActiveEnrollmentAndConfigIsYes() throws Exception {
        setValuesForSuperClassMemberFields(writer, "openLatestCompletedEnrollment", "yes");

        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());
        EnrollmentDetails enrDetails2 = new EnrollmentDetails("xhjKKwoq", "enrollmentId2", "2018-11-06T00:00:00.000",
                null, EnrollmentAPIPayLoad.STATUS_ACTIVE,new ArrayList<>());

        instancesWithEnrollments.put(instanceId, Arrays.asList(enrDetails1, enrDetails2));

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad1, "enrollmentId2") +
                ", " +
                "\"events\":[" +
                getEvent(event1) +
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
    public void shouldCallSuccessResponseProcessorsOnSyncSuccess() throws Exception {
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3, "") +
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
                        getEnrollment(payLoad1, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event1) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad2, "") +
                        ", " +
                        "\"events\":[" +
                            getEvent(event2) +
                        "]" +
                    "}," +
                    "{" +
                        getEnrollment(payLoad3, "") +
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
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
        verify(enrollmentResponseHandler, times(1)).processErrorResponse(any(), any(), any(), any());
        verify(eventResponseHandler, times(1)).process(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldSyncNewEnrollmentWhenInstanceHasOnlyCompletedEnrollmentAndConfigIsNo() throws Exception {
        setValuesForSuperClassMemberFields(writer, "openLatestCompletedEnrollment", "no");

        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        dataValues1.put("jkEjtKqlJtN", "event value1");

        Event event1 = getEvents(instanceId, eventDate, dataValues1, "1");
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1, "1");
        ProcessedTableRow processedTableRow1 = getProcessedTableRow("1", payLoad1);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow1);

        EnrollmentDetails enrDetails1 = new EnrollmentDetails("xhjKKwoq", "enrollmentId1", "2018-11-04T00:00:00.000",
                "2018-12-05T23:07:10.934", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());
        EnrollmentDetails enrDetails2 = new EnrollmentDetails("xhjKKwoq", "enrollmentId2", "2018-11-06T00:00:00.000",
                "2018-12-07T18:14:41.513", EnrollmentAPIPayLoad.STATUS_COMPLETED,new ArrayList<>());

        instancesWithEnrollments.put(instanceId, Arrays.asList(enrDetails1, enrDetails2));

        String requestBody = "{" +
                "\"enrollments\":[" +
                "{" +
                getEnrollment(payLoad1, "") +
                ", " +
                "\"events\":[" +
                getEvent(event1) +
                "]" +
                "}" +
                "]" +
                "}";

        String enrReference1 = "enrReference1";
        String envReference1 = "envReference1";

        List<EnrollmentImportSummary> importSummaries = Arrays.asList(
                new EnrollmentImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                        null, new ArrayList<>(), enrReference1, new Response("ImportSummaries",
                        IMPORT_SUMMARY_RESPONSE_SUCCESS, 1, 0, 0, 0,
                        Collections.singletonList(
                                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS, new ImportCount(1, 0, 0, 0),
                                        null, new ArrayList<>(), envReference1)),
                        1
                )
                ));

        when(syncRepository.sendEnrollmentData(uri, requestBody)).thenReturn(responseEntity);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendEnrollmentData(uri, requestBody);
        verify(responseEntity, times(1)).getBody();
        verify(syncResponse, times(1)).getResponse();
        verify(response, times(1)).getImportSummaries();
    }

    private ProcessedTableRow getProcessedTableRow(String programUniqueId, EnrollmentAPIPayLoad payLoad) {
        return new ProcessedTableRow(
                programUniqueId,
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
