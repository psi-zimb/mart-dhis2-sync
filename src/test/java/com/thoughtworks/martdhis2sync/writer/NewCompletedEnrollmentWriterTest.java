package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class NewCompletedEnrollmentWriterTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<DHISSyncResponse> responseEntity;

    private NewCompletedEnrollmentWriter writer;

    private String uri = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Before
    public void setUp() throws Exception {
        writer = new NewCompletedEnrollmentWriter();

        setValuesForMemberFields(writer, "syncRepository", syncRepository);
    }

    @Test
    public void shouldCallSyncRepoToSendData() throws Exception {
        String patientIdentifier = "NAH00010";
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues = new HashMap<>();
        dataValues.put("gXNu7zJBTDN", "no");

        Event event = getEvents(instanceId, eventDate, dataValues);
        EnrollmentAPIPayLoad payLoad = getEnrollmentPayLoad(instanceId, enrDate, Collections.singletonList(event));
        ProcessedTableRow processedTableRow = getProcessedTableRow(patientIdentifier, payLoad);

        List<ProcessedTableRow> processedTableRows = Collections.singletonList(processedTableRow);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        "\"enrollment\":\"\", " +
                        "\"trackedEntityInstance\":\"instance1\", " +
                        "\"orgUnit\":\"jSsoNjesL\", " +
                        "\"program\":\"xhjKKwoq\", " +
                        "\"enrollmentDate\":\"2018-10-13\", " +
                        "\"incidentDate\":\"2018-10-13\", " +
                        "\"status\":\"ACTIVE\", " +
                        "\"events\":[" +
                            "{" +
                                "\"event\":\"\", " +
                                "\"trackedEntityInstance\":\"instance1\", " +
                                "\"enrollment\":\"\", " +
                                "\"program\":\"xhjKKwoq\", " +
                                "\"programStage\":\"FJTkwmaP\", " +
                                "\"orgUnit\":\"jSsoNjesL\", " +
                                "\"eventDate\":\"2018-10-14\", " +
                                "\"status\":\"COMPLETED\", " +
                                "\"dataValues\":[" +
                                    "{" +
                                        "\"dataElement\":\"gXNu7zJBTDN\", " +
                                        "\"value\":\"no\"" +
                                    "}" +
                                "]" +
                            "}" +
                        "]" +
                    "}" +
                "]" +
            "}";
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    public void shouldCollateTheRecordsWhenMultipleRecordsExistsForAPatient() throws Exception {
        String patientIdentifier = "NAH00010";
        String instanceId = "instance1";
        String enrDate = "2018-10-13";
        String eventDate = "2018-10-14";
        Map<String, String> dataValues1 = new HashMap<>();
        dataValues1.put("gXNu7zJBTDN", "no");
        Map<String, String> dataValues2 = new HashMap<>();
        dataValues2.put("gXNu7zJBTDN", "yes");

        Event event1 = getEvents(instanceId, eventDate, dataValues1);
        List<Event> events1 = new LinkedList<>();
        events1.add(event1);
        EnrollmentAPIPayLoad payLoad1 = getEnrollmentPayLoad(instanceId, enrDate, events1);
        ProcessedTableRow processedTableRow1 = getProcessedTableRow(patientIdentifier, payLoad1);

        Event event2 = getEvents(instanceId, eventDate, dataValues2);
        List<Event> events2 = new LinkedList<>();
        events2.add(event2);
        EnrollmentAPIPayLoad payLoad2 = getEnrollmentPayLoad(instanceId, enrDate, events2);
        ProcessedTableRow processedTableRow2 = getProcessedTableRow(patientIdentifier, payLoad2);

        List<ProcessedTableRow> processedTableRows = Arrays.asList(processedTableRow1, processedTableRow2);
        String requestBody = "{" +
                "\"enrollments\":[" +
                    "{" +
                        "\"enrollment\":\"\", " +
                        "\"trackedEntityInstance\":\"instance1\", " +
                        "\"orgUnit\":\"jSsoNjesL\", " +
                        "\"program\":\"xhjKKwoq\", " +
                        "\"enrollmentDate\":\"2018-10-13\", " +
                        "\"incidentDate\":\"2018-10-13\", " +
                        "\"status\":\"ACTIVE\", " +
                        "\"events\":[" +
                            "{" +
                                "\"event\":\"\", " +
                                "\"trackedEntityInstance\":\"instance1\", " +
                                "\"enrollment\":\"\", " +
                                "\"program\":\"xhjKKwoq\", " +
                                "\"programStage\":\"FJTkwmaP\", " +
                                "\"orgUnit\":\"jSsoNjesL\", " +
                                "\"eventDate\":\"2018-10-14\", " +
                                "\"status\":\"COMPLETED\", " +
                                "\"dataValues\":[" +
                                    "{" +
                                        "\"dataElement\":\"gXNu7zJBTDN\", " +
                                        "\"value\":\"no\"" +
                                    "}" +
                                "]" +
                            "}," +
                            "{" +
                                "\"event\":\"\", " +
                                "\"trackedEntityInstance\":\"instance1\", " +
                                "\"enrollment\":\"\", " +
                                "\"program\":\"xhjKKwoq\", " +
                                "\"programStage\":\"FJTkwmaP\", " +
                                "\"orgUnit\":\"jSsoNjesL\", " +
                                "\"eventDate\":\"2018-10-14\", " +
                                "\"status\":\"COMPLETED\", " +
                                "\"dataValues\":[" +
                                    "{" +
                                        "\"dataElement\":\"gXNu7zJBTDN\", " +
                                        "\"value\":\"yes\"" +
                                    "}" +
                                "]" +
                            "}" +
                        "]" +
                    "}" +
                "]" +
            "}";
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        writer.write(processedTableRows);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    private ProcessedTableRow getProcessedTableRow(String patientIdentifier, EnrollmentAPIPayLoad payLoad) {
        return new ProcessedTableRow(
                patientIdentifier,
                payLoad
        );
    }

    private EnrollmentAPIPayLoad getEnrollmentPayLoad(String instanceId, String enrDate, List<Event> events) {
        return new EnrollmentAPIPayLoad(
                "",
                instanceId,
                "xhjKKwoq",
                "jSsoNjesL",
                enrDate,
                enrDate,
                "ACTIVE",
                "1",
                events
        );
    }

    private Event getEvents(String instanceId, String eventDate, Map<String, String> dataValues) {
        return new Event(
                "",
                instanceId,
                "",
                "xhjKKwoq",
                "FJTkwmaP",
                "jSsoNjesL",
                eventDate,
                "COMPLETED",
                dataValues
        );
    }
}
