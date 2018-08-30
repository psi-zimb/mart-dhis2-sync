package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.response.*;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.response.ImportSummary.RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TEIUtil.class, TrackedEntityInstanceWriter.class})
public class TrackedEntityInstanceWriterTest {

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private DataSource dataSource;

    @Mock
    private TrackedEntityResponse trackedEntityResponse;

    @Mock
    private ResponseEntity<TrackedEntityResponse> responseEntity;

    @Mock
    private Response response;

    @Mock
    private List<ImportSummary> importSummaries;

    @Mock
    private LinkedHashMap<String, String> patientUIDMap;

    @Mock
    private Connection connection;
    @Mock
    private PreparedStatement preparedStatement;

    private TrackedEntityInstanceWriter writer;

    private static final String uri = "/api/trackedEntityInstance";

    private static final String EMPTY_STRING = "\"\"";

    private static List<String> referenceUIDs = Arrays.asList("iUJiNefaBZ4", "AAQzNZIJcUj");

    private static List<String> patientIDs = Arrays.asList("NAH0000000001", "NAH0000000002");

    private String patient1, patient2, requestBody;

    private List<Object> list;

    @Before
    public void setUp() throws Exception {
        writer = new TrackedEntityInstanceWriter();

        setValuesForMemberFields(writer, "dataSource", dataSource);
        setValuesForMemberFields(writer, "teiUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);

        patient1 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03041889\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"412\"}" +
                "]}";

        patient2 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03051886\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"413\"}" +
                "]}";

        list = Arrays.asList(patient1, patient2);

        requestBody = "{\"trackedEntityInstances\":[" + patient1 + "," + patient2 + "]}";
    }

    @Test
    public void shouldCallSyncRepoToSendData() {

        when(responseEntity.getBody()).thenReturn(trackedEntityResponse);
        when(trackedEntityResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);


        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    public void shouldReturnNullWhenRequestFailed() throws Exception {
        when(syncRepository.sendData(uri, requestBody)).thenReturn(null);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(responseEntity, times(0)).getBody();
    }

    @Test
    @SneakyThrows
    public void shouldSuccessfullyProcessResponse() {

        importSummaries = Arrays.asList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(trackedEntityResponse);
        when(trackedEntityResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        mockStatic(TEIUtil.class);
        patientUIDMap = new LinkedHashMap<>();
        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    @SneakyThrows
    public void shouldHandleConflictsInTheResponseMessage() {

        List<Conflict> conflicts = Collections.singletonList(new Conflict("", "Invalid org unit ID: SxgCPPeiq3c_"));
        importSummaries = Collections.singletonList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(0, 0, 0, 0), conflicts, referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(trackedEntityResponse);
        when(trackedEntityResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        mockStatic(TEIUtil.class);
        patientUIDMap = new LinkedHashMap<>();
        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    @SneakyThrows
    public void shouldHandleSQLException() {

        List<Conflict> conflicts = Collections.singletonList(new Conflict("", "Invalid org unit ID: SxgCPPeiq3c_"));
        importSummaries = Collections.singletonList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), conflicts, referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(trackedEntityResponse);
        when(trackedEntityResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenThrow(new SQLException());

        mockStatic(TEIUtil.class);
        patientUIDMap = new LinkedHashMap<>();
        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);
        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }
}