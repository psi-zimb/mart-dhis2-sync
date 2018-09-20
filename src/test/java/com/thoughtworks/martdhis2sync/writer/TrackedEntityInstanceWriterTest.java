package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TEIUtil.class})
@PowerMockIgnore("javax.management.*")
public class TrackedEntityInstanceWriterTest {

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private DataSource dataSource;

    @Mock
    private DHISSyncResponse DHISSyncResponse;

    @Mock
    private ResponseEntity<DHISSyncResponse> responseEntity;

    @Mock
    private Response response;

    @Mock
    private List<ImportSummary> importSummaries;

    @Mock
    private Map<String, String> patientUIDMap = new LinkedHashMap<>();

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private MarkerUtil markerUtil;

    private TrackedEntityInstanceWriter writer;

    private static final String uri = "/api/trackedEntityInstance";

    private static final String EMPTY_STRING = "\"\"";

    private static List<String> referenceUIDs = Arrays.asList("iUJiNefaBZ4", "AAQzNZIJcUj");

    private static List<String> patientIDs = Arrays.asList("NAH0000000001", "NAH0000000002");

    private String requestBody;

    private List<Object> list;

    private String programName = "HTS Service";

    @Before
    public void setUp() throws Exception {
        writer = new TrackedEntityInstanceWriter();

        setValuesForMemberFields(writer, "dataSource", dataSource);
        setValuesForMemberFields(writer, "teiUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "markerUtil", markerUtil);
        setValuesForMemberFields(writer, "programName", programName);
        setValuesForMemberFields(writer, "IS_SYNC_SUCCESS", true);

        String patient1 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03041889\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"412\"}" +
                "]}";

        String patient2 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03051886\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"413\"}" +
                "]}";

        list = Arrays.asList(patient1, patient2);

        requestBody = "{\"trackedEntityInstances\":[" + patient1 + "," + patient2 + "]}";

        mockStatic(TEIUtil.class);
    }

    @After
    public void tearDown() throws Exception {
        patientUIDMap.clear();
    }

    @Test
    public void shouldCallSyncRepoToSendData() throws Exception {
        Date date = new Date(Long.MIN_VALUE);
        String stringDate = "292269055-12-02 22:17:04";
        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        doNothing().when(markerUtil).updateMarkerEntry(anyString(), anyString(), anyString());

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(markerUtil, times(1))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test(expected = Exception.class)
    @SneakyThrows
    public void shouldNotUpdateInstanceTrackerTableAfterSendingUpdatedTEISync() {

        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
    }

    @Test(expected = Exception.class)
    @SneakyThrows
    public void shouldSuccessfullyProcessResponse() {

        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test(expected = Exception.class)
    @SneakyThrows
    public void shouldHandleConflictsInTheResponseMessage() {

        List<Conflict> conflicts = Collections.singletonList(new Conflict("", "Invalid org unit ID: SxgCPPeiq3c_"));
        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, conflicts, null),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test(expected = Exception.class)
    @SneakyThrows
    public void shouldHandleSQLException() {

        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenThrow(new SQLException());

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);
        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test(expected = Exception.class)
    public void shouldNotUpdateMarkerWhenSyncResponseHasConflicts() throws Exception {
        List<Conflict> conflicts = Collections.singletonList(new Conflict("", "Invalid org unit ID: SxgCPPeiq3c_"));
        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 0, 0, 0), null, conflicts, referenceUIDs.get(1)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(list);

        verify(markerUtil, times(0))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test(expected = Exception.class)
    public void shouldThrowSyncFailedExceptionWhenRequestFailed() throws Exception {
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);

        writer.write(list);
    }
}