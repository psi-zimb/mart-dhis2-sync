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
import static com.thoughtworks.martdhis2sync.model.Conflict.*;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.junit.Assert.assertEquals;
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

    private static List<String> referenceUIDs = Arrays.asList("iUJiNefaBZ4", "AAQzNZIJcUj", "tkj86gf4mdl");

    private static List<String> patientIDs = Arrays.asList("NAH0000000001", "NAH0000000002", "NAH0000000003", "NAH0000000004");

    private String patient1, patient2, requestBody;

    private List<Object> list;

    private boolean isSyncFailure = false;

    @Before
    public void setUp() throws Exception {
        writer = new TrackedEntityInstanceWriter();

        setValuesForMemberFields(writer, "dataSource", dataSource);
        setValuesForMemberFields(writer, "teiUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "markerUtil", markerUtil);
        setValuesForMemberFields(writer, "programName", "HTS Service");
        setValuesForMemberFields(writer, "user", "administrator");
        setValuesForMemberFields(writer, "isSyncFailure", isSyncFailure);

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

        mockStatic(TEIUtil.class);
    }

    @After
    public void tearDown() throws Exception {
        patientUIDMap.clear();
        isSyncFailure = false;
    }

    @Test
    @SneakyThrows
    public void shouldCallSyncRepoToSendData() {
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

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(markerUtil, times(1))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldNotUpdateInstanceTrackerTableButUpdateMarkerTableAfterSyncingOnlyUpdatedTEIsSuccessfully() {

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

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(1))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldUpdateTrackerAndMarkerTablesOnSuccessfullySyncingNewTEI() {

        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(2);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(preparedStatement, times(2)).executeUpdate();
        verify(markerUtil, times(1))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldHandleSQLException() {

        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenThrow(new SQLException());

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), SQLException.class);
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
    }

    @Test
    @SneakyThrows
    public void shouldLogMissingOrgUnitInTheDHISServerAndSaveImportedTEIButNotUpdateMarkerAndThrowExceptionWhenSyncReturns200StatusWithConflictsInResponse() {
        List<Conflict> conflicts = Collections.singletonList(new Conflict(null, "No org unit ID in tracked entity instance object"));

        ImportSummary importSummaryForNewPatientWithInvalidOrgUnit = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                new ImportCount(0, 0, 0, 0), null, conflicts, null);
        ImportSummary importSummaryForNewPatientWithValidOrgUnit = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1));

        importSummaries = Arrays.asList(importSummaryForNewPatientWithInvalidOrgUnit, importSummaryForNewPatientWithValidOrgUnit);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }
        verify(dataSource, times(1)).getConnection();
        verify(preparedStatement, times(1)).executeUpdate();
        verify(markerUtil, times(0))
                .updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test(expected = Exception.class)
    @SneakyThrows
    public void shouldThrowSyncFailedExceptionWhenRequestFailed() {
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);

        writer.write(list);
    }

    @Test
    @SneakyThrows
    public void shouldLogIncorrectlyConfiguredTrackedEntityTypeInConflictsButNotUpdateMarkerAndThrowExceptionOnSyncFailure409Conflict() {
        List<Conflict> conflicts = Collections.singletonList(
                new Conflict(CONFLICT_OBJ_TEI_TYPE, "Invalid trackedEntityType o0kaqrZa79Y@#&"));

        ImportSummary importSummaryForNewTEIWithInvalidTEIType = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, null);
        ImportSummary importSummaryForUpdatedTEIWithInvalidTEIType = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, referenceUIDs.get(1));

        importSummaries = Arrays.asList(importSummaryForNewTEIWithInvalidTEIType, importSummaryForUpdatedTEIWithInvalidTEIType);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }
        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldUpdateMarkerOnSyncingOnlyUpdatedTEIWithWronglyConfiguredTrackedEntityTypeAndReceiving200SuccessResponse() {
        ImportSummary importSummaryForUpdatedTEIWithInvalidTEIType1 = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(0));
        ImportSummary importSummaryForUpdatedTEIWithInvalidTEIType2 = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1));

        importSummaries = Arrays.asList(importSummaryForUpdatedTEIWithInvalidTEIType1, importSummaryForUpdatedTEIWithInvalidTEIType2);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(1)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldLogIncorrectlyMappedAttributeUIDInConflictsAndNotUpdateTrackerAndMarkerTablesAndThrowExceptionOnSyncFailure409Conflict() {
        List<Conflict> conflicts = Collections.singletonList(
                new Conflict(CONFLICT_OBJ_ATTRIBUTE, "Invalid attribute rOb34aQ&$#F"));

        ImportSummary importSummaryForNewPatient = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, null);
        ImportSummary importSummaryForUpdatedPatient = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, referenceUIDs.get(1));

        importSummaries = Arrays.asList(importSummaryForNewPatient, importSummaryForUpdatedPatient);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }
        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldLogMissingOrgUnitInTheDHISServerInConflictsWithoutUpdatingTrackerAndMarkerTablesAndThrowExceptionOnSyncFailure409Conflict() {
        List<Conflict> conflicts = Collections.singletonList(
                new Conflict(CONFLICT_OBJ_ORG_UNIT, "Org unit null does not exist"));
        importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), null, conflicts, referenceUIDs.get(0)),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), null, conflicts, null));

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(1), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldLogIncorrectlyDataTypeForAttributeValueInConflictsButNotUpdateMarkerAndThrowExceptionOnSyncFailure409Conflict() {
        List<Conflict> conflicts = Collections.singletonList(
                new Conflict(CONFLICT_OBJ_ATTRIBUTE_VALUE, "Value 'UIC' is not a valid numeric type for attribute rOb84aQKSyC"));

        ImportSummary importSummaryForNewPatientWithWrongDataType = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, null);
        ImportSummary importSummaryForUpdatedPatientWithWrongDataType = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, conflicts, referenceUIDs.get(1));

        importSummaries = Arrays.asList(importSummaryForNewPatientWithWrongDataType, importSummaryForUpdatedPatientWithWrongDataType);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(1));
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }
        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldLogMultipleConflictsButNotUpdateMarkerAndThrowExceptionOnSyncFailure409Conflict() {
        String patient3 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03051886\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"413\"}" +
                "]}";

        String patient4 = "{\"trackedEntity\": \"%teUID\", " +
                "\"trackedEntityInstance\": \"\", " +
                "\"orgUnit\":\"orgUnitUID\"," +
                "\"attributes\":[" +
                "{\"attribute\": \"rOb34aQLSyC\", \"value\": \"GM03051886\"}" +
                "{\"attribute\": \"rOb2gUg43\", \"value\": \"413\"}" +
                "]}";

        list = Arrays.asList(patient1, patient2, patient3, patient4);

        requestBody = "{\"trackedEntityInstances\":[" + patient1 + "," + patient2 + "," + patient3 + "," + patient4 + "]}";
        List<Conflict> newTEIconflicts = Arrays.asList(
                new Conflict(CONFLICT_OBJ_ATTRIBUTE, "Invalid attribute rOb34aQ&$#F"),
                new Conflict(CONFLICT_OBJ_ORG_UNIT, "Org unit null does not exist"),
                new Conflict(CONFLICT_OBJ_TEI_TYPE, "Invalid trackedEntityType o0kaqrZa79Y@#&"));
        List<Conflict> updatedTEIconflicts = Arrays.asList(
                new Conflict(CONFLICT_OBJ_ATTRIBUTE_VALUE, "Value 'UIC' is not a valid numeric type for attribute rOb84aQKSyC"),
                new Conflict(CONFLICT_OBJ_ATTRIBUTE, "Invalid attribute rOb34aQ&$#F"),
                new Conflict(CONFLICT_OBJ_ORG_UNIT, "Org unit null does not exist"));

        ImportSummary importSummaryForNewPatient = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, newTEIconflicts, null);
        ImportSummary importSummaryForUpdatedPatient = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                new ImportCount(0, 0, 1, 0), null, updatedTEIconflicts, referenceUIDs.get(0));
        ImportSummary importSummaryForUpdatedPatientWithoutConflicts = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), referenceUIDs.get(1));
        ImportSummary importSummaryForNewPatientWithoutConflicts = new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), referenceUIDs.get(2));

        importSummaries = Arrays.asList(importSummaryForNewPatient, importSummaryForUpdatedPatient,
                importSummaryForUpdatedPatientWithoutConflicts, importSummaryForNewPatientWithoutConflicts);

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        patientUIDMap.put(patientIDs.get(0), EMPTY_STRING);
        patientUIDMap.put(patientIDs.get(1), referenceUIDs.get(0));
        patientUIDMap.put(patientIDs.get(2), referenceUIDs.get(1));
        patientUIDMap.put(patientIDs.get(3), EMPTY_STRING);
        when(TEIUtil.getPatientIdTEIUidMap()).thenReturn(patientUIDMap);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }
        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(preparedStatement, times(1)).executeUpdate();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
    }
}