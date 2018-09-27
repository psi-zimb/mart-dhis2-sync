package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportCount;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.Response;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import lombok.SneakyThrows;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(EventUtil.class)
@PowerMockIgnore("javax.management.*")
public class EventWriterTest {

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<DHISSyncResponse> responseEntity;

    @Mock
    private MarkerUtil markerUtil;

    @Mock
    private DHISSyncResponse DHISSyncResponse;

    @Mock
    private Response response;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Logger logger;

    private EventWriter writer;

    private String uri = "/api/events?strategy=CREATE_AND_UPDATE";

    @Before
    public void setUp() throws Exception {
        writer = new EventWriter();

        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "markerUtil", markerUtil);
        setValuesForMemberFields(writer, "dataSource", dataSource);
        setValuesForMemberFields(writer, "logger", logger);
        mockStatic(EventUtil.class);
    }

    @Test
    public void shouldCallSyncRepoToSendData() throws Exception {
        String event1 = getEventRequestBody("", "we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("", "lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    public void shouldUpdateMarkerAfterSuccessfulSync() throws Exception {
        String event1 = getEventRequestBody("", "we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("", "lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(new ArrayList<>());

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(markerUtil, times(1)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldNotUpdateTrackerTableAfterSendingOnlyUpdatedEnrollmentsInSync() {
        String event1 = getEventRequestBody("lJKjgRi","we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("pwHIoSl","lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("qsYuLK", "alRfLwm", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("DJwiAlu", "LIfnHys", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), eventTracker1.getEventId()),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), eventTracker2.getEventId()));

        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
    }

    @Test
    @SneakyThrows
    public void shouldUpdateTrackerAndMarkerTablesOnSuccessfullySyncingOnlyNewEnrollments() {
        String event1 = getEventRequestBody("","we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("","lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), "JqsYuLK"),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), "DJwiAlu"));

        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(2);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(preparedStatement, times(2)).executeUpdate();
        verify(markerUtil, times(1)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldHandleSQLException() {
        String event1 = getEventRequestBody("","we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("","lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), "JqsYuLK"),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, new ArrayList<>(), "DJwiAlu"));

        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenThrow(new SQLException());

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
    public void shouldLogDescriptionAndThrowExceptionWhenRequestBodyHasIncorrectProgramForNewEvents() {
        String event1 = getEventRequestBody("","we4FSLEGq", "incorrectProgram");
        String event2 = getEventRequestBody("","lejUhau", "incorrectProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk_1", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk_1", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: rleFtLk_1", null, ""),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: rleFtLk_1", null, ""));

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(logger, times(2)).error("EVENT SYNC: Event.program does not point to a valid program: rleFtLk_1");
    }

    @Test
    @SneakyThrows
    public void shouldUpdateReferencesForFirstEventAndLogDescriptionForSecondEventWhenFirstEventHasCorrectProgramAndSecondHasIncorrectProgram() {
        String event1 = getEventRequestBody("","we4FsLEGq", "correctProgram");
        String event2 = getEventRequestBody("","lejUhQu", "incorrectProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "we4FsLEGq", "correctProgram", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("", "lejUhQu", "incorrectProgram", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, null, "LInfWmd"),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: incorrectProgram", null, null)
        );
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_unique_id, program_start_date, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sqlQuery)).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(1, "LInfWmd");
        doNothing().when(preparedStatement).setString(2, "we4FsLEGq");
        doNothing().when(preparedStatement).setString(3, "correctProgram");
        when(preparedStatement.executeUpdate()).thenReturn(1);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(preparedStatement, times(1)).setString(1, "LInfWmd");
        verify(preparedStatement, times(1)).setString(2, "we4FsLEGq");
        verify(preparedStatement, times(1)).setString(3, "correctProgram");
        verify(logger, times(1)).error("EVENT SYNC: Event.program does not point to a valid program: incorrectProgram");
    }

    @Test
    @SneakyThrows
    public void shouldUpdateReferencesForSecondEventAndLogDescriptionForFirstEventWhenFirstEventHasIncorrectProgramAndSecondHasCorrectProgram() {
        String event1 = getEventRequestBody("","we4FsLEGq", "incorrectProgram");
        String event2 = getEventRequestBody("","lejUhQu", "correctProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "we4FsLEGq", "incorrectProgram", 1, new Date(Long.MIN_VALUE));
        EventTracker eventTracker2 = new EventTracker("", "lejUhQu", "correctProgram", 1, new Date(Long.MIN_VALUE));
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: incorrectProgram", null, null),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, null, "LInfWmd")
        );
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_unique_id, program_start_date, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sqlQuery)).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(1, "LInfWmd");
        doNothing().when(preparedStatement).setString(2, "lejUhQu");
        doNothing().when(preparedStatement).setString(3, "correctProgram");
        when(preparedStatement.executeUpdate()).thenReturn(1);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(preparedStatement, times(1)).setString(1, "LInfWmd");
        verify(preparedStatement, times(1)).setString(2, "lejUhQu");
        verify(preparedStatement, times(1)).setString(3, "correctProgram");
        verify(logger, times(1)).error("EVENT SYNC: Event.program does not point to a valid program: incorrectProgram");
    }

    private String getEventRequestBody(String event, String tei, String program) {
        return "{\"event\": \""+ event +"\", " +
                "\"trackedEntityInstance\": \""+ tei +"\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \""+ program +"\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"eventDate\": \"2018-09-24\", " +
                "\"status\": \"ACTIVE\"" +
                "\"dataValues\":[" +
                "{\"dataElement\": \"JDuBC\", \"value\": \"12\"}" +
                "]}";
    }
}