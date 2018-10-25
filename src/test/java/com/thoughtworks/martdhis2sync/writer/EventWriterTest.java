package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
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
import java.util.Collections;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
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

    @Mock
    private LoggerService loggerService;

    private EventWriter writer;

    private String uri = "/api/events?strategy=CREATE_AND_UPDATE";

    @Before
    public void setUp() throws Exception {
        writer = new EventWriter();

        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "markerUtil", markerUtil);
        setValuesForMemberFields(writer, "dataSource", dataSource);
        setValuesForMemberFields(writer, "logger", logger);
        setValuesForMemberFields(writer, "loggerService", loggerService);
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
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 0, 1, 0), null, new ArrayList<>(), "qsYuLK"),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), null, new ArrayList<>(), "jldDj34S"));


        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(markerUtil, times(1)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

    @Test
    @SneakyThrows
    public void shouldNotUpdateTrackerTableAfterSendingOnlyUpdatedEnrollmentsInSync() {
        String event1 = getEventRequestBody("lJKjgRi", "we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("pwHIoSl", "lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("qsYuLK", "alRfLwm", "rleFtLk", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("DJwiAlu", "LIfnHys", "rleFtLk", "1", "ofdlLjfd");
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
        String event1 = getEventRequestBody("", "we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("", "lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk", "1", "ofdlLjfd");
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
        String event1 = getEventRequestBody("", "we4FSLEGq", "LAfjIOne");
        String event2 = getEventRequestBody("", "lejUhau", "LAfjIOne");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk", "1", "ofdlLjfd");
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
            assertEquals(SQLException.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
    }

    @Test
    @SneakyThrows
    public void shouldLogDescriptionAndThrowExceptionWhenRequestBodyHasIncorrectProgramForNewEvents() {
        String expected = "Event.program does not point to a valid program: rleFtLk_1";
        String event1 = getEventRequestBody("", "we4FSLEGq", "incorrectProgram");
        String event2 = getEventRequestBody("", "lejUhau", "incorrectProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk_1", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("", "LIfnHys", "rleFtLk_1", "1", "ofdlLjfd");
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
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(logger, times(2)).error("EVENT SYNC: Event.program does not point to a valid program: rleFtLk_1");
        verify(loggerService, times(2)).collateLogMessage(expected);
    }

    @Test
    @SneakyThrows
    public void shouldUpdateReferencesForFirstEventAndLogDescriptionForSecondEventWhenFirstEventHasCorrectProgramAndSecondHasIncorrectProgram() {
        String expected = "Event.program does not point to a valid program: incorrectProgram";
        String event1 = getEventRequestBody("", "we4FsLEGq", "correctProgram");
        String event2 = getEventRequestBody("", "lejUhQu", "incorrectProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "we4FsLEGq", "correctProgram", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("", "lejUhQu", "incorrectProgram", "1", "ofdlLjfd");
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, null, "LInfWmd"),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: incorrectProgram", null, null)
        );
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
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
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(preparedStatement, times(1)).setString(1, "LInfWmd");
        verify(preparedStatement, times(1)).setString(2, "we4FsLEGq");
        verify(preparedStatement, times(1)).setString(3, "correctProgram");
        verify(logger, times(1)).error("EVENT SYNC: Event.program does not point to a valid program: incorrectProgram");
        verify(loggerService, times(1)).collateLogMessage(expected);
    }

    @Test
    @SneakyThrows
    public void shouldUpdateReferencesForSecondEventAndLogDescriptionForFirstEventWhenFirstEventHasIncorrectProgramAndSecondHasCorrectProgram() {
        String expected = "Event.program does not point to a valid program: incorrectProgram";
        String event1 = getEventRequestBody("", "we4FsLEGq", "incorrectProgram");
        String event2 = getEventRequestBody("", "lejUhQu", "correctProgram");
        List<String> list = Arrays.asList(event1, event2);
        String requestBody = "{\"events\":[" + event1 + "," + event2 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "we4FsLEGq", "incorrectProgram", "1", "ofdlLjfd");
        EventTracker eventTracker2 = new EventTracker("", "lejUhQu", "correctProgram", "1", "ofdlLjfd");
        List<EventTracker> eventTrackers = Arrays.asList(eventTracker1, eventTracker2);
        List<ImportSummary> importSummaries = Arrays.asList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), "Event.program does not point to a valid program: incorrectProgram", null, null),
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), null, null, "LInfWmd")
        );
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
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
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(preparedStatement, times(1)).setString(1, "LInfWmd");
        verify(preparedStatement, times(1)).setString(2, "lejUhQu");
        verify(preparedStatement, times(1)).setString(3, "correctProgram");
        verify(logger, times(1)).error("EVENT SYNC: Event.program does not point to a valid program: incorrectProgram");
        verify(loggerService, times(1)).collateLogMessage(expected);
    }

    @Test
    @SneakyThrows
    public void shouldLogConflictMessageAndDoNotUpdateTrackerWhenTheResponseHasErrorWithConfilct() {
        String expected = "jfDdErl: value_not_true_only";
        String event1 = getEventRequestBody("", "we4FsLEGq", "correctProgram");
        List<String> list = Collections.singletonList(event1);
        String requestBody = "{\"events\":[" + event1 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "we4FsLEGq", "correctProgram", "1", "ofdlLjfd");
        List<EventTracker> eventTrackers = Collections.singletonList(eventTracker1);
        List<Conflict> conflicts = Collections.singletonList(new Conflict("jfDdErl", "value_not_true_only"));
        List<ImportSummary> importSummaries = Collections.singletonList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 1, 0), null, conflicts, null)
        );

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenReturn(connection);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(logger, times(1)).error("EVENT SYNC: jfDdErl: value_not_true_only");
        verify(loggerService, times(1)).collateLogMessage(expected);
    }

    @Test
    @SneakyThrows
    public void shouldLogConflictMessageAndUpdateTrackerWhenResponseHasWarning() {
        String event1 = "{\"event\": \"\", " +
                "\"trackedEntityInstance\": \"alsHFEo\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \"NGldOrFl\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"eventDate\": \"2018-09-24\", " +
                "\"status\": \"ACTIVE\"" +
                "\"dataValues\":[" +
                "{\"dataElement\": \"JDuBC\", \"value\": \"12\"}, " +
                "{\"dataElement\": \"LUfnWeJ\", \"value\": false}" +
                "]}";
        List<String> list = Collections.singletonList(event1);
        String requestBody = "{\"events\":[" + event1 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "wF4FsLEGq", "correctProgram", "1", "ofdlLjfd");
        List<EventTracker> eventTrackers = Collections.singletonList(eventTracker1);
        List<Conflict> conflicts = Collections.singletonList(new Conflict("jfDdErl", "value_not_true_only"));
        List<ImportSummary> importSummaries = Collections.singletonList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_WARNING,
                        new ImportCount(1, 0, 1, 0), null, conflicts, "alEfNBui")
        );
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(sqlQuery)).thenReturn(preparedStatement);
        doNothing().when(preparedStatement).setString(1, "alEfNBui");
        doNothing().when(preparedStatement).setString(2, "wF4FsLEGq");
        doNothing().when(preparedStatement).setString(3, "correctProgram");
        when(preparedStatement.executeUpdate()).thenReturn(1);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(1)).getConnection();
        verify(preparedStatement, times(1)).setString(1, "alEfNBui");
        verify(preparedStatement, times(1)).setString(2, "wF4FsLEGq");
        verify(preparedStatement, times(1)).setString(3, "correctProgram");
        verify(logger, times(1)).error("EVENT SYNC: jfDdErl: value_not_true_only");
        verify(loggerService, times(1)).collateLogMessage("jfDdErl: value_not_true_only");
    }

    @Test
    @SneakyThrows
    public void shouldLogDescriptionAndThrowExceptionWhenRequestBodyHasIncorrectDataElement() {
        String expected = "Data element gXNu7zJBTDN__ doesn't exist in the system. Please, provide correct data element";
        String event1 = "{\"event\": \"\", " +
                "\"trackedEntityInstance\": \"we4FSLEGq\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \"incorrectProgram\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"eventDate\": \"2018-09-24\", " +
                "\"status\": \"ACTIVE\"" +
                "\"dataValues\":[" +
                "{\"dataElement\": \"gXNu7zJBTDN__\", \"value\": \"12\"}" +
                "]}";
        List<String> list = Collections.singletonList(event1);
        String requestBody = "{\"events\":[" + event1 + "]}";
        EventTracker eventTracker1 = new EventTracker("", "alRfLwm", "rleFtLk_1", "1", "ofdlLjfd");
        List<EventTracker> eventTrackers = Collections.singletonList(eventTracker1);
        List<ImportSummary> importSummaries = Collections.singletonList(
                new ImportSummary("", IMPORT_SUMMARY_RESPONSE_ERROR,
                        new ImportCount(0, 0, 0, 0),
                        "Data element gXNu7zJBTDN__ doesn't exist in the system. Please, provide correct data element",
                        null,
                        "")
        );

        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(responseEntity.getStatusCode()).thenReturn(HttpStatus.CONFLICT);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);

        when(EventUtil.getEventTrackers()).thenReturn(eventTrackers);

        try {
            writer.write(list);
        } catch (Exception e) {
            assertEquals(Exception.class, e.getClass());
        }

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
        verify(markerUtil, times(0)).updateMarkerEntry(anyString(), anyString(), anyString());
        verify(logger, times(1)).error("EVENT SYNC: Data element gXNu7zJBTDN__ doesn't exist in the system. Please, provide correct data element");
        verify(loggerService, times(1)).collateLogMessage(expected);
    }

    private String getEventRequestBody(String event, String tei, String program) {
        return "{\"event\": \"" + event + "\", " +
                "\"trackedEntityInstance\": \"" + tei + "\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \"" + program + "\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"eventDate\": \"2018-09-24\", " +
                "\"status\": \"ACTIVE\"" +
                "\"dataValues\":[" +
                "{\"dataElement\": \"JDuBC\", \"value\": \"12\"}" +
                "]}";
    }
}
