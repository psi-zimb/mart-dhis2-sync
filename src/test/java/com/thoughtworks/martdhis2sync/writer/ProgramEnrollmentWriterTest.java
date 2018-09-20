package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
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
import static com.thoughtworks.martdhis2sync.model.ImportSummary.RESPONSE_SUCCESS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EnrollmentUtil.class})
@PowerMockIgnore("javax.management.*")
public class ProgramEnrollmentWriterTest {
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
    private List<Enrollment> enrollmentsList = new ArrayList<>();

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;


    private ProgramEnrollmentWriter writer;

    private static final String uri = "/api/trackedEntityInstance";

    private static final String EMPTY_STRING = "\"\"";

    private static List<String> referenceUIDs = Arrays.asList("enrollmentABC", "enrollmentXYZ");

    private static List<String> instanceIDs = Arrays.asList("instanceABC", "instanceXYZ");

    private String requestBody;

    private List<Object> list;

    private String programName = "HTS Service";

    @Before
    public void setUp() throws Exception {
        writer = new ProgramEnrollmentWriter();

        setValuesForMemberFields(writer, "programEnrollUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "dataSource", dataSource);

        String enrollments1 = "{\n" +
                "    \"enrollment\": \"\",\n" +
                "    \"trackedEntityInstance\": \"tm02QkL2wJP\",\n" +
                "    \"orgUnit\": \"P3nulPOaMey\",\n" +
                "    \"program\": \"aHoRX5uGMLU\",\n" +
                "    \"enrollmentDate\": \"2018-09-14\",\n" +
                "    \"incidentDate\": \"2018-09-14\",\n" +
                "    \"status\": \"ACTIVE\"\n" +
                "  }";

        String enrollments2 = "{\n" +
                "    \"enrollment\": \"\",\n" +
                "    \"trackedEntityInstance\": \"QBXN2VK4uPV\",\n" +
                "    \"orgUnit\": \"P3nulPOaMey\",\n" +
                "    \"program\": \"aHoRX5uGMLU\",\n" +
                "    \"enrollmentDate\": \"2018-09-14\",\n" +
                "    \"incidentDate\": \"2018-09-14\",\n" +
                "    \"status\": \"ACTIVE\"\n" +
                "  }";

        list = Arrays.asList(enrollments1, enrollments2);

        requestBody = "{\"enrollments\":[" + enrollments1 + "," + enrollments2 + "]}";

        mockStatic(EnrollmentUtil.class);
    }


    @Test
    public void shouldCallSyncRepoToSendData() {
        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
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
    public void shouldNotUpdateEnrollmentTrackerTableAfterSendingUpdatedEnrollmentsInSync() {

        importSummaries = Arrays.asList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(0, 1, 0, 0), new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        enrollmentsList.add(new Enrollment(referenceUIDs.get(0), instanceIDs.get(0), programName, new Date(), "ACTIVE"));
        enrollmentsList.add(new Enrollment(referenceUIDs.get(1), instanceIDs.get(1), programName, new Date(), "ACTIVE"));
        when(EnrollmentUtil.getEnrollmentsList()).thenReturn(enrollmentsList);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(dataSource, times(0)).getConnection();
    }

    @Test
    @SneakyThrows
    public void shouldSuccessfullyProcessResponse() {

        importSummaries = Arrays.asList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        enrollmentsList.add(new Enrollment(EMPTY_STRING, instanceIDs.get(0), programName, new Date(), "ACTIVE"));
        enrollmentsList.add(new Enrollment(EMPTY_STRING, instanceIDs.get(1), programName, new Date(), "ACTIVE"));
        when(EnrollmentUtil.getEnrollmentsList()).thenReturn(enrollmentsList);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    @SneakyThrows
    public void shouldHandleSQLException() {

        importSummaries = Arrays.asList(
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(0)),
                new ImportSummary("", RESPONSE_SUCCESS,
                        new ImportCount(1, 0, 0, 0), new ArrayList<>(), referenceUIDs.get(1)));

        when(responseEntity.getBody()).thenReturn(DHISSyncResponse);
        when(DHISSyncResponse.getResponse()).thenReturn(response);
        when(response.getImportSummaries()).thenReturn(importSummaries);
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        when(dataSource.getConnection()).thenThrow(new SQLException());

        enrollmentsList.add(new Enrollment(EMPTY_STRING, instanceIDs.get(0), programName, new Date(), "ACTIVE"));
        enrollmentsList.add(new Enrollment(EMPTY_STRING, instanceIDs.get(1), programName, new Date(), "ACTIVE"));
        when(EnrollmentUtil.getEnrollmentsList()).thenReturn(enrollmentsList);

        writer.write(list);
        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

}