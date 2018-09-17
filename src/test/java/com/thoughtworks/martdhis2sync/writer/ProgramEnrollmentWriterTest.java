package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.model.TrackedEntityResponse;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class ProgramEnrollmentWriterTest {
    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity<TrackedEntityResponse> responseEntity;

    private ProgramEnrollmentWriter writer;

    String requestBody = "";

    List<String> list = new ArrayList<>();
    String uri = "/api/enrollments";

    @Before
    public void setUp() throws Exception {
        writer = new ProgramEnrollmentWriter();

        setValuesForMemberFields(writer, "programEnrollUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);

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

        mockStatic(TEIUtil.class);
    }


    @Test
    public void shouldCallSyncRepoToSendData() {
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

}