package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class EventWriterTest {

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private ResponseEntity responseEntity;

    @Mock
    private MarkerUtil markerUtil;

    private EventWriter writer;

    private List<String> list = new ArrayList<>();

    private String requestBody;

    private String uri = "localhost/api/events";

    @Before
    public void setUp() throws Exception {
        writer = new EventWriter();

        setValuesForMemberFields(writer, "eventUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);
        setValuesForMemberFields(writer, "markerUtil", markerUtil);

        String enrollments1 = "{\"event\": \"\", " +
                "\"trackedEntityInstance\": \"we4FSLEGq\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \"LAfjIOne\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"dataValues\":[" +
                "{\"dataElement\": \"JDuBC\", \"value\": \"12\"}" +
                "]}";

        String enrollments2 = "{\"event\": \"\", " +
                "\"trackedEntityInstance\": \"we4FSLEGq\", " +
                "\"enrollment\": \"JsFDLAwe\", " +
                "\"program\": \"LAfjIOne\", " +
                "\"programStage\": \"LDLJuyNm\", " +
                "\"orgUnit\":\"LoHtOW\", " +
                "\"dataValues\":[" +
                "{\"dataElement\": \"JDuBC\", \"value\": \"34\"}" +
                "]}";

        list = Arrays.asList(enrollments1, enrollments2);

        requestBody = "{\"events\":[" + enrollments1 + "," + enrollments2 + "]}";

        mockStatic(EnrollmentUtil.class);
    }

    @Test
    public void shouldCallSyncRepoToSendData() {
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }

    @Test
    public void shouldUpdateMarkerAfterSuccessfulSync() {
        when(syncRepository.sendData(uri, requestBody)).thenReturn(responseEntity);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
        verify(markerUtil, times(1)).updateMarkerEntry(anyString(), anyString(), anyString());
    }

}