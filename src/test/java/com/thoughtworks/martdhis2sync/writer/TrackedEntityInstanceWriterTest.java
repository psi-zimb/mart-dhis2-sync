package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;

@RunWith(PowerMockRunner.class)
public class TrackedEntityInstanceWriterTest {

    @Mock
    private SyncRepository syncRepository;

    private TrackedEntityInstanceWriter writer;

    private String uri = "/api/trackedEntityInstance";

    @Before
    public void setUp() throws Exception {
        writer = new TrackedEntityInstanceWriter();
        setValuesForMemberFields(writer, "teiUri", uri);
        setValuesForMemberFields(writer, "syncRepository", syncRepository);
    }

    @Test
    public void shouldCallSyncRepoToSendData() {
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

        List<Object> list = Arrays.asList(patient1, patient2);

        String requestBody = "{\"trackedEntityInstances\":[" + patient1 + "," + patient2 + "]}";
        doNothing().when(syncRepository).sendData(uri, requestBody);

        writer.write(list);

        verify(syncRepository, times(1)).sendData(uri, requestBody);
    }
}