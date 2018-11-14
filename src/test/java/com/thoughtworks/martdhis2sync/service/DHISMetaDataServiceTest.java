package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({EventUtil.class, TEIUtil.class})
public class DHISMetaDataServiceTest {
    private DHISMetaDataService dhisMetaDataService;

    @Mock
    private SyncRepository syncRepository;

    private ResponseEntity<DataElementResponse> dataElementResponseResponse;
    private List<DataElement> dataElements = new LinkedList<>();
    private ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributeResponse;
    private List<TrackedEntityAttribute> trackedEntityAttributes = new LinkedList<>();

    @Before
    public void setUp() throws Exception {
        dhisMetaDataService = new DHISMetaDataService();
        setValuesForMemberFields(dhisMetaDataService, "syncRepository", syncRepository);

        dataElements.add(new DataElement("asfasdfs", "date"));
        dataElements.add(new DataElement("asfasdfs", "time"));

        trackedEntityAttributes.add(new TrackedEntityAttribute("dfgdfd", "date"));
        trackedEntityAttributes.add(new TrackedEntityAttribute("okpfgf", "time"));

        mockStatic(EventUtil.class);
        mockStatic(TEIUtil.class);
    }

    @Test
    public void shouldGetDataElementInfoFromDHIS() {
        dataElementResponseResponse = ResponseEntity.ok(new DataElementResponse(new Pager(), dataElements));
        trackedEntityAttributeResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(new Pager(), trackedEntityAttributes));

        when(syncRepository.getDataElements("")).thenReturn(dataElementResponseResponse);
        when(syncRepository.getTrackedEntityAttributes("")).thenReturn(trackedEntityAttributeResponse);

        dhisMetaDataService.filterByTypeDateTime();

        verify(syncRepository, times(1)).getDataElements("");
        verify(syncRepository, times(1)).getTrackedEntityAttributes("");
        verifyStatic(times(1));
        EventUtil.setElementsOfTypeDateTime(Arrays.asList("asfasdfs", "asfasdfs"));
        verifyStatic(times(1));
        TEIUtil.setAttributeOfTypeDateTime(Arrays.asList("dfgdfd", "okpfgf"));
    }
}
