package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.DataElement;
import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.Pager;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttribute;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
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
import org.slf4j.Logger;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({EventUtil.class, TEIUtil.class})
public class DHISMetaDataServiceTest {
    public static final String DATA_ELEMENT = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";
    public static final String TRACKED_ENTITY_ATTRIBUTE = "/api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME";
    private DHISMetaDataService dhisMetaDataService;

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private Logger logger;

    @Mock
    private Pager pager;

    private ResponseEntity<DataElementResponse> dataElementResponseResponse;
    private List<DataElement> dataElements = new LinkedList<>();
    private ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributeResponse;
    private List<TrackedEntityAttribute> trackedEntityAttributes = new LinkedList<>();
    private String dhis2Url = "http://play.dhis2.org";

    @Before
    public void setUp() throws Exception {
        dhisMetaDataService = new DHISMetaDataService();
        setValuesForMemberFields(dhisMetaDataService, "syncRepository", syncRepository);
        setValuesForMemberFields(dhisMetaDataService, "dhis2Url", dhis2Url);
        setValuesForMemberFields(dhisMetaDataService, "logger", logger);

        dataElements.add(new DataElement("asfasdfs", "date"));
        dataElements.add(new DataElement("asfasdfs", "time"));

        trackedEntityAttributes.add(new TrackedEntityAttribute("dfgdfd", "date"));
        trackedEntityAttributes.add(new TrackedEntityAttribute("okpfgf", "time"));

        mockStatic(EventUtil.class);
        mockStatic(TEIUtil.class);
    }

    @Test
    public void shouldGetDateTimeAttributesAndDataElementsInfoFromDHIS() throws Exception {
        dataElementResponseResponse = ResponseEntity.ok(new DataElementResponse(new Pager(), dataElements));
        trackedEntityAttributeResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(new Pager(), trackedEntityAttributes));

        when(syncRepository.getDataElements(dhis2Url + DATA_ELEMENT)).thenReturn(dataElementResponseResponse);
        when(syncRepository.getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE)).thenReturn(trackedEntityAttributeResponse);

        dhisMetaDataService.filterByTypeDateTime();

        verify(syncRepository, times(1)).getDataElements(dhis2Url + DATA_ELEMENT);
        verify(syncRepository, times(1)).getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE);

        verifyStatic(times(1));
        EventUtil.setElementsOfTypeDateTime(Arrays.asList("asfasdfs", "asfasdfs"));
        verifyStatic(times(1));
        TEIUtil.setAttributeOfTypeDateTime(Arrays.asList("dfgdfd", "okpfgf"));
    }

    @Test
    public void shouldHaveEmptyListWhenDHISDoesNotHaveAnyDateTimeAttributesAndDataElements() throws Exception {
        when(syncRepository.getDataElements(dhis2Url + DATA_ELEMENT)).thenReturn(null);
        when(syncRepository.getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE)).thenReturn(null);

        dhisMetaDataService.filterByTypeDateTime();

        verify(syncRepository, times(1)).getDataElements(dhis2Url + DATA_ELEMENT);
        verify(syncRepository, times(1)).getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE);
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 0 Data Elements of type: /api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME");
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 0 Tracked Entity Attributes of type: /api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME");

        verifyStatic(times(1));
        EventUtil.setElementsOfTypeDateTime(new ArrayList<>());
        verifyStatic(times(1));
        TEIUtil.setAttributeOfTypeDateTime(new LinkedList<>());
    }

    @Test
    public void shouldMakeCallToDHISAgainIfTheNextPageIsNotNullForDataElements() throws Exception {
        List<DataElement> secondPageDataElements = new LinkedList<>();
        secondPageDataElements.add(new DataElement("newDataElement", "dateofbirth"));
        String dataElementsSecondPage = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME&page=2";

        dataElementResponseResponse = ResponseEntity.ok(new DataElementResponse(pager, dataElements));
        trackedEntityAttributeResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(new Pager(), trackedEntityAttributes));
        ResponseEntity<DataElementResponse> secondDataElementResponse = ResponseEntity.ok(new DataElementResponse(new Pager(), secondPageDataElements));

        when(syncRepository.getDataElements(dhis2Url + DATA_ELEMENT)).thenReturn(dataElementResponseResponse);
        when(syncRepository.getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE)).thenReturn(trackedEntityAttributeResponse);
        when(pager.getNextPage()).thenReturn(dhis2Url + dataElementsSecondPage);
        when(syncRepository.getDataElements(dhis2Url + dataElementsSecondPage)).thenReturn(secondDataElementResponse);

        dhisMetaDataService.filterByTypeDateTime();

        verify(syncRepository, times(1)).getDataElements(dhis2Url + DATA_ELEMENT);
        verify(syncRepository, times(1)).getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE);
        verify(syncRepository, times(1)).getDataElements(dhis2Url + dataElementsSecondPage);
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 3 Data Elements of type: /api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME");
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 2 Tracked Entity Attributes of type: /api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME");

        verifyStatic(times(1));
        EventUtil.setElementsOfTypeDateTime(Arrays.asList("asfasdfs", "asfasdfs", "newDataElement"));
        verifyStatic(times(1));
        TEIUtil.setAttributeOfTypeDateTime(Arrays.asList("dfgdfd", "okpfgf"));
    }


    @Test
    public void shouldMakeCallToDHISAgainIfTheNextPageIsNotNullForAttributes() throws Exception {
        List<TrackedEntityAttribute> secondPageTrackedEntityAttributes = new LinkedList<>();
        secondPageTrackedEntityAttributes.add(new TrackedEntityAttribute("newAttr", "dateOfRegister"));
        String attributesSecondPage = "/api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME&page=2";

        dataElementResponseResponse = ResponseEntity.ok(new DataElementResponse(new Pager(), dataElements));
        trackedEntityAttributeResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(pager, trackedEntityAttributes));
        ResponseEntity<TrackedEntityAttributeResponse> secondAttrResponse = ResponseEntity.ok(new TrackedEntityAttributeResponse(new Pager(), secondPageTrackedEntityAttributes));

        when(syncRepository.getDataElements(dhis2Url + DATA_ELEMENT)).thenReturn(dataElementResponseResponse);
        when(syncRepository.getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE)).thenReturn(trackedEntityAttributeResponse);
        when(pager.getNextPage()).thenReturn(dhis2Url + attributesSecondPage);
        when(syncRepository.getTrackedEntityAttributes(dhis2Url + attributesSecondPage)).thenReturn(secondAttrResponse);

        dhisMetaDataService.filterByTypeDateTime();

        verify(syncRepository, times(1)).getDataElements(dhis2Url + DATA_ELEMENT);
        verify(syncRepository, times(1)).getTrackedEntityAttributes(dhis2Url + TRACKED_ENTITY_ATTRIBUTE);
        verify(syncRepository, times(1)).getTrackedEntityAttributes(dhis2Url + attributesSecondPage);
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 2 Data Elements of type: /api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME");
        verify(logger, times(1)).info("DHIS2 Metadata Service: Received 3 Tracked Entity Attributes of type: /api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME");

        verifyStatic(times(1));
        EventUtil.setElementsOfTypeDateTime(Arrays.asList("asfasdfs", "asfasdfs"));
        verifyStatic(times(1));
        TEIUtil.setAttributeOfTypeDateTime(Arrays.asList("dfgdfd", "okpfgf", "newAttr"));
    }
}
