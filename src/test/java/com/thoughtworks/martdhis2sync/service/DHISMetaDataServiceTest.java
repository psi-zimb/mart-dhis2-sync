package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.MappingDAO;
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

import java.io.IOException;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({EventUtil.class, TEIUtil.class})
public class DHISMetaDataServiceTest {
    public static final String DATA_ELEMENT = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";
    public static final String TRACKED_ENTITY_ATTRIBUTE = "/api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME";
    public static final String TRACKED_ENTITY_INSTANCE_URI = "/api/trackedEntityInstances?pageSize=10000";
    private DHISMetaDataService dhisMetaDataService;

    @Mock
    private SyncRepository syncRepository;

    @Mock
    private MappingDAO mappingDAO;

    private ResponseEntity<DataElementResponse> dataElementResponseResponse;
    private List<DataElement> dataElements = new LinkedList<>();
    private ResponseEntity<TrackedEntityAttributeResponse> trackedEntityAttributeResponse;
    private ResponseEntity<TrackedEntityInstanceResponse> trackedEntityInstanceResponse;
    private List<TrackedEntityInstance> trackedEntityInstances = new LinkedList<>();
    private List<TrackedEntityAttribute> trackedEntityAttributes = new LinkedList<>();
    private String dhis2Url = "http://play.dhis2.org";
    private List<Attribute> attributesOfPatient1 = new LinkedList<>();
    private List<Attribute> attributesOfPatient2 = new LinkedList<>();
    private List<Map<String, Object>> searchableValues = new LinkedList<>();
    private String ORG_UNIT_ID = "DiszpKrYNg8";
    private HashMap<String, Object> expectedMapping;

    @Before
    public void setUp() throws Exception {
        dhisMetaDataService = new DHISMetaDataService();
        setValuesForMemberFields(dhisMetaDataService, "syncRepository", syncRepository);
        setValuesForMemberFields(dhisMetaDataService, "dhis2Url", dhis2Url);
        setValuesForMemberFields(dhisMetaDataService, "mappingDAO", mappingDAO);
        setValuesForMemberFields(dhisMetaDataService, "orgUnitID", ORG_UNIT_ID);

        dataElements.add(new DataElement("asfasdfs", "date"));
        dataElements.add(new DataElement("asfasdfs", "time"));

        trackedEntityAttributes.add(new TrackedEntityAttribute("dfgdfd", "date"));
        trackedEntityAttributes.add(new TrackedEntityAttribute("okpfgf", "time"));

        setSearchableValues();
        setTrackedEntityInstances();
        mockStatic(EventUtil.class);
        mockStatic(TEIUtil.class);
    }

    @Test
    public void shouldGetDataElementInfoFromDHIS() {
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
    public void shouldGetTrackedEntityInstanceFromDHIS() throws IOException {
        String program = "HIV Testing Service";
        String queryParams = "&filter=HF8Tu4tg:IN:NINETU190995MT;JKAPTA170994MT;";
        String url = dhis2Url + TRACKED_ENTITY_INSTANCE_URI + "&ou=" + ORG_UNIT_ID + "&ouMode=DESCENDANTS" + queryParams;
        Map<String, Object> searchableMapping = new HashMap<>();

        trackedEntityInstanceResponse = ResponseEntity.ok(new TrackedEntityInstanceResponse(trackedEntityInstances));

        expectedMapping = new HashMap<>();
        expectedMapping.put("lookup_table", "{\"instance\": \"patient_identifier\", \"enrollments\": \"patient_enrollments\"}");
        expectedMapping.put("config", "{\"searchable\": [\"UIC\", \"date_created\"]}");
        expectedMapping.put("mapping_json", "{\"instance\": " +
                "{" +
                "\"UIC\": \"HF8Tu4tg\"," +
                "\"date_created\": \"ojmUIu4tg\"" +
                "}" +
                "}");
        searchableMapping.put("UIC", "HF8Tu4tg");

        when(mappingDAO.getSearchableFields(program)).thenReturn(searchableValues);
        when(mappingDAO.getMapping(program)).thenReturn(expectedMapping);
        when(syncRepository.getTrackedEntityInstances(url)).thenReturn(trackedEntityInstanceResponse);

        dhisMetaDataService.getTrackedEntityInstances(program);

        verify(mappingDAO, times(1)).getSearchableFields(program);
        verify(syncRepository, times(1)).getTrackedEntityInstances(url);
        verifyStatic(times(1));
        TEIUtil.setTrackedEntityInstances(trackedEntityInstances);
    }

    private void setTrackedEntityInstances() {
        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "***REMOVED***",
                "MMD_PER_NAM",
                "First name",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "w75KJ2mc4zz",
                "Michel"
        ));

        attributesOfPatient1.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "***REMOVED***",
                "",
                "Last name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        trackedEntityInstances.add(new TrackedEntityInstance(
                "2018-09-21T17:54:00.294",
                "SxgCPPeiq3c",
                "2018-09-21T17:54:01.337",
                "w3MoRtzP4SO",
                "2018-09-21T17:54:01.337",
                "o0kaqrZa79Y",
                "2018-09-21T17:54:01.337",
                false,
                false,
                "NONE",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                attributesOfPatient1
        ));

        attributesOfPatient2.add(new Attribute(
                "2018-11-26T09:24:57.158",
                "***REMOVED***",
                "MMD_PER_NAM",
                "First name",
                "2018-11-26T09:24:57.158",
                "TEXT",
                "w75KJ2mc4zz",
                "Jinny"
        ));

        attributesOfPatient2.add(new Attribute(
                "2018-11-26T09:24:57.153",
                "***REMOVED***",
                "",
                "Last name",
                "2018-11-26T09:24:57.152",
                "TEXT",
                "zDhUuAYrxNC",
                "Jackson"
        ));

        trackedEntityInstances.add(new TrackedEntityInstance(
                "2018-09-22T13:24:00.24",
                "SxgCPPeiq3c",
                "2018-09-21T17:54:01.337",
                "tzP4SOw3MoR",
                "2018-09-22T13:24:00.241",
                "o0kaqrZa79Y",
                "2018-09-21T17:54:01.337",
                false,
                false,
                "NONE",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                attributesOfPatient2
        ));
    }

    private void setSearchableValues() {
        Map<String, Object> searchable1 = new HashMap<>();
        searchable1.put("UIC", "NINETU190995MT");
        searchableValues.add(searchable1);

        Map<String, Object> searchable2 = new HashMap<>();
        searchable2.put("UIC", "JKAPTA170994MT");
        searchableValues.add(searchable2);
    }
}
