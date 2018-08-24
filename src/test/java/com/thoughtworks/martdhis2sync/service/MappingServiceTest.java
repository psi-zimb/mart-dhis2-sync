package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class MappingServiceTest {

    @Mock
    private MappingDAO mappingDAO;

    private MappingService mappingService;

    @Before
    public void setUp() throws Exception {
        mappingService = new MappingService();
        setValuesForMemberFields(mappingService, "mappingDAO", mappingDAO);
    }

    @Test
    public void shouldReturnMappingDetailsOnCallToGetMappingOfMappingDAO() {
        String mapping = "HTS Service";
        Map<String, Object> expected = new HashMap<>();
        expected.put("lookup_table", "{\"instance\": \"patient_identifier\", \"enrollments\": \"patient_enrollments\"}");
        expected.put("mapping_json", "{\"instance\": " +
                "{" +
                "\"patient_id\": \"HF8Tu4tg\"" +
                "}" +
                "}");

        when(mappingDAO.getMapping(mapping)).thenReturn(expected);

        Map<String, Object> actual = mappingService.getMapping(mapping);

        assertEquals(expected, actual);
        verify(mappingDAO, times(1)).getMapping(mapping);
    }
}