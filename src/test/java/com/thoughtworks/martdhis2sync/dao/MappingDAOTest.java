package com.thoughtworks.martdhis2sync.dao;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.*;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchUtil.class})
public class MappingDAOTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private Resource searchableResource;

    private MappingDAO mappingDAO;
    private Map<String, Object> expectedMapping;
    private String getMappingSql;
    private String mappingName;

    @Before
    public void setUp() throws Exception {
        mappingDAO = new MappingDAO();
        setValuesForMemberFields(mappingDAO, "jdbcTemplate", jdbcTemplate);
        setValuesForMemberFields(mappingDAO, "searchableResource", searchableResource);

        mappingName = "Patient Identifier Details";
        getMappingSql = "SELECT lookup_table, mapping_json, config FROM mapping WHERE mapping_name='"+ mappingName +"'";
        expectedMapping = new HashMap<>();
        expectedMapping.put("lookup_table", "{\"instance\": \"patient_identifier\", \"enrollments\": \"patient_enrollments\"}");
        expectedMapping.put("config", "{\"searchable\": [\"UIC\", \"date_created\"]}");
        expectedMapping.put("mapping_json", "{\"instance\": " +
                "{" +
                "\"patient_id\": \"HF8Tu4tg\"" +
                "}" +
                "}");
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldReturnMappingDetails() {

        when(jdbcTemplate.queryForMap(getMappingSql)).thenReturn(expectedMapping);

        Map<String, Object> actual = mappingDAO.getMapping(mappingName);

        assertEquals(expectedMapping, actual);
        verify(jdbcTemplate, times(1)).queryForMap(getMappingSql);
    }

    @Test
    public void shouldReturnSearchableRecord() throws IOException {
        List<Map<String, Object>> actual;
        Map<String, Object> record1 = new HashMap<>();
        Map<String, Object> record2 = new HashMap<>();

        String sql = "SELECT %s " +
                "FROM %s pi " +
                "INNER JOIN marker m ON pi.date_created :: TIMESTAMP > COALESCE(m.last_synced_date, '-infinity') " +
                "AND category = 'instance' AND program_name = '%s'";

        String actualSql = "SELECT \"UIC\",\"date_created\" " +
                "FROM patient_identifier pi " +
                "INNER JOIN marker m ON pi.date_created :: TIMESTAMP > COALESCE(m.last_synced_date, '-infinity') " +
                "AND category = 'instance' AND program_name = 'Patient Identifier Details'";

        System.out.println(actualSql);

        record1.put("UIC", "NINETU190995MT");
        record1.put("date_created", "2018-11-14 05:10:43.000000");
        record2.put("UIC", "JKAPTA170994MT");
        record2.put("date_created", "2018-11-14 05:10:43.000000");

        List<Map<String, Object>> expected = Arrays.asList(record1, record2);

        when(BatchUtil.convertResourceOutputToString(searchableResource)).thenReturn(sql);
        when(jdbcTemplate.queryForMap(getMappingSql)).thenReturn(expectedMapping);
        when(jdbcTemplate.queryForList(actualSql)).thenReturn(expected);

        actual = mappingDAO.getSearchableFields(mappingName);

        assertEquals(expected, actual);
    }
}