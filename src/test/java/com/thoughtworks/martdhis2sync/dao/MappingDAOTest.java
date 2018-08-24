package com.thoughtworks.martdhis2sync.dao;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class MappingDAOTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private MappingDAO mappingDAO;

    @Before
    public void setUp() throws Exception {
        mappingDAO = new MappingDAO();
        setValuesForMemberFields(mappingDAO, "jdbcTemplate", jdbcTemplate);
    }

    @Test
    public void shouldReturnMappingDetails() {
        String mapping = "Patient Identifier Details";
        String sql = "SELECT lookup_table, mapping_json FROM mapping WHERE mapping_name='Patient Identifier Details'";
        Map<String, Object> expected = new HashMap<>();
        expected.put("lookup_table", "{\"instance\": \"patient_identifier\", \"enrollments\": \"patient_enrollments\"}");
        expected.put("mapping_json", "{\"instance\": " +
                "{" +
                "\"patient_id\": \"HF8Tu4tg\"" +
                "}" +
            "}");

        when(jdbcTemplate.queryForMap(sql)).thenReturn(expected);

        Map<String, Object> actual = mappingDAO.getMapping(mapping);

        assertEquals(expected, actual);
        verify(jdbcTemplate, times(1)).queryForMap(sql);
    }

}