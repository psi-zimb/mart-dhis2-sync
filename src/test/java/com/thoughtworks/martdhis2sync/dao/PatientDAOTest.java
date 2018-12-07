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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BatchUtil.class)
public class PatientDAOTest {
    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private Resource resource;

    private PatientDAO patientDAO;
    private String enrollmentTable = "enrollment_table";
    private String programName = "hts";

    @Before
    public void setUp() throws Exception {
        patientDAO = new PatientDAO();

        setValuesForMemberFields(patientDAO, "deltaInstances", resource);
        setValuesForMemberFields(patientDAO, "jdbcTemplate", jdbcTemplate);
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldThrowErrorWhenFileCannotConvertToString() throws IOException {
        when(BatchUtil.convertResourceOutputToString(resource))
                .thenThrow(new IOException("Could not convert sql file to string"));

        try {
            patientDAO.getDeltaInstanceIds(enrollmentTable, programName);
        } catch (Exception e) {
            verifyStatic(times(1));
            BatchUtil.convertResourceOutputToString(resource);
            assertEquals("Error in converting sql to string:: Could not convert sql file to string", e.getMessage());
        }
    }

    @Test
    public void shouldReturnInstanceIds() throws Exception {
        String sql = "SELECT * FROM %s et WHERE program_name=%s";
        String formattedSql = "SELECT * FROM enrollment_table et WHERE program_name=hts";
        Map<String, Object> instance1= new HashMap<>();
        instance1.put("instance_id", "instance1");
        Map<String, Object> instance2= new HashMap<>();
        instance1.put("instance_id", "instance2");

        List<Map<String, Object>> expected = Arrays.asList(instance1, instance2);

        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        when(jdbcTemplate.queryForList(formattedSql)).thenReturn(expected);

        List<Map<String, Object>> actual = patientDAO.getDeltaInstanceIds(enrollmentTable, programName);

        verifyStatic(times(1));
        BatchUtil.convertResourceOutputToString(resource);
        verify(jdbcTemplate, times(1)).queryForList(formattedSql);
        assertEquals(expected, actual);
    }
}
