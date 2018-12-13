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
    private String eventTable = "event_table";

    @Before
    public void setUp() throws Exception {
        patientDAO = new PatientDAO();

        setValuesForMemberFields(patientDAO, "deltaEnrollmentInstances", resource);
        setValuesForMemberFields(patientDAO, "jdbcTemplate", jdbcTemplate);
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldThrowErrorWhenFileCannotConvertToString() throws IOException {
        when(BatchUtil.convertResourceOutputToString(resource))
                .thenThrow(new IOException("Could not convert sql file to string"));

        try {
            patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);
        } catch (Exception e) {
            verifyStatic(times(1));
            BatchUtil.convertResourceOutputToString(resource);
            assertEquals("Error in converting sql to string:: Could not convert sql file to string", e.getMessage());
        }
    }

    @Test
    public void shouldReturnInstanceIds() throws Exception {
        String sql = "SELECT\n" +
                "  COALESCE(enrollmentsTable.program, eventsTable.program)   AS program,\n" +
                "  insTracker.instance_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM %s enrTable\n" +
                "      INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "          AND category = 'enrollment' AND program_name = '%s'\n" +
                "     ) AS enrollmentsTable\n" +
                "FULL OUTER JOIN (SELECT evnTable.*,\n" +
                "                   enrollments.program_unique_id AS event_program_unique_id,\n" +
                "                   enrollments.status            AS event_program_status\n" +
                "                   FROM %s evnTable\n" +
                "                   INNER JOIN %s enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)\n" +
                "                   INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                       AND category = 'event' AND program_name = '%s'\n" +
                "                ) AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "    AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)\n" +
                "INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program\n" +
                "                                          AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                          AND enrolTracker.program_unique_id = COALESCE(enrollmentsTable.program_unique_id, eventsTable.event_program_unique_id) :: TEXT\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED' OR\n" +
                "eventsTable.event_program_status = 'COMPLETED' OR eventsTable.event_program_status = 'CANCELLED')\n";
        String formattedSql = "SELECT\n" +
                "  COALESCE(enrollmentsTable.program, eventsTable.program)   AS program,\n" +
                "  insTracker.instance_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM enrollment_table enrTable\n" +
                "      INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "          AND category = 'enrollment' AND program_name = 'hts'\n" +
                "     ) AS enrollmentsTable\n" +
                "FULL OUTER JOIN (SELECT evnTable.*,\n" +
                "                   enrollments.program_unique_id AS event_program_unique_id,\n" +
                "                   enrollments.status            AS event_program_status\n" +
                "                   FROM event_table evnTable\n" +
                "                   INNER JOIN enrollment_table enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)\n" +
                "                   INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                       AND category = 'event' AND program_name = 'hts'\n" +
                "                ) AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "    AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)\n" +
                "INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program\n" +
                "                                          AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                          AND enrolTracker.program_unique_id = COALESCE(enrollmentsTable.program_unique_id, eventsTable.event_program_unique_id) :: TEXT\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED' OR\n" +
                "eventsTable.event_program_status = 'COMPLETED' OR eventsTable.event_program_status = 'CANCELLED')\n";
        Map<String, Object> instance1= new HashMap<>();
        instance1.put("instance_id", "instance1");
        Map<String, Object> instance2= new HashMap<>();
        instance1.put("instance_id", "instance2");

        List<Map<String, Object>> expected = Arrays.asList(instance1, instance2);

        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        when(jdbcTemplate.queryForList(formattedSql)).thenReturn(expected);

        List<Map<String, Object>> actual = patientDAO.getDeltaEnrollmentInstanceIds(enrollmentTable, eventTable, programName);

        verifyStatic(times(1));
        BatchUtil.convertResourceOutputToString(resource);
        verify(jdbcTemplate, times(1)).queryForList(formattedSql);
        assertEquals(expected, actual);
    }
}
