package com.thoughtworks.martdhis2sync.reader;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MappingReader.class, BatchUtil.class})
@PowerMockIgnore("javax.management.*")
public class MappingReaderTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private JdbcCursorItemReader jdbcCursorItemReader;

    @Mock
    private ColumnMapRowMapper columnMapRowMapper;

    @Mock
    private Resource resource;

    @Mock
    private Logger logger;

    private MappingReader mappingReader;

    private static final String programName = "HTS Service";

    @Before
    public void setUp() throws Exception {
        mappingReader = new MappingReader();
        setValuesForMemberFields(mappingReader, "dataSource", dataSource);
        setValuesForMemberFields(mappingReader, "instanceResource", resource);
        setValuesForMemberFields(mappingReader, "enrollmentResource", resource);
        setValuesForMemberFields(mappingReader, "eventResource", resource);
        setValuesForMemberFields(mappingReader, "newCompletedEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "updatedCompletedEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "newActiveEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "updatedActiveEnrWithEventsResource", resource);
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldReturnInstanceReaderWithDetailsBasedOnInput() throws Exception {
        String lookupTable = "patient_identifier";

        String sql = String.format("SELECT lt.*, CASE WHEN i.instance_id is NULL THEN '' else i.instance_id END as instance_id  " +
                        "FROM patient_identifier lt LEFT join instance_tracker i ON  lt.\"Patient_Identifier\" = i.patient_id " +
                        "WHERE date_created > COALESCE((SELECT last_synced_date\n" +
                        "                                    FROM marker\n" +
                        "                                    WHERE category='instance' AND program_name='%s'), '-infinity');",
                programName);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getInstanceReader(lookupTable, programName);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldLogWhenResourceCanNotBeConvertedToString() throws Exception {
        String lookupTable = "patient_identifier";

        setValuesForMemberFields(mappingReader, "logger", logger);
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        when(BatchUtil.convertResourceOutputToString(resource))
                .thenThrow(new IOException("Could not convert sql file to string"));

        try {
            mappingReader.getInstanceReader(lookupTable, programName);
        } catch (Exception e) {
            verify(logger, times(1))
                    .error("Error in converting sql to string : Could not convert sql file to string");
        }
    }

    @Test
    public void shouldReturnReaderForProgramEnrollment() throws Exception {
        String lookupTable = "programs";

        String sql = String.format("SELECT mappedTable. *, insTracker.instance_id, orgTracker.id as orgunit_id,\n" +
                "  CASE WHEN enrTracker.enrollment_id is NULL THEN '' ELSE enrTracker.enrollment_id END AS enrollment_id\n" +
                        "FROM %s mappedTable\n" +
                        "INNER JOIN instance_tracker insTracker\n" +
                        "  ON insTracker.patient_id = mappedTable.\"Patient_Identifier\"\n" +
                        "INNER JOIN orgunit_tracker orgTracker\n" +
                        "  ON orgTracker.orgUnit = mappedTable.\"OrgUnit\"\n" +
                        "LEFT JOIN enrollment_tracker enrTracker\n" +
                        "  ON mappedTable.program = enrTracker.program\n" +
                        "    AND enrTracker.instance_id = insTracker.instance_id\n" +
                        "    AND enrTracker.program_unique_id = mappedTable.program_unique_id\n" +
                        "  WHERE mappedTable.date_created > COALESCE((SELECT last_synced_date\n" +
                        "                                    FROM marker\n" +
                        "                                    WHERE category='enrollment' AND program_name='%s'), '-infinity');\n",
                        lookupTable, programName);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getEnrollmentReader(lookupTable, programName);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForEvent() throws Exception {
        String lookupTable = "event";
        String enrollmentLookupTable = "enrollment";

        String sql = String.format("SELECT event.*,\n" +
                        "       enrTracker.instance_id,\n" +
                        "       enrTracker.enrollment_id,\n" +
                        "       orgTracker.id as orgunit_id\n" +
                        "FROM %s event\n" +
                        "INNER JOIN %s enrollment ON event.\"Patient_Identifier\" = enrollment.\"Patient_Identifier\"\n" +
                        "INNER JOIN orgunit_tracker orgTracker ON event.\"OrgUnit\" = orgTracker.orgunit\n" +
                        "INNER JOIN instance_tracker insTracker ON event.\"Patient_Identifier\" = insTracker.patient_id\n" +
                        "INNER JOIN enrollment_tracker enrTracker ON insTracker.instance_id = enrTracker.instance_id\n" +
                        "  AND enrollment.program_unique_id = enrTracker.program_unique_id AND event.program = enrTracker.program_name\n" +
                        "LEFT JOIN event_tracker ON insTracker.instance_id = event_tracker.instance_id\n" +
                        "  AND CASE WHEN event.program_unique_id IS NULL THEN date(event.program_start_date)::text ELSE event.program_unique_id END\n" +
                        "      = CASE WHEN event_tracker.program_unique_id IS NULL THEN date(event_tracker.program_start_date)::text ELSE event_tracker.program_unique_id END\n" +
                        "  AND event.program = event_tracker.program\n" +
                        "WHERE event.date_created > COALESCE((SELECT last_synced_date\n" +
                        "  FROM marker\n" +
                        "  WHERE category='event' AND program_name='%s'), '-infinity');",
                        lookupTable, enrollmentLookupTable, programName);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getEventReader(lookupTable, programName, enrollmentLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForNewCompletedEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";

        String sql = String.format("SELECT enrTable.incident_date,\n" +
                        "       enrTable.date_created       AS enrollment_date_created,\n" +
                        "       enrTable.program_unique_id  AS program_unique_id,\n" +
                        "       evnTable.*,\n" +
                        "       orgTracker.id                              AS orgunit_id,\n" +
                        "       insTracker.instance_id\n" +
                        "FROM %s enrTable\n" +
                        "       LEFT JOIN %s evnTable ON evnTable.\"Patient_Identifier\" = enrTable.\"Patient_Identifier\" AND\n" +
                        "                                                      evnTable.enrollment_date = enrTable.enrollment_date\n" +
                        "       INNER JOIN instance_tracker insTracker ON insTracker.patient_id = enrTable.\"Patient_Identifier\"\n" +
                        "       INNER JOIN orgunit_tracker orgTracker ON orgTracker.orgUnit = enrTable.\"OrgUnit\"\n" +
                        "       LEFT JOIN enrollment_tracker enrTracker\n" +
                        "         ON enrTable.program = enrTracker.program AND enrTracker.instance_id = insTracker.instance_id\n" +
                        "              AND enrTracker.program_unique_id = enrTable.program_unique_id :: text\n" +
                        "WHERE enrTable.date_created :: TIMESTAMP > COALESCE((SELECT last_synced_date FROM marker WHERE category = 'enrollment'\n" +
                        "                                                                                           AND program_name = '%s'),\n" +
                        "                                                    '-infinity')\n" +
                        "  AND enrTable.status = 'COMPLETED'\n" +
                        "  AND enrTracker.instance_id IS NULL;\n",
                enrollmentLookupTable, eventLookupTable, programName);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getNewCompletedEnrollmentWithEventsReader(enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForUpdatedCompletedEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        EnrollmentAPIPayLoad enr1 = new EnrollmentAPIPayLoad();
        enr1.setEnrollmentId("NAH0000000009");
        EnrollmentAPIPayLoad enr2 = new EnrollmentAPIPayLoad();
        enr2.setEnrollmentId("NAH0000000004");
        EnrollmentAPIPayLoad enr3 = new EnrollmentAPIPayLoad();
        enr3.setEnrollmentId("NAH0000000004");
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr1);
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr2);
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr3);

        String sql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM %s enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = '%s') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*, enrollments.program_unique_id AS event_program_unique_id\n" +
                "                   FROM %s evnTable\n" +
                "                     INNER JOIN %s enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = enrollments.enrollment_date\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = '%s') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = enrollmentsTable.enrollment_date\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON enrollmentsTable.program = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = enrollmentsTable.program_unique_id :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL %s;\n";

        String formattedSql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM enrollment enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = 'HTS Service') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*, enrollments.program_unique_id AS event_program_unique_id\n" +
                "                   FROM event evnTable\n" +
                "                     INNER JOIN enrollment enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = enrollments.enrollment_date\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = 'HTS Service') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = enrollmentsTable.enrollment_date\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON enrollmentsTable.program = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = enrollmentsTable.program_unique_id :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL AND enrolTracker.enrollment_id NOT IN ('NAH0000000009','NAH0000000004','NAH0000000004');\n";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(formattedSql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(formattedSql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldNotAddAndClauseToTheReaderSqlWhenEnrollmentsToSaveInTrackerIsEmpty() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();

        String sql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM %s enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = '%s') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*, enrollments.program_unique_id AS event_program_unique_id\n" +
                "                   FROM %s evnTable\n" +
                "                     INNER JOIN %s enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = enrollments.enrollment_date\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = '%s') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = enrollmentsTable.enrollment_date\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON enrollmentsTable.program = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = enrollmentsTable.program_unique_id :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL %s;\n";

        String formattedSql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM enrollment enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = 'HTS Service') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*, enrollments.program_unique_id AS event_program_unique_id\n" +
                "                   FROM event evnTable\n" +
                "                     INNER JOIN enrollment enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = enrollments.enrollment_date\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = 'HTS Service') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = enrollmentsTable.enrollment_date\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON enrollmentsTable.program = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = enrollmentsTable.program_unique_id :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL ;\n";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(formattedSql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(formattedSql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldReturnReaderForNewActiveEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";

        String sql = String.format("SELECT enrTable.incident_date,\n" +
                        "       enrTable.date_created       AS enrollment_date_created,\n" +
                        "       enrTable.program_unique_id  AS program_unique_id,\n" +
                        "       enrTable.program            AS enrolled_program,\n" +
                        "       enrTable.enrollment_date    AS enr_date,\n" +
                        "       enrTable.status             AS enrollment_status,\n" +
                        "       enrTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                        "       evnTable.*,\n" +
                        "       orgTracker.id               AS orgunit_id,\n" +
                        "       insTracker.instance_id\n" +
                        "FROM %s enrTable\n" +
                        "       LEFT JOIN %s evnTable ON evnTable.\"Patient_Identifier\" = enrTable.\"Patient_Identifier\" AND\n" +
                        "                                                      evnTable.enrollment_date = enrTable.enrollment_date\n" +
                        "       INNER JOIN instance_tracker insTracker ON insTracker.patient_id = enrTable.\"Patient_Identifier\"\n" +
                        "       INNER JOIN orgunit_tracker orgTracker ON orgTracker.orgUnit = enrTable.\"OrgUnit\"\n" +
                        "       LEFT JOIN enrollment_tracker enrTracker\n" +
                        "         ON enrTable.program = enrTracker.program AND enrTracker.instance_id = insTracker.instance_id\n" +
                        "              AND enrTracker.program_unique_id = enrTable.program_unique_id :: text\n" +
                        "WHERE enrTable.date_created :: TIMESTAMP > COALESCE((SELECT last_synced_date FROM marker WHERE category = 'enrollment'\n" +
                        "                                                                                           AND program_name = '%s'),\n" +
                        "                                                    '-infinity')\n" +
                        "  AND enrTracker.instance_id IS NULL\n" +
                        "  AND enrTable.status = 'ACTIVE';\n",
                enrollmentLookupTable, eventLookupTable, programName);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getNewActiveEnrollmentWithEventsReader(enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForUpdatedActiveEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        EnrollmentAPIPayLoad enr1 = new EnrollmentAPIPayLoad();
        enr1.setEnrollmentId("NAH0000000009");
        EnrollmentAPIPayLoad enr2 = new EnrollmentAPIPayLoad();
        enr2.setEnrollmentId("NAH0000000004");
        EnrollmentAPIPayLoad enr3 = new EnrollmentAPIPayLoad();
        enr3.setEnrollmentId("NAH0000000004");
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr1);
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr2);
        EnrollmentUtil.enrollmentsToSaveInTracker.add(enr3);

        String sql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM %s enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = '%s') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*,\n" +
                "                   enrollments.program_unique_id AS event_program_unique_id,\n" +
                "                   enrollments.status            AS event_program_status,\n" +
                "                   enrollments.incident_date     AS event_program_incident_date\n" +
                "                   FROM %s evnTable\n" +
                "                     INNER JOIN %s enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = '%s') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = COALESCE(enrollmentsTable.program_unique_id, eventsTable.event_program_unique_id) :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'ACTIVE' OR eventsTable.event_program_status = 'ACTIVE')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL %s;\n";

        String formattedSql = "SELECT\n" +
                "  enrollmentsTable.incident_date,\n" +
                "  enrollmentsTable.date_created         AS enrollment_date_created,\n" +
                "  enrollmentsTable.program_unique_id    AS program_unique_id,\n" +
                "  enrollmentsTable.status               AS enrollment_status,\n" +
                "  enrollmentsTable.program              AS enrolled_program,\n" +
                "  enrollmentsTable.enrollment_date      AS enr_date,\n" +
                "  enrollmentsTable.\"Patient_Identifier\" AS enrolled_patient_identifier,\n" +
                "  eventsTable.*,\n" +
                "  orgTracker.id                         AS orgunit_id,\n" +
                "  insTracker.instance_id,\n" +
                "  enrolTracker.enrollment_id,\n" +
                "  evntTracker.event_id\n" +
                "FROM (SELECT enrTable.*\n" +
                "      FROM enrollment enrTable\n" +
                "        INNER JOIN marker enrollment_marker\n" +
                "          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')\n" +
                "             AND category = 'enrollment' AND program_name = 'HTS Service') AS enrollmentsTable\n" +
                "  FULL OUTER JOIN (SELECT evnTable.*,\n" +
                "                   enrollments.program_unique_id AS event_program_unique_id,\n" +
                "                   enrollments.status            AS event_program_status,\n" +
                "                   enrollments.incident_date     AS event_program_incident_date\n" +
                "                   FROM event evnTable\n" +
                "                     INNER JOIN enrollment enrollments ON evnTable.\"Patient_Identifier\" = enrollments.\"Patient_Identifier\"\n" +
                "                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)\n" +
                "                     INNER JOIN marker event_marker\n" +
                "                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')\n" +
                "                          AND category = 'event' AND program_name = 'HTS Service') AS eventsTable\n" +
                "    ON enrollmentsTable.\"Patient_Identifier\" = eventsTable.\"Patient_Identifier\"\n" +
                "       AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)\n" +
                "  INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable.\"OrgUnit\", enrollmentsTable.\"OrgUnit\") = orgTracker.orgunit\n" +
                "  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable.\"Patient_Identifier\", enrollmentsTable.\"Patient_Identifier\") = insTracker.patient_id\n" +
                "  LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program\n" +
                "                                               AND enrolTracker.instance_id = insTracker.instance_id\n" +
                "                                               AND enrolTracker.program_unique_id = COALESCE(enrollmentsTable.program_unique_id, eventsTable.event_program_unique_id) :: TEXT\n" +
                "  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id\n" +
                "                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id\n" +
                "                                         AND eventsTable.program = evntTracker.program\n" +
                "                                         AND eventsTable.program_stage = evntTracker.program_stage\n" +
                "WHERE (enrollmentsTable.status = 'ACTIVE' OR eventsTable.event_program_status = 'ACTIVE')\n" +
                "                                            AND enrolTracker.instance_id IS NOT NULL AND enrolTracker.enrollment_id NOT IN ('NAH0000000009','NAH0000000004','NAH0000000004');\n";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(formattedSql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(formattedSql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

}
