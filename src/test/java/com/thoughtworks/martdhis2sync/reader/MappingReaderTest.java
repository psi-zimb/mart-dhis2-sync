package com.thoughtworks.martdhis2sync.reader;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
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

    @Before
    public void setUp() throws Exception {
        mappingReader = new MappingReader();
        setValuesForMemberFields(mappingReader, "dataSource", dataSource);
        setValuesForMemberFields(mappingReader, "instanceResource", resource);
        setValuesForMemberFields(mappingReader, "enrollmentResource", resource);
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldReturnInstanceReaderWithDetailsBasedOnInput() throws Exception {
        String lookupTable = "patient_identifier";
        String programName = "HTS Service";

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

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.get(lookupTable, programName);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldLogWhenResourceCanNotBeConvertedToString() throws Exception {
        String lookupTable = "patient_identifier";
        String programName = "HTS Service";

        setValuesForMemberFields(mappingReader, "logger", logger);
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        when(BatchUtil.convertResourceOutputToString(resource))
                .thenThrow(new IOException("Could not convert sql file to string"));

        try {
            mappingReader.get(lookupTable, programName);
        } catch (Exception e) {
            verify(logger, times(1))
                    .error("Error in converting sql to string : Could not convert sql file to string");
        }
    }

    @Test
    public void shouldReturnReaderForProgramEnrollment() throws Exception {
        String lookupTable = "programs";
        String programName = "HTS Service";

        String sql = String.format("SELECT lt.*,\n" +
                        "  CASE WHEN i.instance_id is NULL THEN '' ELSE i.instance_id END AS instance_id,\n" +
                        "  CASE WHEN o.id is NULL THEN '' ELSE o.id END AS orgunit_id,\n" +
                        "  CASE WHEN e.enrollment_id is NULL THEN '' ELSE e.enrollment_id END AS enrollment_id\n" +
                        "FROM %s lt\n" +
                        "LEFT JOIN instance_tracker i ON  lt.\"Patient_Identifier\" = i.patient_id\n" +
                        "LEFT JOIN orgunit_tracker o ON  lt.\"OrgUnit\" = o.orgUnit\n" +
                        "LEFT JOIN enrollment_tracker e ON  i.instance_id = e.instance_id\n" +
                        "\n" +
                        "WHERE i.instance_id IS NOT NULL;\n",
                lookupTable);

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setSql(sql);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getEnrollmentReader(lookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }
}
