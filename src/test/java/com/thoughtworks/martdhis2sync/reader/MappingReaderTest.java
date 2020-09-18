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
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MappingReader.class, BatchUtil.class})
@PowerMockIgnore("javax.management.*")
public class MappingReaderTest {

    private static final String programName = "HTS Service";
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
        setValuesForMemberFields(mappingReader, "eventResource", resource);
        setValuesForMemberFields(mappingReader, "newCompletedEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "updatedCompletedEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "updatedCancelledEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "newActiveEnrWithEventsResource", resource);
        setValuesForMemberFields(mappingReader, "updatedActiveEnrWithEventsResource", resource);
        mockStatic(BatchUtil.class);
    }

    @Test
    public void shouldReturnInstanceReaderWithDetailsBasedOnInput() throws Exception {
        String lookupTable = "patient_identifier";

        String sql = "someQuery";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
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
    public void shouldReturnReaderForNewCompletedProgramEnrollment() throws Exception {
        String lookupTable = "programs";

        String sql = "someQuery";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
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

        String sql = "someQuery";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
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
        String instanceLookupTable = "instance";
        String enrollmentLookupTable = "enrollment";

        String sql = "someQuery";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getNewCompletedEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForUpdatedCompletedEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        String instanceLookupTable = "instance";
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

        String sql = "someQuery";

        String formattedSql = "someQuery";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable, EnrollmentUtil.enrollmentsToSaveInTracker);

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
        String instanceLookupTable = "instance";
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();

        String sql = "someQuery";

        String formattedSql = "someQuery";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable, EnrollmentUtil.enrollmentsToSaveInTracker);

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
        String instanceLookupTable = "instance";

        String sql = "someQuery";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getNewActiveEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(sql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
    }

    @Test
    public void shouldReturnReaderForUpdatedActiveEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        String instanceLookupTable = "instance";
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

        String sql = "someQuery";

        String formattedSql = "someQuery";
        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCompletedEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable, EnrollmentUtil.enrollmentsToSaveInTracker);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(formattedSql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

    @Test
    public void shouldReturnReaderForUpdatedCancelledEnrollmentsWithEvents() throws Exception {
        String eventLookupTable = "event";
        String enrollmentLookupTable = "enrollment";
        String instanceLookupTable = "instance";
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

        String sql = "queryString";
        String formattedSql = "queryString";

        whenNew(JdbcCursorItemReader.class).withNoArguments().thenReturn(jdbcCursorItemReader);
        whenNew(ColumnMapRowMapper.class).withNoArguments().thenReturn(columnMapRowMapper);
        when(BatchUtil.convertResourceOutputToString(resource)).thenReturn(sql);
        doNothing().when(jdbcCursorItemReader).setDataSource(dataSource);
        doNothing().when(jdbcCursorItemReader).setRowMapper(columnMapRowMapper);

        JdbcCursorItemReader<Map<String, Object>> actual = mappingReader.getUpdatedCancelledEnrollmentWithEventsReader(instanceLookupTable,enrollmentLookupTable, programName, eventLookupTable, EnrollmentUtil.enrollmentsToSaveInTracker);

        assertEquals(jdbcCursorItemReader, actual);

        verify(jdbcCursorItemReader, times(1)).setDataSource(dataSource);
        verify(jdbcCursorItemReader, times(1)).setSql(formattedSql);
        verify(jdbcCursorItemReader, times(1)).setRowMapper(columnMapRowMapper);
        EnrollmentUtil.enrollmentsToSaveInTracker.clear();
    }

}
