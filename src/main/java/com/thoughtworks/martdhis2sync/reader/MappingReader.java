package com.thoughtworks.martdhis2sync.reader;

import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class MappingReader {

    @Autowired
    private DataSource dataSource;

    @Value("classpath:sql/InstanceReader.sql")
    private Resource instanceResource;

    @Value("classpath:sql/InstanceReaderWithDateRange.sql")
    private Resource instanceResourceWithDateRange;

    @Value("classpath:sql/EnrollmentReader.sql")
    private Resource enrollmentResource;

    @Value("classpath:sql/EventReader.sql")
    private Resource eventResource;

    @Value("classpath:sql/NewCompletedEnrollmentWithEvents.sql")
    private Resource newCompletedEnrWithEventsResource;

    @Value("classpath:sql/NewCompletedEnrollmentWithEventsWithDateRange.sql")
    private Resource newCompletedEnrWithEventsResourceWithDateRange;

    @Value("classpath:sql/NewCancelledEnrollmentWithEvents.sql")
    private Resource newCancelledEnrWithEventsResource;

    @Value("classpath:sql/NewCancelledEnrollmentWithEventsWithDateRange.sql")
    private Resource newCancelledEnrWithEventsResourceWithDateRange;

    @Value("classpath:sql/UpdatedCancelledEnrollmentWithEvents.sql")
    private Resource updatedCancelledEnrWithEventsResource;

    @Value("classpath:sql/UpdatedCancelledEnrollmentWithEventsWithDateRange.sql")
    private Resource updatedCancelledEnrWithEventsResourceWithDateRange;

    @Value("classpath:sql/UpdatedCompletedEnrollmentWithEvents.sql")
    private Resource updatedCompletedEnrWithEventsResource;

    @Value("classpath:sql/UpdatedCompletedEnrollmentWithEventsWithDateRange.sql")
    private Resource updatedCompletedEnrWithEventsResourceWithDateRange;

    @Value("classpath:sql/NewActiveEnrollmentWithEvents.sql")
    private Resource newActiveEnrWithEventsResource;

    @Value("classpath:sql/NewActiveEnrollmentWithEventsWithDateRange.sql")
    private Resource newActiveEnrWithEventsResourceWithDateRange;

    @Value("classpath:sql/UpdatedActiveEnrollmentWithEvents.sql")
    private Resource updatedActiveEnrWithEventsResource;

    @Value("classpath:sql/UpdatedActiveEnrollmentWithEventsWithDateRange.sql")
    private Resource updatedActiveEnrWithEventsResourceWithDateRange;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private JdbcCursorItemReader<Map<String, Object>> get(String sql) {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(sql);
        reader.setRowMapper(new ColumnMapRowMapper());
        return reader;
    }

    private String getSql(Resource resource) {
        String sql = "";
        try {
            sql = BatchUtil.convertResourceOutputToString(resource);
        } catch (IOException e) {
            logger.error("Error in converting sql to string : " + e.getMessage());
        }
        return sql;
    }

    public JdbcCursorItemReader<Map<String, Object>> getEnrollmentReader(String lookupTable, String programName) {
        String sql = String.format(getSql(enrollmentResource), lookupTable, programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getInstanceReader(String lookupTable, String programName) {
        String sql = String.format(getSql(instanceResource), lookupTable, programName);
       return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getInstanceReaderWithDateRange(String lookupTable, String programName, String startDate, String endDate) {
        String sql = String.format(getSql(instanceResourceWithDateRange), lookupTable, programName, endDate);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getEventReader(String lookupTable, String programName, String enrollmentLookupTable) {
        String sql = String.format(getSql(eventResource), lookupTable, enrollmentLookupTable, programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewCompletedEnrollmentWithEventsReader(
            String instanceLookupTable,String enrollmentLookupTable, String programName, String eventLookupTable) {
        String sql = String.format(getSql(newCompletedEnrWithEventsResource), enrollmentLookupTable,
                eventLookupTable,instanceLookupTable, programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewCompletedEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable, String startDate, String endDate) {
        String sql = String.format(getSql(newCompletedEnrWithEventsResourceWithDateRange), enrollmentLookupTable,
                eventLookupTable, startDate, endDate);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewCancelledEnrollmentWithEventsReader(
            String instanceLookupTable,String enrollmentLookupTable, String programName, String eventLookupTable) {
        String sql = String.format(getSql(newCancelledEnrWithEventsResource), enrollmentLookupTable,
                eventLookupTable,instanceLookupTable, programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewCancelledEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable, String startDate, String endDate) {
        String sql = String.format(getSql(newCancelledEnrWithEventsResourceWithDateRange), enrollmentLookupTable,
                eventLookupTable, startDate, endDate);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getUpdatedCompletedEnrollmentWithEventsReader(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedCompletedEnrWithEventsResource), enrollmentLookupTable, programName,
                eventLookupTable, enrollmentLookupTable, programName, andClause);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getUpdatedCompletedEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String startDate, String endDate) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedCompletedEnrWithEventsResourceWithDateRange), enrollmentLookupTable, startDate,
                eventLookupTable, enrollmentLookupTable, startDate, endDate,  andClause);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getUpdatedCancelledEnrollmentWithEventsReader(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedCancelledEnrWithEventsResource), enrollmentLookupTable, programName,
                eventLookupTable, enrollmentLookupTable, programName, andClause);
        return get(sql);
    }
    public JdbcCursorItemReader<Map<String, Object>> getUpdatedCancelledEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String startDate, String endDate) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedCancelledEnrWithEventsResourceWithDateRange), enrollmentLookupTable, startDate,
                eventLookupTable, enrollmentLookupTable, startDate, endDate, andClause);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewActiveEnrollmentWithEventsReader(
            String instanceLookupTable,String enrollmentLookupTable, String programName, String eventLookupTable) {

        String sql = String.format(getSql(newActiveEnrWithEventsResource), enrollmentLookupTable,
                eventLookupTable, instanceLookupTable,programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewActiveEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable, String startDate, String endDate) {

        String sql = String.format(getSql(newActiveEnrWithEventsResourceWithDateRange), enrollmentLookupTable,
                eventLookupTable, startDate, endDate);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getUpdatedActiveEnrollmentWithEventsReader(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedActiveEnrWithEventsResource), enrollmentLookupTable, programName,
                eventLookupTable, enrollmentLookupTable, programName, andClause);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getUpdatedActiveEnrollmentWithEventsReaderWithDateRange(
            String enrollmentLookupTable, String programName, String eventLookupTable,
            List<EnrollmentAPIPayLoad> enrollmentsToIgnore, String startDate, String endDate) {

        String syncedCompletedEnrollmentIds = getEnrollmentIds(enrollmentsToIgnore);
        String andClause = StringUtils.isEmpty(syncedCompletedEnrollmentIds) ? ""
                : String.format("AND enrolTracker.enrollment_id NOT IN (%s)", syncedCompletedEnrollmentIds);
        String sql = String.format(getSql(updatedActiveEnrWithEventsResourceWithDateRange), enrollmentLookupTable, startDate,
                eventLookupTable, enrollmentLookupTable, startDate, endDate, andClause);
        return get(sql);
    }

    private String getEnrollmentIds(List<EnrollmentAPIPayLoad> enrollmentsToIgnore) {
        List<String> enrollmentIds = new ArrayList<>();
        enrollmentsToIgnore.forEach(enrollment ->
                enrollmentIds.add("'" + enrollment.getEnrollmentId() + "'")
        );

        return String.join(",", enrollmentIds);
    }
}
