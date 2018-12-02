package com.thoughtworks.martdhis2sync.reader;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;

@Component
public class MappingReader {

    @Autowired
    private DataSource dataSource;

    @Value("classpath:sql/InstanceReader.sql")
    private Resource instanceResource;

    @Value("classpath:sql/EnrollmentReader.sql")
    private Resource enrollmentResource;

    @Value("classpath:sql/EventReader.sql")
    private Resource eventResource;

    @Value("classpath:sql/NewCompletedEnrollmentWithEvents.sql")
    private Resource newCompletedEnrWithEventsResource;

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

    public JdbcCursorItemReader<Map<String, Object>> getEventReader(String lookupTable, String programName, String enrollmentLookupTable) {
        String sql = String.format(getSql(eventResource), lookupTable, enrollmentLookupTable, programName);
        return get(sql);
    }

    public JdbcCursorItemReader<Map<String, Object>> getNewCompletedEnrollmentWithEventsReader(String eventLookupTable, String programName, String enrollmentLookupTable) {
        String sql = String.format(getSql(newCompletedEnrWithEventsResource), enrollmentLookupTable, eventLookupTable, programName);
        return get(sql);
    }
}
