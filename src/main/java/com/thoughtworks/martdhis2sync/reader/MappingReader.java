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

    private Logger logger = LoggerFactory.getLogger(MappingReader.class);

    private JdbcCursorItemReader<Map<String, Object>> get(String lookupTable, String programName, Resource resource) {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        try {
            String sql = BatchUtil.convertResourceOutputToString(resource);
            reader.setDataSource(dataSource);
            reader.setSql(String.format(sql, lookupTable, programName));
            reader.setRowMapper(new ColumnMapRowMapper());
        } catch (IOException e) {
            logger.error("Error in converting sql to string : " + e.getMessage());
        }
        return reader;
    }

    public JdbcCursorItemReader<Map<String, Object>> getEnrollmentReader(String lookupTable, String programName) {
        return get(lookupTable, programName, enrollmentResource);
    }

    public JdbcCursorItemReader<Map<String, Object>> getInstanceReader(String lookupTable, String programName) {
        return get(lookupTable, programName, instanceResource);
    }

    public JdbcCursorItemReader<Map<String, Object>> getEventReader(String lookupTable, String programName) {
        return get(lookupTable, programName, eventResource);
    }
}
