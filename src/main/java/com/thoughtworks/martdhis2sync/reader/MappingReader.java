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
import java.util.Date;
import java.util.Map;

@Component
public class MappingReader {

    @Autowired
    private DataSource dataSource;

    @Value("classpath:sql/Reader.sql")
    private Resource resource;

    private Logger logger = LoggerFactory.getLogger(MappingReader.class);

    public JdbcCursorItemReader<Map<String, Object>> get(String lookupTable, Date date, String category, String programName) {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        try {
            String sql = BatchUtil.convertResourceOutputToString(resource);
            reader.setDataSource(dataSource);
            reader.setSql(String.format(sql, lookupTable, category, programName, date.toString()));
            reader.setRowMapper(new ColumnMapRowMapper());
        } catch (IOException e) {
            logger.error("Error in converting sql to string : " + e.getMessage());
        }
        return reader;
    }
}
