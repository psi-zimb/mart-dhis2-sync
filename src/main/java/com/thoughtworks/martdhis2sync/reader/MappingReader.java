package com.thoughtworks.martdhis2sync.reader;

import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;

@Component
public class MappingReader {

    @Autowired
    private DataSource dataSource;

    public JdbcCursorItemReader<Map<String, Object>> get(String lookupTable) {

        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(String.format("SELECT * FROM %s", lookupTable));
        reader.setRowMapper(new ColumnMapRowMapper());

        return reader;
    }
}
