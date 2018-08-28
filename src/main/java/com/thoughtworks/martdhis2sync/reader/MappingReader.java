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

        StringBuilder query = new StringBuilder();
        query.append("SELECT lt.*, CASE WHEN i.instance_id is NULL THEN '' else i.instance_id END as instance_id ");
        query.append(String.format(" FROM %s lt ", lookupTable));
        query.append("LEFT join instance_tracker i ");
        query.append("ON  lt.\"Patient_Identifier\" = i.patient_id ;");

        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql(query.toString());
        reader.setRowMapper(new ColumnMapRowMapper());

        return reader;
    }
}
