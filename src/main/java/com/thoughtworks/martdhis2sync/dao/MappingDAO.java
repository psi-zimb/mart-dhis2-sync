package com.thoughtworks.martdhis2sync.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MappingDAO {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;


    public Map<String, Object> getMapping(String mapping) {
        String sql = String.format("SELECT lookup_table, mapping_json FROM mapping WHERE mapping_name='%s'", mapping);

        return jdbcTemplate.queryForMap(sql);
    }

}
