package com.thoughtworks.martdhis2sync.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class MarkerUtil {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    public void updateMarkerEntry(String programName, String category) {
        String sql = String.format("UPDATE marker SET last_synced_date = '%s' WHERE program_name = '%s' AND category = '%s'",
                BatchUtil.getStringFromDate(TEIUtil.date), programName, category);
        jdbcTemplate.update(sql);
    }
}
