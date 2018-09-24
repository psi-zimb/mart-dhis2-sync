package com.thoughtworks.martdhis2sync.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;

@Component
public class MarkerUtil {

    public static final String CATEGORY_INSTANCE = "instance";
    public static final String CATEGORY_ENROLLMENT = "enrollment";
    public static final String CATEGORY_EVENT = "event";

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    public void updateMarkerEntry(String programName, String category, String date) {
        String sql = String.format("UPDATE marker SET last_synced_date = '%s' WHERE program_name = '%s' AND category = '%s'",
                date, programName, category);
        jdbcTemplate.update(sql);
    }

    public Date getLastSyncedDate(String programName, String category) {
        String sql = String.format("SELECT last_synced_date FROM marker WHERE program_name='%s' AND category='%s'",
                programName, category);

        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        Object lastSyncedDate = list.get(0).get("last_synced_date");

        if(lastSyncedDate == null) {
            return new Date(Long.MIN_VALUE);
        }

        return BatchUtil.getDateFromString(lastSyncedDate.toString(), DATEFORMAT_WITH_24HR_TIME);
    }
}
