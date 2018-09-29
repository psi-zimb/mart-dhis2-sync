package com.thoughtworks.martdhis2sync.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.GetUTCDateTimeAsString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;

@Component
public class LoggerDAO {
    @Autowired
    @Qualifier("namedJdbcTemplate")
    private NamedParameterJdbcTemplate parameterJdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "LoggerDAO: ";

    public void addLog(String service, String user, String comments) {
        String sql = "INSERT INTO log (program, synced_by, comments, status, failure_reason, date_created) " +
                "VALUES (:service, :user, :comments, 'pending', '', :dateCreated)";
        Date date = getDateFromString(GetUTCDateTimeAsString(), DATEFORMAT_WITH_24HR_TIME);

        MapSqlParameterSource parameterSource = new MapSqlParameterSource();
        parameterSource.addValue("service", service);
        parameterSource.addValue("user", user);
        parameterSource.addValue("comments", comments);
        parameterSource.addValue("dateCreated", date);

        int update = parameterJdbcTemplate.update(sql, parameterSource);

        if (update == 1) {
            logger.info(LOG_PREFIX + "Successfully inserted into log table");
        } else {
            logger.error(LOG_PREFIX + "Failed to insert into log table");
        }
    }
}