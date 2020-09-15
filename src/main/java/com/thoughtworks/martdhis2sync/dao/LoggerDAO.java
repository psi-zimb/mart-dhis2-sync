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
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;

@Component
public class LoggerDAO {
    @Autowired
    @Qualifier("namedJdbcTemplate")
    private NamedParameterJdbcTemplate parameterJdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "LoggerDAO: ";

    public void addLog(String service, String user, String comments, Date startDate, Date endDate) {
        String sql = "INSERT INTO log (program, synced_by, comments, status, status_info, date_created, start_date, end_date) " +
                "VALUES (:service, :user, :comments, 'pending', '', :dateCreated, :startDate, :endDate);";
        String stringFromDate = getStringFromDate(new Date(), DATEFORMAT_WITH_24HR_TIME);
        Date dateFromString = getDateFromString(stringFromDate, DATEFORMAT_WITH_24HR_TIME);

        MapSqlParameterSource parameterSource = new MapSqlParameterSource();
        parameterSource.addValue("service", service);
        parameterSource.addValue("user", user);
        parameterSource.addValue("comments", comments);
        parameterSource.addValue("dateCreated", dateFromString);

        if (startDate != null && endDate != null) {
            String stringFromStartDate = getStringFromDate(startDate, DATEFORMAT_WITH_24HR_TIME);
            Date startDateFromString = getDateFromString(stringFromStartDate, DATEFORMAT_WITH_24HR_TIME);

            String stringFromEndDate = getStringFromDate(endDate, DATEFORMAT_WITH_24HR_TIME);
            Date endDateFromString = getDateFromString(stringFromEndDate, DATEFORMAT_WITH_24HR_TIME);

            parameterSource.addValue("startDate", startDateFromString);
            parameterSource.addValue("endDate", endDateFromString);
        } else {
            parameterSource.addValue("startDate", null);
            parameterSource.addValue("endDate", null);
        }

        int update = parameterJdbcTemplate.update(sql, parameterSource);

        if(update ==1)

        {
            logger.info(LOG_PREFIX + "Successfully inserted into log table");
        } else

        {
            logger.error(LOG_PREFIX + "Failed to insert into log table");
        }

    }

    public void updateLog(String service, String status, String statusInfo) {
        String sql = "UPDATE log SET status = :status, status_info = :statusInfo " +
                "WHERE program = :service AND status = 'pending';";

        MapSqlParameterSource parameterSource = new MapSqlParameterSource();
        parameterSource.addValue("status", status);
        parameterSource.addValue("statusInfo", statusInfo);
        parameterSource.addValue("service", service);

        int update = parameterJdbcTemplate.update(sql, parameterSource);

        if (update == 1) {
            logger.info(LOG_PREFIX + String.format("Successfully updated status of the %s sync", service));
        } else {
            logger.error(LOG_PREFIX + String.format("Failed updated status of the %s sync", service));
        }
    }
}
