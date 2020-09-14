package com.thoughtworks.martdhis2sync.dao;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class PatientDAO {
    @Value("classpath:sql/DeltaEnrollmentInstances.sql")
    private Resource deltaEnrollmentInstances;

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public List<Map<String, Object>> getDeltaEnrollmentInstanceIds(String enrollmentTable, String eventTable, String programName) throws Exception {
        String sql;
        try {
            sql = BatchUtil.convertResourceOutputToString(deltaEnrollmentInstances);
        } catch (IOException e) {
            throw new Exception("Error in converting sql to string:: " + e.getMessage());
        }
        logger.info("getDeltaEnrollmentInstanceIds: SQL :: " + sql);
        return jdbcTemplate.queryForList(String.format(sql, enrollmentTable, programName, enrollmentTable, programName,enrollmentTable, programName,enrollmentTable, programName, enrollmentTable, programName, enrollmentTable, programName, eventTable, enrollmentTable, programName));
    }
}