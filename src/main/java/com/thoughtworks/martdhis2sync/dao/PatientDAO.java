package com.thoughtworks.martdhis2sync.dao;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
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

    public List<Map<String, Object>> getDeltaEnrollmentInstanceIds(String enrollmentTable, String eventTable, String programName) throws Exception {
        String sql;
        try {
            sql = BatchUtil.convertResourceOutputToString(deltaEnrollmentInstances);
        } catch (IOException e) {
            throw new Exception("Error in converting sql to string:: " + e.getMessage());
        }

        return jdbcTemplate.queryForList(String.format(sql, enrollmentTable, programName, eventTable, enrollmentTable, programName));
    }
}
