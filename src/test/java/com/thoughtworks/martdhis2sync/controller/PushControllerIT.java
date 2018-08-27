package com.thoughtworks.martdhis2sync.controller;

import com.thoughtworks.martdhis2sync.MartDhis2SyncApplication;
import com.thoughtworks.martdhis2sync.SystemPropertyActiveProfileResolver;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MartDhis2SyncApplication.class)
@ActiveProfiles(profiles = "test", resolver = SystemPropertyActiveProfileResolver.class)
public class PushControllerIT {

    @Autowired
    private PushController pushController;

    @Qualifier("jdbcTemplate")
    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Test
    @Sql(scripts = {"classpath:mapping/mapping.sql"})
    public void shouldReturnJobStatusAsExecuted() {
        String service = "HTS Service";
        Map<String, String> actual = pushController.pushData(service);

        assertEquals("Executed", actual.get("Job Status"));
    }

    @After
    public void tearDown() throws Exception {
        jdbcTemplate.execute("DROP TABLE IF EXISTS mapping CASCADE; " +
                "CREATE TABLE public.mapping(" +
                "mapping_name text," +
                "lookup_table json, " +
                "mapping_json json, " +
                "created_by text, " +
                "created_date date, " +
                "modifed_by text, " +
                "modifed_date date)");
    }
}