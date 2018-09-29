package com.thoughtworks.martdhis2sync.dao;

import com.thoughtworks.martdhis2sync.util.BatchUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Date;

import static com.thoughtworks.martdhis2sync.CommonTestHelper.setValuesForMemberFields;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MapSqlParameterSource.class, BatchUtil.class, LoggerDAO.class})
@PowerMockIgnore("javax.management.*")
public class LoggerDAOTest {

    @Mock
    private NamedParameterJdbcTemplate parameterJdbcTemplate;

    @Mock
    private MapSqlParameterSource mapSqlParameterSource;

    @Mock
    private Logger logger;

    @Mock
    private Date date;

    private LoggerDAO loggerDAO;

    private String service = "HT Service";
    private String user = "Superman";
    private String comments = "comments";
    String sql = "INSERT INTO log (program, synced_by, comments, status, failure_reason, date_created) " +
            "VALUES (:service, :user, :comments, 'pending', '', :dateCreated)";
    String dateStr = "2018-01-02 10:00:00";

    @Before
    public void setUp() throws Exception {
        loggerDAO = new LoggerDAO();
        setValuesForMemberFields(loggerDAO, "parameterJdbcTemplate", parameterJdbcTemplate);
        setValuesForMemberFields(loggerDAO, "logger", logger);
        mockStatic(BatchUtil.class);
        whenNew(MapSqlParameterSource.class).withNoArguments().thenReturn(mapSqlParameterSource);
        when(BatchUtil.GetUTCDateTimeAsString()).thenReturn(dateStr);
        when(BatchUtil.getDateFromString(dateStr, DATEFORMAT_WITH_24HR_TIME)).thenReturn(date);
    }

    @Test
    public void shouldLogSuccessMessageOnLogUpdate() throws Exception {
        when(parameterJdbcTemplate.update(sql, mapSqlParameterSource)).thenReturn(1);

        loggerDAO.addLog(service, user, comments);

        verifyStatic();
        BatchUtil.GetUTCDateTimeAsString();
        verifyStatic();
        BatchUtil.getDateFromString(dateStr, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verify(parameterJdbcTemplate, times(1)).update(sql, mapSqlParameterSource);
        verify(logger, times(1)).info("LoggerDAO: Successfully inserted into log table");
    }

    @Test
    public void shouldLogErrorMessageOnLogUpdateFail() throws Exception {
        when(parameterJdbcTemplate.update(sql, mapSqlParameterSource)).thenReturn(0);

        loggerDAO.addLog(service, user, comments);

        verifyStatic();
        BatchUtil.GetUTCDateTimeAsString();
        verifyStatic();
        BatchUtil.getDateFromString(dateStr, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verify(parameterJdbcTemplate, times(1)).update(sql, mapSqlParameterSource);
        verify(logger, times(1)).error("LoggerDAO: Failed to insert into log table");
    }
}