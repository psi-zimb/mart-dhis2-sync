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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchUtil.class})
@PowerMockIgnore("javax.management.*")
public class LoggerDAOTest {

    @Mock
    private NamedParameterJdbcTemplate parameterJdbcTemplate;

    @Mock
    private Logger logger;

    @Mock
    private Date date;

    private LoggerDAO loggerDAO;

    private String service = "HT Service";
    private String user = "Superman";
    private String comments = "comments";
    private String dateStr = "2018-01-02 10:00:00";
    private Date startDate;
    private Date endDate;

    @Before
    public void setUp() throws Exception {
        loggerDAO = new LoggerDAO();
        setValuesForMemberFields(loggerDAO, "parameterJdbcTemplate", parameterJdbcTemplate);
        setValuesForMemberFields(loggerDAO, "logger", logger);
        mockStatic(BatchUtil.class);
        when(BatchUtil.getStringFromDate(any(Date.class), anyString())).thenReturn(dateStr);
        when(BatchUtil.getDateFromString(dateStr, DATEFORMAT_WITH_24HR_TIME)).thenReturn(date);
        startDate=new Date();
        endDate = startDate;
    }

    @Test
    public void shouldLogSuccessMessageOnLogInsert() {
        when(parameterJdbcTemplate.update(anyString(), any(MapSqlParameterSource.class))).thenReturn(1);

        loggerDAO.addLog(service, user, comments, startDate, endDate);

        verifyStatic(times(3));
        BatchUtil.getStringFromDate(any(Date.class), anyString());
        verifyStatic(times(3));
        BatchUtil.getDateFromString(dateStr, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verify(parameterJdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
        verify(logger, times(1)).info("LoggerDAO: Successfully inserted into log table");
    }

    @Test
    public void shouldLogErrorMessageOnLogInsertFail() {
        when(parameterJdbcTemplate.update(anyString(), any(MapSqlParameterSource.class))).thenReturn(0);

        loggerDAO.addLog(service, user, comments, startDate, endDate);

        verifyStatic(times(3));
        BatchUtil.getStringFromDate(any(Date.class), anyString());
        verifyStatic(times(3));
        BatchUtil.getDateFromString(dateStr, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        verify(parameterJdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
        verify(logger, times(1)).error("LoggerDAO: Failed to insert into log table");
    }

    @Test
    public void shouldLogForSuccessMessageOnLogUpdate() {
        String status = "success";
        String statusInfo = "";

        when(parameterJdbcTemplate.update(anyString(), any(MapSqlParameterSource.class))).thenReturn(1);

        loggerDAO.updateLog(service, status, statusInfo);

        verify(parameterJdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
        verify(logger, times(1)).info("LoggerDAO: Successfully updated status of the HT Service sync");
    }

    @Test
    public void shouldLogForErrorMessageOnLogUpdateFail() {
        String status = "failed";
        String statusInfo = "conflict";

        when(parameterJdbcTemplate.update(anyString(), any(MapSqlParameterSource.class))).thenReturn(0);

        loggerDAO.updateLog(service, status, statusInfo);

        verify(parameterJdbcTemplate, times(1)).update(anyString(), any(MapSqlParameterSource.class));
        verify(logger, times(1)).error("LoggerDAO: Failed updated status of the HT Service sync");
    }
}
