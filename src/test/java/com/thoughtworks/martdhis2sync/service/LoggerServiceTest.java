package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.CommonTestHelper;
import com.thoughtworks.martdhis2sync.dao.LoggerDAO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.thoughtworks.martdhis2sync.service.LoggerService.CONTACT_ADMIN;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;

@RunWith(PowerMockRunner.class)
public class LoggerServiceTest {

    @Mock
    private LoggerDAO loggerDAO;

    private LoggerService loggerService;

    @Before
    public void setUp() throws Exception {
        loggerService = new LoggerService();
        CommonTestHelper.setValuesForMemberFields(loggerService, "loggerDAO", loggerDAO);
    }

    @Test
    public void shouldCallLoggerDAO() {
        String service = "HT Service";
        String user = "superman";
        String comments = "comments";
        doNothing().when(loggerDAO).addLog(service, user, comments);

        loggerService.addLog(service, user, comments);

        verify(loggerDAO, times(1)).addLog(service, user, comments);
    }

    @Test
    public void shouldCallLoggerDAOUpdate() {
        String service = "HT Service";
        String status = "failed";
        String statusInfo = "conflict";

        loggerService.collateLogMessage(statusInfo);
        loggerService.updateLog(service, status);

        verify(loggerDAO, times(1)).updateLog(service, status, statusInfo + ", " + CONTACT_ADMIN);
    }

    @Test
    public void shouldNotAddContactAdminToTheLogMessageWhenStatusIsSuccess() throws NoSuchFieldException, IllegalAccessException {
        String service = "HT Service";
        String status = "success";

        loggerService.updateLog(service, status);

        verify(loggerDAO, times(1)).updateLog(service, status, "");
    }
}
