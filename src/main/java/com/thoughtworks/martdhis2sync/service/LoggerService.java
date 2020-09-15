package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.LoggerDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

@Component
public class LoggerService {

    @Autowired
    private LoggerDAO loggerDAO;

    public static final String CONTACT_ADMIN = "Please contact Admin team.";

    public static final String NO_DELTA_DATA = "No delta data to sync.";

    public static final String SUCCESS = "success";

    public static final String FAILED = "failed";

    private Set<String> logMessage = new LinkedHashSet<>();

    public void addLog(String service, String user, String comments, Date startDate, Date endDate) {
        logMessage.clear();
        loggerDAO.addLog(service, user, comments, startDate, endDate);
    }

    public void updateLog(String service, String status) {
        if (FAILED.equalsIgnoreCase(status)) {
            logMessage.add(CONTACT_ADMIN);
        }
        String message = logMessage.toString();
        loggerDAO.updateLog(service, status, message.substring(1, message.length() - 1));
    }

    public void collateLogMessage(String message) {
        logMessage.add(message);
    }

    public void clearLog() {
        logMessage.clear();
    }
}
