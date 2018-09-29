package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.LoggerDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LoggerService {

    @Autowired
    private LoggerDAO loggerDAO;

    public void addLog(String service, String user, String comments) {
        loggerDAO.addLog(service, user, comments);
    }

    public void updateLog(String service, String status, String failedReason) {
        loggerDAO.updateLog(service, status, failedReason);
    }
}