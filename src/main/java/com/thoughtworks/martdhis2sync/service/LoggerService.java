package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.dao.LoggerDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LoggerService {

    @Autowired
    private LoggerDAO loggerDAO;

    private static StringBuilder statusInfo = new StringBuilder();

    private static final int COMMA_AND_SPACE_SIZE = 2;

    public void addLog(String service, String user, String comments) {
        loggerDAO.addLog(service, user, comments);
    }

    public void updateLog(String service, String status) {
        loggerDAO.updateLog(service, status, removeChars(statusInfo, COMMA_AND_SPACE_SIZE));
    }

    public void collateLogInfo(String info) {
        statusInfo.append(info).append(", ");
    }

    public static String removeChars(StringBuilder value, int noOfChars) {
        int length = value.length();

        return length >= noOfChars ? value.delete(length - noOfChars, length).toString() : value.toString();
    }
}
