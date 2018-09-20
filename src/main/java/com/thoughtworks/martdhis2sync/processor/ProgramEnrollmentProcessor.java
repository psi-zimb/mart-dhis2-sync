package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Date;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;

@Component
public class ProgramEnrollmentProcessor implements ItemProcessor {
    private static final String ENROLLMENT_API_FORMAT = "{\"enrollment\": %s, " +
            "\"trackedEntityInstance\": %s, " +
            "\"orgUnit\":%s," +
            "\"program\":%s," +
            "\"enrollmentDate\":\"%s\"," +
            "\"incidentDate\":\"%s\"," +
            "\"status\": %s}";

    @Override
    public String process(Object tableRow) {

        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();

        EnrollmentUtil.addEnrollment(tableRowJsonObject);

        String dateCreated = getUnquotedString(tableRowJsonObject.get("date_created").toString());
        String enrollmentDate = getUnquotedString(tableRowJsonObject.get("enrollment_date").toString());
        String incidentDate = getUnquotedString(tableRowJsonObject.get("incident_date").toString());
        String enrollmentDateWithoutTime = getFormattedDateString(enrollmentDate,
                                                                DATEFORMAT_WITH_24HR_TIME,
                                                                DATEFORMAT_WITHOUT_TIME);
        String incidentDateWithoutTime = getFormattedDateString(incidentDate,
                                                                DATEFORMAT_WITH_24HR_TIME,
                                                                DATEFORMAT_WITHOUT_TIME);

        updateLatestDateCreated(dateCreated);

        return String.format(
                ENROLLMENT_API_FORMAT,
                tableRowJsonObject.get("enrollment_id").toString(),
                tableRowJsonObject.get("instance_id").toString(),
                tableRowJsonObject.get("orgunit_id").toString(),
                tableRowJsonObject.get("program").toString(),
                enrollmentDateWithoutTime,
                incidentDateWithoutTime,
                tableRowJsonObject.get("status").toString().toUpperCase()
        );
    }

    private void updateLatestDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (EnrollmentUtil.date.compareTo(bahmniDateCreated) < 1) {
            EnrollmentUtil.date = bahmniDateCreated;
        }
    }
}
