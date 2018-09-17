package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;

@Component
public class ProgramEnrollmentProcessor implements ItemProcessor {
    private static final String EMPTY_STRING = "\"\"";

    private static final String ENROLLMENT_API_FORMAT = "{\"enrollment\": " + EMPTY_STRING + ", " +
            "\"trackedEntityInstance\": %s, " +
            "\"orgUnit\":%s," +
            "\"program\":%s," +
            "\"enrollmentDate\":%s," +
            "\"incidentDate\":%s," +
            "\"status\": %s}";

    private static final String ORGUNIT_UID = "orgunit_id";

    @Override
    public String process(Object tableRow) {

        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITHOUT_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();

        EnrollmentUtil.addEnrollment(tableRowJsonObject);

        return String.format(
                ENROLLMENT_API_FORMAT,
                tableRowJsonObject.get("instance_id").toString(),
                tableRowJsonObject.get(ORGUNIT_UID).toString(),
                tableRowJsonObject.get("program").toString(),
                tableRowJsonObject.get("enrollment_date").toString(),
                tableRowJsonObject.get("incident_date").toString(),
                tableRowJsonObject.get("status").toString().toUpperCase()
        );
    }
}
