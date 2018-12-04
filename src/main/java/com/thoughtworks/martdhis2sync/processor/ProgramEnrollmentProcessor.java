package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.updateLatestEnrollmentDateCreated;

@Component
public class ProgramEnrollmentProcessor implements ItemProcessor {

    @Override
    public Enrollment process(Object tableRow) {

        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();

        String dateCreated = getUnquotedString(tableRowJsonObject.get("date_created").toString());
        updateLatestEnrollmentDateCreated(dateCreated);

        return new Enrollment(
                getUnquotedString(tableRowJsonObject.get("enrollment_id").toString()),
                getUnquotedString(tableRowJsonObject.get("instance_id").toString()),
                getUnquotedString(tableRowJsonObject.get("program").toString()),
                getUnquotedString(tableRowJsonObject.get("orgunit_id").toString()),
                getFormattedDateString(getUnquotedString(tableRowJsonObject.get("enrollment_date").toString()),
                        DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                getFormattedDateString(getUnquotedString(tableRowJsonObject.get("incident_date").toString()),
                        DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                getUnquotedString(tableRowJsonObject.get("status").toString().toUpperCase()),
                tableRowJsonObject.get("program_unique_id").toString());
    }
}
