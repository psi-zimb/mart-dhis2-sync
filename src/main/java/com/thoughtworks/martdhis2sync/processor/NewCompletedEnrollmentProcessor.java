package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.Enrollment;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.LinkedList;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;

@Component
public class NewCompletedEnrollmentProcessor implements ItemProcessor {

    @Override
    public EnrollmentAPIPayLoad process(Object tableRow) {
        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();

        return new EnrollmentAPIPayLoad(
                "",
                tableRowJsonObject.get("instance_id").getAsString(),
                tableRowJsonObject.get("program").getAsString(),
                tableRowJsonObject.get("orgunit_id").getAsString(),
                getFormattedDateString(tableRowJsonObject.get("enrollment_date").getAsString(),
                        DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                getFormattedDateString(tableRowJsonObject.get("incident_date").getAsString(),
                        DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                Enrollment.STATUS_COMPLETED,
                tableRowJsonObject.get("program_unique_id").getAsString(),
                new LinkedList<>()
        );
    }
}
