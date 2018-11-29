package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DHIS_ACCEPTABLE_DATEFORMAT;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.EventUtil.addNewEventTracker;

public class NewCompletedEnrollmentProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Override
    public ProcessedTableRow process(Object tableRow) {
        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        addNewEventTracker(tableRowJsonObject);

        updateLatestEventDateCreated(tableRowJsonObject.get("date_created").getAsString());
        updateLatestEnrollmentDateCreated(tableRowJsonObject.get("enrollment_date_created").getAsString());

        Event event = getEvent(tableRowJsonObject, mappingJsonObject);
        EnrollmentAPIPayLoad enrollmentAPIPayLoad = getEnrollmentAPIPayLoad(tableRowJsonObject, event);

        return new ProcessedTableRow(
                tableRowJsonObject.get("\"Patient_Identifier\"").getAsString(),
                enrollmentAPIPayLoad
        );
    }

    private EnrollmentAPIPayLoad getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, Event event) {
        return new EnrollmentAPIPayLoad(
               "",
               tableRowJsonObject.get("instance_id").getAsString(),
               tableRowJsonObject.get("program").getAsString(),
               tableRowJsonObject.get("orgunit_id").getAsString(),
               getFormattedDateString(tableRowJsonObject.get("enrollment_date").getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               getFormattedDateString(tableRowJsonObject.get("incident_date").getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               EnrollmentAPIPayLoad.STATUS_ACTIVE,
               tableRowJsonObject.get("program_unique_id").getAsString(),
                Collections.singletonList(event)
        );
    }

    private Event getEvent(JsonObject tableRow, JsonObject mapping) {
        String eventDate = tableRow.get("event_date").getAsString();
        String dateString = getFormattedDateString(eventDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        return new Event(
                "",
                tableRow.get("instance_id").getAsString(),
                "",
                tableRow.get("program").getAsString(),
                tableRow.get("program_stage").getAsString(),
                tableRow.get("orgunit_id").getAsString(),
                dateString,
                Event.STATUS_COMPLETED,
                getDataValues(tableRow, mapping)
        );
    }

    private Map<String, String> getDataValues(JsonObject tableRow, JsonObject mapping) {
        Set<String> keys = tableRow.keySet();
        Map<String, String> dataValues = new HashMap<>();

        for (String key : keys) {
            JsonElement dataElement = mapping.get(key);
            if (hasValue(dataElement)) {
                String value = tableRow.get(key).getAsString();
                String dataElementInStringFormat = dataElement.getAsString();
                dataValues.put(
                        dataElementInStringFormat,
                        changeFormatIfDate(dataElementInStringFormat, value)
                );
            }
        }

        return dataValues;
    }

    private String changeFormatIfDate(String elementId, String value) {
        return EventUtil.getElementsOfTypeDateTime().contains(elementId) ?
                getFormattedDateString(
                        value,
                        DATEFORMAT_WITH_24HR_TIME,
                        DHIS_ACCEPTABLE_DATEFORMAT
                )
                : value;
    }

    private void updateLatestEventDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (EventUtil.date.compareTo(bahmniDateCreated) < 1) {
            EventUtil.date = bahmniDateCreated;
        }
    }

    private void updateLatestEnrollmentDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (EnrollmentUtil.date.compareTo(bahmniDateCreated) < 1) {
            EnrollmentUtil.date = bahmniDateCreated;
        }
    }
}
