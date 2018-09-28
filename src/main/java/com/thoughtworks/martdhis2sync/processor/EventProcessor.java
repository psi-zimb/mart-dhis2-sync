package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Set;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getDateFromString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;
import static com.thoughtworks.martdhis2sync.util.EventUtil.addExistingEventTracker;
import static com.thoughtworks.martdhis2sync.util.EventUtil.addNewEventTracker;

@Component
public class EventProcessor implements ItemProcessor {

    private static final String EVENT_API_FORMAT = "{\"event\": \"\", " +
            "\"trackedEntityInstance\": %s, " +
            "\"enrollment\": %s, " +
            "\"program\": %s, " +
            "\"programStage\": %s, " +
            "\"orgUnit\": %s, " +
            "\"eventDate\": \"%s\", " +
            "\"status\": %s, " +
            "\"dataValues\":[%s]}";
    @Setter
    private Object mappingObj;

    @Override
    public Object process(Object tableRow) {
        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        if (hasValue(tableRowJsonObject.get("event_id"))) {
            addExistingEventTracker(tableRowJsonObject);
        } else {
            addNewEventTracker(tableRowJsonObject);
        }

        updateLatestDateCreated(getUnquotedString(tableRowJsonObject.get("date_created").toString()));
        return createRequestBodyElements(tableRowJsonObject, mappingJsonObject);
    }

    private void updateLatestDateCreated(String dateCreated) {
        Date bahmniDateCreated = getDateFromString(dateCreated, DATEFORMAT_WITH_24HR_TIME);
        if (EventUtil.date.compareTo(bahmniDateCreated) < 1) {
            EventUtil.date = bahmniDateCreated;
        }
    }

    private String createRequestBodyElements(JsonObject tableRow, JsonObject mapping) {

        String eventDate = getUnquotedString(tableRow.get("event_date").toString());
        String dateString = getFormattedDateString(eventDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        return String.format(EVENT_API_FORMAT,
                tableRow.get("instance_id").toString(),
                tableRow.get("enrollment_id").toString(),
                tableRow.get("program").toString(),
                tableRow.get("program_stage").toString(),
                tableRow.get("orgunit_id").toString(),
                dateString,
                tableRow.get("status").toString(),
                getDataValues(tableRow, mapping)
        );
    }

    private String getDataValues(JsonObject tableRow, JsonObject mapping) {
        Set<String> keys = tableRow.keySet();
        StringBuilder dataValues = new StringBuilder();

        for (String key : keys) {
            JsonElement dataElement = mapping.get(key);
            if (hasValue(dataElement)) {
                String value = tableRow.get(key).toString();
                dataValues.append(String.format("{\"dataElement\": %s, \"value\": %s},", dataElement.toString(), value));
            }
        }

        return removeLastChar(dataValues);
    }
}
