package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.updateLatestEnrollmentDateCreated;
import static com.thoughtworks.martdhis2sync.util.EventUtil.updateLatestEventDateCreated;

@Component
public abstract class EnrollmentWithEventProcessor {

    public ProcessedTableRow process(Object tableRow, Object mappingObj) throws Exception {
        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        JsonElement eventDateCreated = tableRowJsonObject.get("date_created");
        JsonElement enrollmentDateCreated = tableRowJsonObject.get("enrollment_date_created");
        JsonElement enrollmentType = tableRowJsonObject.get("enrollment_type");
        updateLatestEventDateCreated(hasValue(eventDateCreated) ? eventDateCreated.getAsString() : "");
        updateLatestEnrollmentDateCreated((hasValue(enrollmentDateCreated) ? enrollmentDateCreated.getAsString() : ""), (enrollmentType != null ? enrollmentType.getAsString() : ""));

        Event event = getEvent(tableRowJsonObject, mappingJsonObject);
        List<Event> events = new LinkedList<>();
        if (event != null) {
            events.add(event);
        }
        EnrollmentAPIPayLoad enrollmentAPIPayLoad = getEnrollmentAPIPayLoad(tableRowJsonObject, events).orElse(null);

        JsonElement programUniqueId = tableRowJsonObject.get("program_unique_id");
        JsonElement eventProgramUniqueId = tableRowJsonObject.get("event_program_unique_id");
        return new ProcessedTableRow(
                hasValue(programUniqueId) ? programUniqueId.getAsString()
                        : eventProgramUniqueId.getAsString(),
                enrollmentAPIPayLoad
        );
    }

    abstract Event getEvent(JsonObject tableRow, JsonObject mapping);
    abstract Optional<EnrollmentAPIPayLoad> getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, List<Event> events) throws Exception;
}
