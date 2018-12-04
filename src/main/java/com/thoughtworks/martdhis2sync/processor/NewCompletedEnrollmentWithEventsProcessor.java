package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedList;
import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.updateLatestEnrollmentDateCreated;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getDataValues;
import static com.thoughtworks.martdhis2sync.util.EventUtil.updateLatestEventDateCreated;

@Component
public class NewCompletedEnrollmentWithEventsProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Override
    public ProcessedTableRow process(Object tableRow) {
        Gson gson = new GsonBuilder().setDateFormat(DATEFORMAT_WITH_24HR_TIME).create();
        JsonElement tableRowJsonElement = gson.toJsonTree(tableRow);
        JsonElement mappingObjJsonElement = gson.toJsonTree(mappingObj);

        JsonObject tableRowJsonObject = tableRowJsonElement.getAsJsonObject();
        JsonObject mappingJsonObject = mappingObjJsonElement.getAsJsonObject();

        updateLatestEventDateCreated(tableRowJsonObject.get("date_created").getAsString());
        updateLatestEnrollmentDateCreated(tableRowJsonObject.get("enrollment_date_created").getAsString());

        Event event = getEvent(tableRowJsonObject, mappingJsonObject);
        List<Event> events = new LinkedList<>();
        if (event != null) {
            events.add(event);
        }
        EnrollmentAPIPayLoad enrollmentAPIPayLoad = getEnrollmentAPIPayLoad(tableRowJsonObject, events);

        return new ProcessedTableRow(
                tableRowJsonObject.get("enrolled_patient_identifier").getAsString(),
                enrollmentAPIPayLoad
        );
    }

    private EnrollmentAPIPayLoad getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, List<Event> events) {
        return new EnrollmentAPIPayLoad(
               "",
               tableRowJsonObject.get("instance_id").getAsString(),
               tableRowJsonObject.get("enrolled_program").getAsString(),
               tableRowJsonObject.get("orgunit_id").getAsString(),
               getFormattedDateString(tableRowJsonObject.get("enr_date").getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               getFormattedDateString(tableRowJsonObject.get("incident_date").getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               tableRowJsonObject.get("enrollment_status").getAsString(),
               tableRowJsonObject.get("program_unique_id").getAsString(),
               events
        );
    }

    private Event getEvent(JsonObject tableRow, JsonObject mapping) {
        if (StringUtils.isEmpty(tableRow.get("event_unique_id").getAsString())) {
            return null;
        }

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
                tableRow.get("event_unique_id").getAsString(),
                getDataValues(tableRow, mapping)
        );
    }
}
