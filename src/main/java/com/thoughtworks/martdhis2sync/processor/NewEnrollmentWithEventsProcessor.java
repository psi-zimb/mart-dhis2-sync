package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITHOUT_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getFormattedDateString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.hasValue;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getDataValues;

@Component
public class NewEnrollmentWithEventsProcessor extends EnrollmentWithEventProcessor implements ItemProcessor{

    @Setter
    private Object mappingObj;

    @Override
    public ProcessedTableRow process(Object tableRow) {
        return super.process(tableRow, mappingObj);
    }

    EnrollmentAPIPayLoad getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, List<Event> events) {
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

    Event getEvent(JsonObject tableRow, JsonObject mapping) {
        if (!hasValue(tableRow.get("event_unique_id"))) {
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
