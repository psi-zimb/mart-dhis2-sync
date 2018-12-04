package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getDataValues;

@Component
public class UpdatedCompletedEnrollmentWithEventsProcessor extends EnrollmentWithEventProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Override
    public ProcessedTableRow process(Object tableRow) {
        return super.process(tableRow, mappingObj);
    }

    EnrollmentAPIPayLoad getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, List<Event> events) {
        return new EnrollmentAPIPayLoad(
               tableRowJsonObject.get("enrollment_id").getAsString(),
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
        if (StringUtils.isEmpty(tableRow.get("event_unique_id").getAsString())) {
            return null;
        }

        String eventDate = tableRow.get("event_date").getAsString();
        String dateString = getFormattedDateString(eventDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        return new Event(
                tableRow.get("event_id").getAsString(),
                tableRow.get("instance_id").getAsString(),
                tableRow.get("enrollment_id").getAsString(),
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
