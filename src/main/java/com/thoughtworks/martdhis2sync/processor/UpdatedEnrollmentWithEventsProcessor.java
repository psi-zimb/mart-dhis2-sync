package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonElement;
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
public class UpdatedEnrollmentWithEventsProcessor extends EnrollmentWithEventProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Override
    public ProcessedTableRow process(Object tableRow) {
        return super.process(tableRow, mappingObj);
    }

    EnrollmentAPIPayLoad getEnrollmentAPIPayLoad(JsonObject tableRowJsonObject, List<Event> events) {
        JsonElement enrolledProgram = tableRowJsonObject.get("enrolled_program");
        JsonElement program = tableRowJsonObject.get("program");
        JsonElement enrDate = tableRowJsonObject.get("enr_date");
        JsonElement enrollmentDate = tableRowJsonObject.get("enrollment_date");
        JsonElement incidentDate = tableRowJsonObject.get("incident_date");
        JsonElement eventProgramIncidentDate = tableRowJsonObject.get("event_program_incident_date");
        JsonElement enrollmentStatus = tableRowJsonObject.get("enrollment_status");
        JsonElement eventEnrollmentStatus = tableRowJsonObject.get("event_program_status");
        JsonElement programUniqueId = tableRowJsonObject.get("program_unique_id");
        JsonElement eventProgramUniqueId = tableRowJsonObject.get("event_program_unique_id");
        return new EnrollmentAPIPayLoad(
               tableRowJsonObject.get("enrollment_id").getAsString(),
               tableRowJsonObject.get("instance_id").getAsString(),
               hasValue(enrolledProgram) ? enrolledProgram.getAsString() : program.getAsString(),
               tableRowJsonObject.get("orgunit_id").getAsString(),
               getFormattedDateString(hasValue(enrDate) ? enrDate.getAsString() : enrollmentDate.getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               getFormattedDateString(hasValue(incidentDate) ? incidentDate.getAsString() : eventProgramIncidentDate.getAsString(),
                       DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
               hasValue(enrollmentStatus) ? enrollmentStatus.getAsString() : eventEnrollmentStatus.getAsString(),
               hasValue(programUniqueId) ? programUniqueId.getAsString() : eventProgramUniqueId.getAsString(),
               events
        );
    }

    Event getEvent(JsonObject tableRow, JsonObject mapping) {
        if (!hasValue(tableRow.get("event_unique_id"))) {
            return null;
        }

        String eventDate = tableRow.get("event_date").getAsString();
        JsonElement eventStatus = tableRow.get("status");
        String dateString = getFormattedDateString(eventDate, DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME);

        JsonElement eventId = tableRow.get("event_id");
        return new Event(
                StringUtils.isEmpty(eventId) ? "": eventId.getAsString(),
                tableRow.get("instance_id").getAsString(),
                tableRow.get("enrollment_id").getAsString(),
                tableRow.get("program").getAsString(),
                tableRow.get("program_stage").getAsString(),
                tableRow.get("orgunit_id").getAsString(),
                dateString,
                hasValue(eventStatus) ? eventStatus.getAsString() : Event.STATUS_COMPLETED,
                tableRow.get("event_unique_id").getAsString(),
                getDataValues(tableRow, mapping)
        );
    }

}
