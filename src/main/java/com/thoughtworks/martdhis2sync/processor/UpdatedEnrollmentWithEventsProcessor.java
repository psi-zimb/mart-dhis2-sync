package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.model.TrackedEntityInstanceInfo;
import com.thoughtworks.martdhis2sync.service.TEIService;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Optional;

import static com.thoughtworks.martdhis2sync.controller.PushController.suggestedRemovableDuplicatesSet;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getDataValues;

@Component
public class UpdatedEnrollmentWithEventsProcessor extends EnrollmentWithEventProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Autowired
    private TEIService teiService;


    @Override
    public ProcessedTableRow process(Object tableRow) throws Exception {
        return super.process(tableRow, mappingObj);
    }

    Optional<EnrollmentAPIPayLoad> getEnrollmentAPIPayLoad(JsonObject localInstanceJsonObject, List<Event> events) throws Exception {
        JsonElement enrolledProgram = localInstanceJsonObject.get("enrolled_program");
        JsonElement program = localInstanceJsonObject.get("program");
        JsonElement enrDate = localInstanceJsonObject.get("enr_date");
        JsonElement enrollmentDate = localInstanceJsonObject.get("enrollment_date");
        JsonElement incidentDate = localInstanceJsonObject.get("incident_date");
        JsonElement eventProgramIncidentDate = localInstanceJsonObject.get("event_program_incident_date");
        JsonElement enrollmentStatus = localInstanceJsonObject.get("enrollment_status");
        JsonElement eventEnrollmentStatus = localInstanceJsonObject.get("event_program_status");
        JsonElement programUniqueId = localInstanceJsonObject.get("program_unique_id");
        JsonElement eventProgramUniqueId = localInstanceJsonObject.get("event_program_unique_id");
        String uic = localInstanceJsonObject.get("uic").getAsString();

        List<TrackedEntityInstanceInfo> trackedEntityInstancesInDHIS = teiService.getTrackedEntityInstancesForUIC(uic);
        if (trackedEntityInstancesInDHIS.size()>1){
            suggestedRemovableDuplicatesSet.add(uic);
        }
        if (teiService.instanceExistsInDHIS(localInstanceJsonObject, trackedEntityInstancesInDHIS)) {
            return Optional.of(new EnrollmentAPIPayLoad(
                    localInstanceJsonObject.get("enrollment_id").getAsString(),
                    localInstanceJsonObject.get("instance_id").getAsString(),
                    hasValue(enrolledProgram) ? enrolledProgram.getAsString() : program.getAsString(),
                    localInstanceJsonObject.get("orgunit_id").getAsString(),
                    getFormattedDateString(hasValue(enrDate) ? enrDate.getAsString() : enrollmentDate.getAsString(),
                            DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                    getFormattedDateString(hasValue(incidentDate) ? incidentDate.getAsString() : eventProgramIncidentDate.getAsString(),
                            DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                    hasValue(enrollmentStatus) ? enrollmentStatus.getAsString() : eventEnrollmentStatus.getAsString(),
                    hasValue(programUniqueId) ? programUniqueId.getAsString() : eventProgramUniqueId.getAsString(),
                    events
            ));
        } else {
            return Optional.of(new EnrollmentAPIPayLoad());
        }
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
