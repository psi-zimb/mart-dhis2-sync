package com.thoughtworks.martdhis2sync.processor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.EnrollmentService;
import com.thoughtworks.martdhis2sync.service.TEIService;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import lombok.Setter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

import static com.thoughtworks.martdhis2sync.controller.PushController.suggestedRemovableDuplicatesSet;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.*;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getDataValues;

@Component
public class NewEnrollmentWithEventsProcessor extends EnrollmentWithEventProcessor implements ItemProcessor {

    @Setter
    private Object mappingObj;

    @Autowired
    private TEIService teiService;

    @Autowired
    private EnrollmentService enrollmentService;

    @Override
    public ProcessedTableRow process(Object tableRow) throws Exception {
        return super.process(tableRow, mappingObj);
    }

    Optional<EnrollmentAPIPayLoad> getEnrollmentAPIPayLoad(JsonObject localInstanceJsonObject, List<Event> events) throws Exception {
        List<EnrollmentDetails> enrollmentDetails = TEIUtil.getInstancesWithEnrollments().get(localInstanceJsonObject.get("instance_id").getAsString());
        String enrollmentId = "";
        if (enrollmentDetails != null) {
            Optional<EnrollmentDetails> activeEnrollment = enrollmentDetails.stream()
                    .filter(enrollment -> EnrollmentAPIPayLoad.STATUS_ACTIVE.equals(enrollment.getStatus()))
                    .findFirst();
            enrollmentId = activeEnrollment.isPresent() ? activeEnrollment.get().getEnrollment() : "";
        }
        if ("".equals(enrollmentId)) {
            enrollmentId = EnrollmentUtil.instanceIDEnrollmentIDMap.getOrDefault(localInstanceJsonObject.get("instance_id").getAsString(), "");
        }

        String uic = localInstanceJsonObject.get("uic").getAsString();
        List<TrackedEntityInstanceInfo> trackedEntityInstancesInDHIS = teiService.getTrackedEntityInstancesForUIC(uic);
        if (trackedEntityInstancesInDHIS.size()>1){
            suggestedRemovableDuplicatesSet.add(uic);
        }
        if (teiService.instanceExistsInDHIS(localInstanceJsonObject, trackedEntityInstancesInDHIS)) {
            return Optional.of(new EnrollmentAPIPayLoad(
                    enrollmentId,
                    localInstanceJsonObject.get("instance_id").getAsString(),
                    localInstanceJsonObject.get("enrolled_program").getAsString(),
                    localInstanceJsonObject.get("orgunit_id").getAsString(),
                    getFormattedDateString(localInstanceJsonObject.get("enr_date").getAsString(),
                            DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                    getFormattedDateString(localInstanceJsonObject.get("incident_date").getAsString(),
                            DATEFORMAT_WITH_24HR_TIME, DATEFORMAT_WITHOUT_TIME),
                    localInstanceJsonObject.get("enrollment_status").getAsString(),
                    localInstanceJsonObject.get("program_unique_id").getAsString(),
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

        return new Event(
                "",
                tableRow.get("instance_id").getAsString(),
                "",
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
