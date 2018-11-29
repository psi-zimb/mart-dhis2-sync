package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;

@Component
@StepScope
public class NewCompletedEnrollmentWriter implements ItemWriter<ProcessedTableRow> {
    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    private SyncRepository syncRepository;

    private static final String EVENT_API_FORMAT = "{" +
                "\"event\":\"%s\", " +
                "\"trackedEntityInstance\":\"%s\", " +
                "\"enrollment\":\"%s\", " +
                "\"program\":\"%s\", " +
                "\"programStage\":\"%s\", " +
                "\"orgUnit\":\"%s\", " +
                "\"eventDate\":\"%s\", " +
                "\"status\":\"%s\", " +
                "\"dataValues\":[%s]" +
            "}";

    private static final String ENROLLMENT_API_FORMAT = "{" +
                "\"enrollment\":\"%s\", " +
                "\"trackedEntityInstance\":\"%s\", " +
                "\"orgUnit\":\"%s\", " +
                "\"program\":\"%s\", " +
                "\"enrollmentDate\":\"%s\", " +
                "\"incidentDate\":\"%s\", " +
                "\"status\":\"%s\", " +
                "\"events\":[%s]" +
            "}";

    @Override
    public void write(List<? extends ProcessedTableRow> tableRows) throws Exception {
        Map<String, EnrollmentAPIPayLoad> groupedEnrollmentPayLoad = getGroupedEnrollmentPayLoad(tableRows);
        String apiBody = getAPIBody(groupedEnrollmentPayLoad);
        System.out.println(apiBody);
        ResponseEntity<DHISSyncResponse> enrollmentResponse = syncRepository.sendData(URI, apiBody);
    }

    private Map<String, EnrollmentAPIPayLoad> getGroupedEnrollmentPayLoad(List<? extends ProcessedTableRow> tableRows) {
        Map<String, EnrollmentAPIPayLoad> groupedEnrollments = new HashMap<>();
        tableRows.forEach(row -> {
            if (groupedEnrollments.containsKey(row.getPatientIdentifier())) {
                EnrollmentAPIPayLoad enrollmentAPIPayLoad = groupedEnrollments.get(row.getPatientIdentifier());
                Event incomingEvent = row.getPayLoad().getEvents().get(0);
                enrollmentAPIPayLoad.getEvents().add(incomingEvent);
            } else {
                groupedEnrollments.put(row.getPatientIdentifier(), row.getPayLoad());
            }
        });

        return groupedEnrollments;
    }

    private String getAPIBody(Map<String, EnrollmentAPIPayLoad> groupedEnrollmentPayLoad) {
        StringBuilder body = new StringBuilder("");

        groupedEnrollmentPayLoad.forEach((key, value) -> {
            body
                .append(String.format(
                        ENROLLMENT_API_FORMAT,
                        "",
                        value.getInstanceId(),
                        value.getOrgUnit(),
                        value.getProgram(),
                        value.getProgramStartDate(),
                        value.getIncidentDate(),
                        value.getStatus(),
                        getEventBody(value.getEvents())
                ))
                .append(",");
        });

        return String.format("{\"enrollments\":[%s]}", removeLastChar(body));
    }

    private String getEventBody(List<Event> events) {
        StringBuilder eventsApiBuilder = new StringBuilder();
        events.forEach(event -> {
            eventsApiBuilder
                .append(String.format(EVENT_API_FORMAT,
                        "",
                        event.getTrackedEntityInstance(),
                        event.getEnrollment(),
                        event.getProgram(),
                        event.getProgramStage(),
                        event.getOrgUnit(),
                        event.getEventDate(),
                        event.getStatus(),
                        getDataValues(event.getDataValues())
                ))
                .append(",");
        });

        return removeLastChar(eventsApiBuilder);
    }

    private String getDataValues(Map<String, String> dataValues) {
        StringBuilder dataValuesApiBuilder = new StringBuilder();
        dataValues.forEach((key, value) -> {
            dataValuesApiBuilder.append(
                    String.format("{\"dataElement\":\"%s\", \"value\":\"%s\"},", key, value)
            );
        });

        return removeLastChar(dataValuesApiBuilder);
    }
}
