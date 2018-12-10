package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getEventTrackers;
import static com.thoughtworks.martdhis2sync.util.EventUtil.placeNewEventsFirst;

@Component
public class UpdatedCompletedEnrollmentWithEventsWriter implements ItemWriter<ProcessedTableRow> {

    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private EnrollmentResponseHandler enrollmentResponseHandler;

    @Autowired
    private EventResponseHandler eventResponseHandler;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "UPDATE COMPLETED ENROLLMENT WITH EVENTS SYNC: ";

    private List<EventTracker> eventTrackers = new ArrayList<>();

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
        PushController.IS_DELTA_EXISTS = true;
        eventTrackers.clear();
        Map<String, EnrollmentAPIPayLoad> groupedEnrollmentPayLoad = getGroupedEnrollmentPayLoad(tableRows);
        Collection<EnrollmentAPIPayLoad> payLoads = groupedEnrollmentPayLoad.values();
        String apiBody = getAPIBody(groupedEnrollmentPayLoad);
        ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse = syncRepository.sendEnrollmentData(URI, apiBody);
        processResponseEntity(enrollmentResponse, payLoads);
    }

    private void processResponseEntity(ResponseEntity<DHISEnrollmentSyncResponse> responseEntity, Collection<EnrollmentAPIPayLoad> payLoads) throws Exception {
        Iterator<EnrollmentAPIPayLoad> iterator = payLoads.iterator();
        List<EnrollmentImportSummary> enrollmentImportSummaries = responseEntity.getBody().getResponse().getImportSummaries();
        if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            enrollmentResponseHandler.processImportSummaries(enrollmentImportSummaries, iterator);
            eventResponseHandler.process(payLoads, enrollmentImportSummaries, eventTrackers, logger, LOG_PREFIX);
        } else {
            JobService.setIS_JOB_FAILED(true);
            enrollmentResponseHandler.processErrorResponse(enrollmentImportSummaries, iterator, logger, LOG_PREFIX);
            eventResponseHandler.process(payLoads, enrollmentImportSummaries, eventTrackers, logger, LOG_PREFIX);
        }
    }


    private Map<String, EnrollmentAPIPayLoad> getGroupedEnrollmentPayLoad(List<? extends ProcessedTableRow> tableRows) {
        Map<String, EnrollmentAPIPayLoad> groupedEnrollments = new HashMap<>();
        tableRows.forEach(row -> {
            if (groupedEnrollments.containsKey(row.getProgramUniqueId())) {
                EnrollmentAPIPayLoad enrollmentAPIPayLoad = groupedEnrollments.get(row.getProgramUniqueId());
                Event incomingEvent = row.getPayLoad().getEvents().get(0);
                enrollmentAPIPayLoad.getEvents().add(incomingEvent);
            } else {
                groupedEnrollments.put(row.getProgramUniqueId(), row.getPayLoad());
            }
        });

        return groupedEnrollments;
    }

    private String getAPIBody(Map<String, EnrollmentAPIPayLoad> groupedEnrollmentPayLoad) {
        StringBuilder body = new StringBuilder("");

        groupedEnrollmentPayLoad.forEach((key, value) -> {
            List<Event> events = value.getEvents();
            body
                    .append(String.format(
                            ENROLLMENT_API_FORMAT,
                            value.getEnrollmentId(),
                            value.getInstanceId(),
                            value.getOrgUnit(),
                            value.getProgram(),
                            value.getProgramStartDate(),
                            value.getIncidentDate(),
                            EnrollmentAPIPayLoad.STATUS_ACTIVE,
                            getEventBody(events)
                    ))
                    .append(",");

            eventTrackers.addAll(getEventTrackers(placeNewEventsFirst(events)));
        });

        return String.format("{\"enrollments\":[%s]}", removeLastChar(body));
    }

    private String getEventBody(List<Event> events) {
        StringBuilder eventsApiBuilder = new StringBuilder();
        events.forEach(event -> {
            eventsApiBuilder
                    .append(String.format(EVENT_API_FORMAT,
                            event.getEvent(),
                            event.getTrackedEntityInstance(),
                            event.getEnrollment(),
                            event.getProgram(),
                            event.getProgramStage(),
                            event.getOrgUnit(),
                            event.getEventDate(),
                            event.getStatus().toUpperCase(),
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
