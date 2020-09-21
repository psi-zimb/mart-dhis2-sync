package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentDetails;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.EnrollmentResponse;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.responseHandler.EnrollmentResponseHandler;
import com.thoughtworks.martdhis2sync.responseHandler.EventResponseHandler;
import com.thoughtworks.martdhis2sync.service.JobService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;
import static com.thoughtworks.martdhis2sync.util.EventUtil.getEventTrackers;
import static com.thoughtworks.martdhis2sync.util.EventUtil.placeNewEventsFirst;

@Component
@StepScope
public class UpdatedEnrollmentWithEventsWriter {

    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    protected SyncRepository syncRepository;

    @Autowired
    protected EnrollmentResponseHandler enrollmentResponseHandler;

    @Autowired
    protected EventResponseHandler eventResponseHandler;

    @Autowired
    protected LoggerService loggerService;

    @Value("#{jobParameters['service']}")
    protected String programName;

    @Autowired
    protected MarkerUtil markerUtil;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "UPDATE COMPLETED ENROLLMENT WITH EVENTS SYNC: ";

    protected List<EventTracker> eventTrackers = new ArrayList<>();

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

    protected void processWrite(List<? extends ProcessedTableRow> tableRows) throws Exception {
        ResponseEntity<DHISEnrollmentSyncResponse> enrollmentResponse = null;
        Collection<EnrollmentAPIPayLoad> payLoads = null;
        try {
            PushController.IS_DELTA_EXISTS = true;
            eventTrackers.clear();
            Map<String, EnrollmentAPIPayLoad> groupedEnrollmentPayLoad = getGroupedEnrollmentPayLoad(tableRows);
            payLoads = groupedEnrollmentPayLoad.values();
            String apiBody = getAPIBody(groupedEnrollmentPayLoad);
            if (!JobService.isIS_JOB_FAILED()) {
                enrollmentResponse = syncRepository.sendEnrollmentDataForUpdate(URI, apiBody);
            }
        } catch (Exception e){
            JobService.setIS_JOB_FAILED(true);
            throw new Exception();
        }

        processResponseEntity(enrollmentResponse, payLoads);
    }

    private void processResponseEntity(ResponseEntity<DHISEnrollmentSyncResponse> responseEntity, Collection<EnrollmentAPIPayLoad> payLoads) throws Exception {
        Iterator<EnrollmentAPIPayLoad> iterator = payLoads.iterator();
        List<EnrollmentImportSummary> enrollmentImportSummaries = null;
        if(responseEntity != null) {
            EnrollmentResponse response = responseEntity.getBody().getResponse();
            enrollmentImportSummaries = response == null ?
                    Collections.emptyList()
                    : response.getImportSummaries();
            if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
                enrollmentResponseHandler.processImportSummaries(enrollmentImportSummaries, iterator);
                eventResponseHandler.process(payLoads, enrollmentImportSummaries, eventTrackers, logger, LOG_PREFIX);
            } else {
                JobService.setIS_JOB_FAILED(true);
                String message = responseEntity.getBody().getMessage();
                if (!StringUtils.isEmpty(message)) {
                    logger.error(LOG_PREFIX + message);
                    loggerService.collateLogMessage(String.format("%s", message));
                } else {
                    enrollmentResponseHandler.processErrorResponse(enrollmentImportSummaries, iterator, logger, LOG_PREFIX);
                    eventResponseHandler.process(payLoads, enrollmentImportSummaries, eventTrackers, logger, LOG_PREFIX);
                }
            }
        }
    }


    private Map<String, EnrollmentAPIPayLoad> getGroupedEnrollmentPayLoad(List<? extends ProcessedTableRow> tableRows) {
        Map<String, EnrollmentAPIPayLoad> groupedEnrollments = new HashMap<>();
        tableRows.forEach(row -> {
            if (groupedEnrollments.containsKey(row.getProgramUniqueId())) {
                EnrollmentAPIPayLoad enrollmentAPIPayLoad = groupedEnrollments.get(row.getProgramUniqueId());
                List<Event> events = row.getPayLoad().getEvents();
                if (events.size() > 0) {
                    enrollmentAPIPayLoad.getEvents().add(events.get(0));
                }
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
                            getEnrollmentId(value),
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
                    String.format("{\"dataElement\":\"%s\", \"value\":\"%s\"},", key, BatchUtil.getEscapedString(value))
            );
        });

        return removeLastChar(dataValuesApiBuilder);
    }

    private String getEnrollmentId(EnrollmentAPIPayLoad enrollment) {
        List<EnrollmentDetails> enrollmentDetails = TEIUtil.getInstancesWithEnrollments().get(enrollment.getInstanceId());

        String activeEnrollmentId = getActiveEnrollmentId(enrollmentDetails);

        if (enrollment.getEnrollmentId().equals(activeEnrollmentId) || StringUtils.isEmpty(activeEnrollmentId)) {
            return enrollment.getEnrollmentId();
        }

        JobService.setIS_JOB_FAILED(true);
        String message = "DHIS has another active enrollment going on. Can't complete this enrollment. " +
                "BAHMNI enrollment id: %s and DHIS enrollment id: %s";
        logger.error(LOG_PREFIX + String.format(message, enrollment.getEnrollmentId(), activeEnrollmentId));
        loggerService.collateLogMessage(String.format(message, enrollment.getEnrollmentId(), activeEnrollmentId));

        return "";
    }

    private String getActiveEnrollmentId(List<EnrollmentDetails> enrollmentDetails) {
        if(enrollmentDetails == null)
            return "";
        Optional<EnrollmentDetails> activeEnrollment = enrollmentDetails.stream()
                .filter(enrollment -> EnrollmentAPIPayLoad.STATUS_ACTIVE.equals(enrollment.getStatus()))
                .findFirst();

        return activeEnrollment.isPresent() ? activeEnrollment.get().getEnrollment() : "";
    }
}
