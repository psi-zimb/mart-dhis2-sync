package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISEnrollmentSyncResponse;
import com.thoughtworks.martdhis2sync.model.EnrollmentAPIPayLoad;
import com.thoughtworks.martdhis2sync.model.EnrollmentImportSummary;
import com.thoughtworks.martdhis2sync.model.Event;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.model.ProcessedTableRow;
import com.thoughtworks.martdhis2sync.model.Response;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
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

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_WARNING;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.removeLastChar;
import static com.thoughtworks.martdhis2sync.util.EnrollmentUtil.enrollmentsToSaveInTracker;
import static com.thoughtworks.martdhis2sync.util.EventUtil.eventsToSaveInTracker;

@Component
@StepScope
public class NewCompletedEnrollmentWithEventsWriter implements ItemWriter<ProcessedTableRow> {
    private static final String URI = "/api/enrollments?strategy=CREATE_AND_UPDATE";

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private LoggerService loggerService;

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "NEW COMPLETED ENROLLMENT SYNC: ";

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
        clearTrackerLists();
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
            processEnrollmentImportSummaries(enrollmentImportSummaries, iterator);
            processEventResponse(payLoads, enrollmentImportSummaries);
        } else {
            processEnrollmentErrorResponse(enrollmentImportSummaries, iterator);
            processEventResponse(payLoads, enrollmentImportSummaries);
        }
    }

    private void processEventResponse(Collection<EnrollmentAPIPayLoad> payLoads, List<EnrollmentImportSummary> importSummaries) {
        Iterator<EventTracker> eventTrackerIterator = eventTrackers.iterator();
        Iterator<EnrollmentAPIPayLoad> finalIterator = payLoads.iterator();
        importSummaries.forEach(summary -> {
            EnrollmentAPIPayLoad payLoad = finalIterator.next();
            Response eventsResponse = summary.getEvents();
            if (eventsResponse == null) {
                List<Event> events = payLoad.getEvents();
                events.forEach(i -> eventTrackerIterator.next());
            } else if (IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(eventsResponse.getStatus())) {
                processEventImportSummaries(eventsResponse.getImportSummaries(), eventTrackerIterator);
            } else {
                processEventErrorResponse(eventsResponse.getImportSummaries(), eventTrackerIterator);
            }
        });
    }

    private void processEventImportSummaries(List<ImportSummary> importSummaries, Iterator<EventTracker> eventTrackerIterator) {
        importSummaries.forEach(importSummary -> {
            EventTracker eventTracker = eventTrackerIterator.next();
            eventTracker.setEventId(importSummary.getReference());
            eventsToSaveInTracker.add(eventTracker);
        });
    }

    private void processEventErrorResponse(List<ImportSummary> importSummaries, Iterator<EventTracker> eventTrackerIterator) {
        for (ImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                eventTrackerIterator.next();
                logger.error(LOG_PREFIX + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
                if(isImported(importSummary)) {
                    processEventImportSummaries(Collections.singletonList(importSummary), eventTrackerIterator);
                } else {
                    eventTrackerIterator.next();
                }
            } else {
                processEventImportSummaries(Collections.singletonList(importSummary), eventTrackerIterator);
            }
        }
    }

    private void processEnrollmentImportSummaries(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator) {
        importSummaries.forEach(importSummary -> {
            EnrollmentAPIPayLoad enrollment = payLoadIterator.next();
            enrollment.setEnrollmentId(importSummary.getReference());
            enrollmentsToSaveInTracker.add(enrollment);
        });
    }

    private void processEnrollmentErrorResponse(List<EnrollmentImportSummary> importSummaries, Iterator<EnrollmentAPIPayLoad> payLoadIterator) {
        for (EnrollmentImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                payLoadIterator.next();
                logger.error(LOG_PREFIX + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                payLoadIterator.next();
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
            } else {
                processEnrollmentImportSummaries(Collections.singletonList(importSummary), payLoadIterator);
            }
        }
    }

    private boolean isIgnored(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isConflicted(EnrollmentImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() == 1
                && !importSummary.getConflicts().isEmpty();
    }

    private boolean isIgnored(ImportSummary importSummary) {
        return (IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus())
                || IMPORT_SUMMARY_RESPONSE_WARNING.equals(importSummary.getStatus()))
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isConflicted(ImportSummary importSummary) {
        return (IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus())
                || IMPORT_SUMMARY_RESPONSE_WARNING.equals(importSummary.getStatus()))
                && !importSummary.getConflicts().isEmpty();
    }

    private boolean isImported(ImportSummary importSummary) {
        return (IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(importSummary.getStatus())
                || IMPORT_SUMMARY_RESPONSE_WARNING.equals(importSummary.getStatus()))
                && importSummary.getImportCount().getImported() > 0;
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
            List<Event> events = value.getEvents();
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
                        getEventBody(events)
                ))
                .append(",");

            eventTrackers.addAll(EventUtil.getEventTrackers(events));
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

    private void clearTrackerLists() {
        eventTrackers.clear();
        eventsToSaveInTracker.clear();
        enrollmentsToSaveInTracker.clear();
    }
}
