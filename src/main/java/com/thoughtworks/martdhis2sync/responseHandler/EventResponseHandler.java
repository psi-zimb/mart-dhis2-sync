package com.thoughtworks.martdhis2sync.responseHandler;

import com.thoughtworks.martdhis2sync.model.*;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_WARNING;
import static com.thoughtworks.martdhis2sync.util.EventUtil.eventsToSaveInTracker;

@Component
public class EventResponseHandler {

    @Autowired
    private LoggerService loggerService;

    public void process(Collection<EnrollmentAPIPayLoad> payLoads, List<EnrollmentImportSummary> importSummaries, List<EventTracker> eventTrackers, Logger logger, String logPrefix) {
        Iterator<EventTracker> eventTrackerIterator = eventTrackers.iterator();
        Iterator<EnrollmentAPIPayLoad> finalIterator = payLoads.iterator();
        importSummaries.forEach(summary -> {
            EnrollmentAPIPayLoad payLoad = finalIterator.next();
            Response eventsResponse = summary.getEvents();
            if (eventsResponse == null) {
                List<Event> events = payLoad.getEvents();
                events.forEach(i -> eventTrackerIterator.next());
            } else if (IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(eventsResponse.getStatus())) {
                processImportSummaries(eventsResponse.getImportSummaries(), eventTrackerIterator);
            } else {
                processErrorResponse(eventsResponse.getImportSummaries(), eventTrackerIterator, logger, logPrefix);
            }
        });
    }

    private void processImportSummaries(List<ImportSummary> importSummaries, Iterator<EventTracker> eventTrackerIterator) {
        importSummaries.forEach(importSummary -> {
            EventTracker eventTracker = eventTrackerIterator.next();
            if (isImported(importSummary)) {
                eventTracker.setEventId(importSummary.getReference());
                eventsToSaveInTracker.add(eventTracker);
            }
        });
    }

    private void processErrorResponse(List<ImportSummary> importSummaries, Iterator<EventTracker> eventTrackerIterator, Logger logger, String logPrefix) {
        for (ImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                eventTrackerIterator.next();
                logger.error(logPrefix + importSummary.getDescription());
                loggerService.collateLogMessage(String.format("%s", importSummary.getDescription()));
            } else if (isConflicted(importSummary)) {
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(logPrefix + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
                if(isImported(importSummary)) {
                    processImportSummaries(Collections.singletonList(importSummary), eventTrackerIterator);
                } else {
                    eventTrackerIterator.next();
                }
            } else {
                processImportSummaries(Collections.singletonList(importSummary), eventTrackerIterator);
            }
        }
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
}
