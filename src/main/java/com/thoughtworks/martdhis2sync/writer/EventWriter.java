package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.EventTracker;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_ERROR;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_WARNING;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.GetUTCDateTimeAsString;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;

@Component
@StepScope
public class EventWriter implements ItemWriter {

    private static final String URI = "/api/events?strategy=CREATE_AND_UPDATE";

    @Value("#{jobParameters['service']}")
    private String programName;

    @Value("#{jobParameters['user']}")
    private String user;

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private MarkerUtil markerUtil;

    @Autowired
    private DataSource dataSource;

    private Iterator<EventTracker> trackerIterator;

    private static List<EventTracker> newEventsToSave = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "EVENT SYNC: ";

    @Override
    public void write(List list) throws Exception {
        PushController.IS_DELTA_EXISTS = true;
        StringBuilder eventApi = new StringBuilder("{\"events\":[");
        list.forEach(item -> eventApi.append(item).append(","));
        int length = eventApi.length();
        eventApi.replace(length - 1, length, "]}");

        ResponseEntity<DHISSyncResponse> eventResponse = syncRepository.sendData(URI, eventApi.toString());
        newEventsToSave.clear();
        trackerIterator = EventUtil.getEventTrackers().iterator();
        if (HttpStatus.OK.equals(eventResponse.getStatusCode())) {
            processResponse(eventResponse.getBody().getResponse().getImportSummaries());
            updateTracker();
            updateMarker();
        } else {
            processErrorResponse(eventResponse.getBody().getResponse().getImportSummaries());
            updateTracker();
            throw new Exception();
        }
    }

    private void processResponse(List<ImportSummary> importSummaries) {
        importSummaries.forEach(importSummary -> {
            if (isImported(importSummary)) {
                while (trackerIterator.hasNext()) {
                    EventTracker eventTracker = trackerIterator.next();
                    if (StringUtils.isEmpty(eventTracker.getEventId())) {
                        eventTracker.setEventId(importSummary.getReference());
                        newEventsToSave.add(eventTracker);
                        break;
                    }
                }
            }
        });
    }

    private void processErrorResponse(List<ImportSummary> importSummaries) {
        for (ImportSummary importSummary : importSummaries) {
            if (isIgnored(importSummary)) {
                if (trackerIterator.hasNext()) {
                    trackerIterator.next();
                }
                logger.error(LOG_PREFIX + importSummary.getDescription());
            } else if (isConflicted(importSummary)) {
                importSummary.getConflicts().forEach(conflict ->
                        logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue()));
                if(isImported(importSummary)) {
                    processResponse(Collections.singletonList(importSummary));
                } else {
                    if (trackerIterator.hasNext()) {
                        trackerIterator.next();
                    }
                }
            } else {
                processResponse(Collections.singletonList(importSummary));
            }
        }
    }

    private boolean isIgnored(ImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus()) && importSummary.getImportCount().getIgnored() > 0
                && !StringUtils.isEmpty(importSummary.getDescription());
    }

    private boolean isImported(ImportSummary importSummary) {
        return (IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(importSummary.getStatus())
                || IMPORT_SUMMARY_RESPONSE_WARNING.equals(importSummary.getStatus()))
                && importSummary.getImportCount().getImported() > 0;
    }

    private boolean isConflicted(ImportSummary importSummary) {
        return (IMPORT_SUMMARY_RESPONSE_ERROR.equals(importSummary.getStatus())
                || IMPORT_SUMMARY_RESPONSE_WARNING.equals(importSummary.getStatus()))
                && importSummary.getImportCount().getIgnored() > 0
                && !importSummary.getConflicts().isEmpty();
    }

    private void updateTracker() {
        String sqlQuery = "INSERT INTO public.event_tracker(" +
                "event_id, instance_id, program, program_stage, event_unique_id, created_by, date_created)" +
                "values (?, ?, ?, ?, ?, ?, ?)";

        try {
            if (!newEventsToSave.isEmpty()) {
                int recordsCreated = getUpdateCount(sqlQuery);
                logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " Event UIDs.");
            }
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting Event UIDs:" + e.getMessage());
            e.printStackTrace();
        }
    }

    private int getUpdateCount(String sqlQuery) throws SQLException {
        int updateCount;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                updateCount = 0;
                for (EventTracker eventTracker : newEventsToSave) {
                    ps.setString(1, eventTracker.getEventId());
                    ps.setString(2, eventTracker.getInstanceId());
                    ps.setString(3, eventTracker.getProgram());
                    ps.setString(4, eventTracker.getProgramStage());
                    ps.setString(5, eventTracker.getEventUniqueId());
                    ps.setString(6, user);
                    ps.setTimestamp(7, Timestamp.valueOf(GetUTCDateTimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, CATEGORY_EVENT,
                getStringFromDate(EventUtil.date, DATEFORMAT_WITH_24HR_TIME));
    }
}
