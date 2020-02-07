package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.controller.PushController;
import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.service.JobService;
import com.thoughtworks.martdhis2sync.service.LoggerService;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;

import static com.thoughtworks.martdhis2sync.model.ImportSummary.IMPORT_SUMMARY_RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_INSTANCE;

@Component
@StepScope
public class TrackedEntityInstanceWriter implements ItemWriter {

    private static final String EMPTY_STRING = "\"\"";
    private static Map<String, String> newTEIUIDs = new LinkedHashMap<>();
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String LOG_PREFIX = "TEI SYNC: ";
    private boolean isSyncFailure;

    private static final String URI = "/api/trackedEntityInstances?strategy=CREATE_AND_UPDATE";

    @Value("#{jobParameters['user']}")
    private String user;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private MarkerUtil markerUtil;

    @Autowired
    private LoggerService loggerService;

    @Value("#{jobParameters['service']}")
    private String programName;

    private Iterator<Entry<String, String>> mapIterator;

    @Override
    public void write(List list) throws Exception {
        PushController.IS_DELTA_EXISTS = true;
        try {
            StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");

            list.forEach(item -> instanceApiFormat.append(item).append(","));
            instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");

            isSyncFailure = false;
            ResponseEntity<DHISSyncResponse> responseEntity = null;
            try {
                responseEntity = syncRepository.sendData(URI, instanceApiFormat.toString());
            } catch (Exception e) {
                isSyncFailure = true;
                JobService.setIS_JOB_FAILED(true);
                throw new Exception();
            }
            mapIterator = TEIUtil.getPatientIdTEIUidMap().entrySet().iterator();
            newTEIUIDs.clear();

            TEIUtil.getTrackedEntityInstanceIDs().forEach((key, value) -> newTEIUIDs.put(getUnquotedString(key), getUnquotedString(value)));
            TEIUtil.resetTrackedEntityInstaceIDs();

            if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
                processResponse(responseEntity.getBody().getResponse().getImportSummaries());
            } else {
                isSyncFailure = true;
                if (!StringUtils.isEmpty(responseEntity) && !StringUtils.isEmpty(responseEntity.getBody())) {
                    processErrorResponse(responseEntity.getBody().getResponse().getImportSummaries());
                }
            }
            updateTracker();
        } catch(Exception e){
            isSyncFailure = true;
        }
        if (isSyncFailure) {
            throw new Exception();
        } else {
            updateMarker();
        }
        TEIUtil.resetPatientTEIUidMap();
    }

    private void processErrorResponse(List<ImportSummary> importSummaries) {
        for (ImportSummary importSummary : importSummaries) {
            if (isConflicted(importSummary)) {
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getObject() + ": " + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s: %s", conflict.getObject(), conflict.getValue()));
                });
                if (mapIterator.hasNext()) {
                    mapIterator.next();
                }
            } else {
                processResponse(Collections.singletonList(importSummary));
            }
        }
    }

    private void processResponse(List<ImportSummary> importSummaries) {
        importSummaries.forEach(importSummary -> {
            if (isImported(importSummary)) {
                while (mapIterator.hasNext()) {
                    Entry<String, String> entry = mapIterator.next();
                    if (EMPTY_STRING.equals(entry.getValue())) {
                        newTEIUIDs.put(getUnquotedString(entry.getKey()), importSummary.getReference());
                        break;
                    }
                }
            } else if (isConflicted(importSummary)) {
                isSyncFailure = true;
                importSummary.getConflicts().forEach(conflict -> {
                    logger.error(LOG_PREFIX + conflict.getValue());
                    loggerService.collateLogMessage(String.format("%s", conflict.getValue()));
                });
                if (mapIterator.hasNext()) {
                    mapIterator.next();
                }
            }
        });
    }

    private boolean isConflicted(ImportSummary importSummary) {
        return !importSummary.getConflicts().isEmpty();
    }

    private boolean isImported(ImportSummary importSummary) {
        return IMPORT_SUMMARY_RESPONSE_SUCCESS.equals(importSummary.getStatus()) && importSummary.getImportCount().getImported() == 1;
    }

    private void updateTracker() throws SQLException {

        String sqlQuery = "INSERT INTO public.instance_tracker(patient_id, instance_id, created_by, date_created) values (? , ?, ?, ?)";

        if (!newTEIUIDs.isEmpty()) {
            int updateCount;
            try (Connection connection = dataSource.getConnection()) {
                try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                    updateCount = 0;
                    for (Entry entry : newTEIUIDs.entrySet()) {
                        ps.setString(1, entry.getKey().toString());
                        ps.setString(2, entry.getValue().toString());
                        ps.setString(3, user);
                        ps.setTimestamp(4, Timestamp.valueOf(BatchUtil.GetUTCDateTimeAsString()));
                        updateCount += ps.executeUpdate();
                    }
                }
            }
            logger.info(LOG_PREFIX + "Successfully inserted " + updateCount + " TrackedEntityInstance UIDs.");
        }
    }

    private void updateMarker() {
        String teiDate = BatchUtil.getStringFromDate(TEIUtil.date, BatchUtil.DATEFORMAT_WITH_24HR_TIME);
        logger.info("updating marker with date : " + teiDate);
        markerUtil.updateMarkerEntry(programName, CATEGORY_INSTANCE, teiDate);
    }
}
