package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.model.DHISSyncResponse;
import com.thoughtworks.martdhis2sync.model.ImportSummary;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.thoughtworks.martdhis2sync.model.Conflict.CONFLICT_OBJ_ATTRIBUTE;
import static com.thoughtworks.martdhis2sync.model.Conflict.CONFLICT_OBJ_TEI_TYPE;
import static com.thoughtworks.martdhis2sync.model.ImportSummary.RESPONSE_SUCCESS;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getUnquotedString;

@Component
@StepScope
public class TrackedEntityInstanceWriter implements ItemWriter {

    private static final String EMPTY_STRING = "\"\"";
    private static Map<String, String> newTEIUIDs = new LinkedHashMap<>();
    private Logger logger = LoggerFactory.getLogger(TrackedEntityInstanceWriter.class);
    private static final String LOG_PREFIX = "TEI SYNC: ";
    private static boolean IS_SYNC_SUCCESS = true;

    @Value("${uri.tei}")
    private String teiUri;

    @Value("#{jobParameters['user']}")
    private String user;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private MarkerUtil markerUtil;

    @Value("#{jobParameters['service']}")
    private String programName;

    @Override
    public void write(List list) throws Exception {
        StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");
        list.forEach(item -> instanceApiFormat.append(item).append(","));
        instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");

        IS_SYNC_SUCCESS = true;
        ResponseEntity<DHISSyncResponse> responseEntity = syncRepository.sendData(teiUri, instanceApiFormat.toString());

        if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            processResponse(responseEntity.getBody().getResponse().getImportSummaries());
            if (IS_SYNC_SUCCESS) {
                updateMarker();
            }
        } else {
            IS_SYNC_SUCCESS = false;
            processErrorResponse(responseEntity.getBody().getResponse().getImportSummaries());
        }

        if(!IS_SYNC_SUCCESS){
            throw new Exception();
        }
    }

    private void processErrorResponse(List<ImportSummary> importSummaries) {
        ImportSummary importSummary = importSummaries.get(0);
        if (isConflicted(importSummary)) {
            String conflictObject = importSummary.getConflicts().get(0).getObject();
            if (CONFLICT_OBJ_TEI_TYPE.equals(conflictObject) ||
                    CONFLICT_OBJ_ATTRIBUTE.equals(conflictObject)) {
                logger.error(LOG_PREFIX + conflictObject + ": " + importSummary.getConflicts().get(0).getValue());
            } else {
                processResponse(importSummaries);
            }
        }
    }

    private void processResponse(List<ImportSummary> importSummaries) {
        newTEIUIDs.clear();
        Iterator<Entry<String, String>> mapIterator = TEIUtil.getPatientIdTEIUidMap().entrySet().iterator();

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
                IS_SYNC_SUCCESS = false;
                importSummary.getConflicts().forEach(
                        conflict -> logger.error(LOG_PREFIX + conflict.getObject() + conflict.getValue()));
                if (mapIterator.hasNext()) {
                    mapIterator.next();
                }
            }
        });
        try {
            if (!newTEIUIDs.isEmpty()) {
                int recordsCreated = updateTracker();
                logger.info(LOG_PREFIX + "Successfully inserted " + recordsCreated + " TrackedEntityInstance UIDs.");
            }
        } catch (SQLException e) {
            logger.error(LOG_PREFIX + "Exception occurred while inserting TrackedEntityInstance UIDs:" + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean isConflicted(ImportSummary importSummary) {
        return !importSummary.getConflicts().isEmpty();
    }

    private boolean isImported(ImportSummary importSummary) {
        return RESPONSE_SUCCESS.equals(importSummary.getStatus()) && importSummary.getImportCount().getImported() == 1;
    }

    private int updateTracker() throws SQLException {

        String sqlQuery = "INSERT INTO public.instance_tracker(patient_id, instance_id, created_by, date_created) values (? , ?, ?, ?)";
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
        return updateCount;
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, "instance",
                BatchUtil.getStringFromDate(TEIUtil.date, BatchUtil.DATEFORMAT_WITH_24HR_TIME));
    }
}
