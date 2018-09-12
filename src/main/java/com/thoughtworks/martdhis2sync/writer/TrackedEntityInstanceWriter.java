package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.response.ImportSummary;
import com.thoughtworks.martdhis2sync.response.TrackedEntityResponse;
import com.thoughtworks.martdhis2sync.util.BatchUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

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

import static com.thoughtworks.martdhis2sync.response.ImportSummary.RESPONSE_SUCCESS;

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
    public void write(List list) {
        StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");
        list.forEach(item -> instanceApiFormat.append(item).append(","));
        instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");

        ResponseEntity<TrackedEntityResponse> responseEntity = syncRepository.sendData(teiUri, instanceApiFormat.toString());

        if (null == responseEntity) {
            return;
        }
        logger.info(LOG_PREFIX + "Received " + responseEntity.getStatusCode() + " status code.");
        IS_SYNC_SUCCESS = true;
        processResponse(responseEntity.getBody().getResponse().getImportSummaries());
        if(IS_SYNC_SUCCESS) {
            updateMarker();
        }
    }

    private void processResponse(List<ImportSummary> importSummaries) {
        newTEIUIDs.clear();
        Iterator<Entry<String, String>> mapIterator = TEIUtil.getPatientIdTEIUidMap().entrySet().iterator();

        importSummaries.forEach(importSummary -> {
            if (RESPONSE_SUCCESS.equals(importSummary.getStatus()) && importSummary.getImportCount().getImported() == 1) {

                while (mapIterator.hasNext()) {
                    Entry<String, String> entry = mapIterator.next();
                    if (EMPTY_STRING.equals(entry.getValue())) {
                        newTEIUIDs.put(StringUtils
                                .replace(entry.getKey(), "\"", ""), importSummary.getReference());
                        break;
                    }
                }
            } else if (RESPONSE_SUCCESS.equals(importSummary.getStatus()) && !importSummary.getConflicts().isEmpty()) {
                IS_SYNC_SUCCESS = false;
                importSummary.getConflicts().forEach(conflict -> logger.error(LOG_PREFIX + "" + conflict.getValue()));
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
                    ps.setTimestamp(4, Timestamp.valueOf(BatchUtil.GetUTCdatetimeAsString()));
                    updateCount += ps.executeUpdate();
                }
            }
        }
        return updateCount;
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, "instance");
    }
}
