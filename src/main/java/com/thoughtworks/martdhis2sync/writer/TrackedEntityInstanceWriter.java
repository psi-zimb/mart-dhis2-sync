package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.response.ImportSummary;
import com.thoughtworks.martdhis2sync.response.TrackedEntityResponse;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@Component
public class TrackedEntityInstanceWriter implements ItemWriter {

    @Value("${tei.uri}")
    private String teiUri;

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private SyncRepository syncRepository;

    private static final String EMPTY_STRING = "\"\"";

    private static Map<String, String> newPatientIdTEIUidMap = new LinkedHashMap<>();

    private Logger logger = LoggerFactory.getLogger(TrackedEntityInstanceWriter.class);

    @Override
    public void write(List list) {
        StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");
        list.forEach(item -> instanceApiFormat.append(item).append(","));
        instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");

        ResponseEntity<TrackedEntityResponse> responseEntity = syncRepository.sendData(teiUri, instanceApiFormat.toString());

        if (HttpStatus.OK.equals(responseEntity.getStatusCode())) {
            processResponse(responseEntity.getBody().getResponse().getImportSummaries());
        } else if (HttpStatus.CONFLICT.equals(responseEntity.getStatusCode())) {
            System.out.println("Conflict ");
        } else if (HttpStatus.INTERNAL_SERVER_ERROR.equals(responseEntity.getStatusCode())) {
            System.out.println("Internal Server Error");
        }

    }

    private void processResponse(List<ImportSummary> importSummaries) {
        Iterator<Entry<String, String>> mapIterator = TEIUtil.getPatientIdTEIUidMap().entrySet().iterator();

        importSummaries.forEach(importSummary -> {
            if (importSummary.getImportCount().getImported() == 1) {

                while (mapIterator.hasNext()) {
                    Entry<String, String> entry = mapIterator.next();
                    if (EMPTY_STRING.equals(entry.getValue())) {
                        newPatientIdTEIUidMap.put(StringUtils.replace(entry.getKey(), "\"", ""), importSummary.getReference());
                        break;
                    }
                }
            }
        });
        int recordsCreated = updateTracker();
        logger.info("Successfully inserted " + recordsCreated + " TrackedEntityInstance UIDs.");
    }

    private int updateTracker() {
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO public.instance_tracker(patient_id, instance_id) values");

        Iterator<Entry<String, String>> iterator = newPatientIdTEIUidMap.entrySet().iterator();
        iterator.forEachRemaining(entry -> query.append(String.format("('%s', '%s'),", entry.getKey(), entry.getValue())));
        query.replace(query.length() - 1, query.length(), "");

        return jdbcTemplate.update(query.toString());
    }
}
