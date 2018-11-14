package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.TrackedEntityAttributeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class DateTimeTEAService {

    @Autowired
    private SyncRepository syncRepository;

    private ResponseEntity<TrackedEntityAttributeResponse> dateTimeTEAttributes;
    public static String URI_DATE_TIME_T_E_ATTRIBUTES = "/api/trackedEntityAttributes?pageSize=10&filter=valueType:eq:DATETIME";
    private List<String> trackedEntityAttributes = new LinkedList<>();
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Tracked Entity Attribute Service: ";


    public void getTEAttributes(){
        String url;
        do {
            dateTimeTEAttributes = syncRepository.getTrackedEntityAttributes("");
            if (null == dateTimeTEAttributes) {
                logger.error(LOG_PREFIX + "Received empty response.");
                return;
            }
            url = dateTimeTEAttributes.getBody().getPager().getNextPage();
            dateTimeTEAttributes.getBody().getTrackedEntityAttributes().forEach(
                    tEA -> trackedEntityAttributes.add(tEA.getId())
            );
        } while (null != url);

        TrackedEntityAttributeUtil.setDateTimeAttributes(trackedEntityAttributes);

        logger.info(LOG_PREFIX + "Saved " + trackedEntityAttributes.size() + " Tracked Entity Attributes");
    }
}
