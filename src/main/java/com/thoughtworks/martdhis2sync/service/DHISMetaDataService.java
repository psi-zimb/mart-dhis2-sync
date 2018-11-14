package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class DHISMetaDataService {

    @Autowired
    private SyncRepository syncRepository;

    private List<String> dateTimeDataElements = new LinkedList<>();
    private List<String> trackedEntityAttributes = new LinkedList<>();

    private static final String LOG_PREFIX = "Data Element Service: ";
    public static final String URI_DATE_TIME_DATA_ELEMENTS = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";
    public static final String URI_DATE_TIME_T_E_ATTRIBUTES = "/api/trackedEntityAttributes?pageSize=10&filter=valueType:eq:DATETIME";

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String url;

    private List<String> getTEAttributes() {
        url = "";
        do {
            ResponseEntity<TrackedEntityAttributeResponse> dateTimeTEAttributes =
                    syncRepository.getTrackedEntityAttributes("");
            if (null != dateTimeTEAttributes) {
                url = dateTimeTEAttributes.getBody().getPager().getNextPage();
                dateTimeTEAttributes.getBody().getTrackedEntityAttributes().forEach(
                        tEA -> trackedEntityAttributes.add(tEA.getId())
                );
            }
        } while (null != url);

        logger.info(LOG_PREFIX + "Received " + trackedEntityAttributes.size() + " Tracked Entity Attributes of type DateTime");
        return trackedEntityAttributes;
    }

    private List<String> getDataElements() {
        url = "";
        do {
            ResponseEntity<DataElementResponse> dataElementResponse = syncRepository.getDataElements("");
            if (null != dataElementResponse) {
                dataElementResponse.getBody().getDataElements().forEach(
                        dataElement -> dateTimeDataElements.add(dataElement.getId())
                );
                url = dataElementResponse.getBody().getPager().getNextPage();
            }
        } while (null != url);

        logger.info(LOG_PREFIX + "Received " + dateTimeDataElements.size() + " Data Elements of type DateTime");
        return dateTimeDataElements;
    }

    public void filterByTypeDateTime() {
        EventUtil.setElementsOfTypeDateTime(getDataElements());
        TEIUtil.setAttributeOfTypeDateTime(getTEAttributes());
    }
}
