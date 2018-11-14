package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.TrackedEntityAttributeResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.TEIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class DHISMetaDataService {

    @Autowired
    private SyncRepository syncRepository;

    @Value("${dhis2.url}")
    private String dhis2Url;

    private static final String LOG_PREFIX = "Data Element Service: ";
    private static final String URI_DATE_TIME_DATA_ELEMENTS = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";
    private static final String URI_DATE_TIME_T_E_ATTRIBUTES = "/api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME";

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private String url;

    private List<String> getTEAttributes() {
        List<String> trackedEntityAttributes = new LinkedList<>();
        url = dhis2Url + URI_DATE_TIME_T_E_ATTRIBUTES;
        do {
            ResponseEntity<TrackedEntityAttributeResponse> dateTimeTEAttributes =
                    syncRepository.getTrackedEntityAttributes(url);
            if (null == dateTimeTEAttributes) {
                break;
            }
            url = dateTimeTEAttributes.getBody().getPager().getNextPage();
            dateTimeTEAttributes.getBody().getTrackedEntityAttributes()
                    .forEach(tEA -> trackedEntityAttributes.add(tEA.getId()));
        } while (null != url);

        logger.info(LOG_PREFIX + "Received " + trackedEntityAttributes.size() + " Tracked Entity Attributes of type DateTime");
        return trackedEntityAttributes;
    }

    private List<String> getDataElements() {
        List<String> dateTimeDataElements = new LinkedList<>();
        url = dhis2Url + URI_DATE_TIME_DATA_ELEMENTS;
        do {
            ResponseEntity<DataElementResponse> dataElementResponse = syncRepository.getDataElements(url);
            if (null == dataElementResponse) {
                break;
            }
            dataElementResponse.getBody().getDataElements()
                    .forEach(dataElement -> dateTimeDataElements.add(dataElement.getId()));
            url = dataElementResponse.getBody().getPager().getNextPage();
        } while (null != url);

        logger.info(LOG_PREFIX + "Received " + dateTimeDataElements.size() + " Data Elements of type DateTime");
        return dateTimeDataElements;
    }

    public void filterByTypeDateTime() {
        EventUtil.setElementsOfTypeDateTime(getDataElements());
        TEIUtil.setAttributeOfTypeDateTime(getTEAttributes());
    }
}
