package com.thoughtworks.martdhis2sync.service;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.thoughtworks.martdhis2sync.dao.MappingDAO;
import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.model.MappingJson;
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
public class DHISMetaDataService {

    @Autowired
    private SyncRepository syncRepository;

    @Value("${dhis2.url}")
    private String dhis2Url;

    @Value("${country.org.unit.id}")
    private String orgUnitID;

    @Autowired
    private MappingDAO mappingDAO;

    private static final String LOG_PREFIX = "Data Element Service: ";
    private static final String URI_DATE_TIME_DATA_ELEMENTS = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";
    private static final String URI_DATE_TIME_T_E_ATTRIBUTES = "/api/trackedEntityAttributes?pageSize=1000&filter=valueType:eq:DATETIME";
    private static final String TEI_URI = "/api/trackedEntityInstances?pageSize=10000";

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

    public void getTrackedEntityInstances(String mappingName) throws IOException {
        StringBuilder uri = new StringBuilder();

        uri.append(TEI_URI);
        uri.append("&ou=");
        uri.append(orgUnitID);
        uri.append("&ouMode=DESCENDANTS");

        Map<String, Object> mapping = mappingDAO.getMapping(mappingName);

        Gson gson = new Gson();
        LinkedTreeMap instanceMapping = (LinkedTreeMap) gson.fromJson(mapping.get("mapping_json").toString(), MappingJson.class).getInstance();

        List<Map<String, Object>> searchableFields = mappingDAO.getSearchableFields(mappingName);

        searchableFields.get(0).keySet().forEach(filter -> {
            uri.append("&filter=");
            uri.append(instanceMapping.get(filter));
            uri.append(":IN:");

            searchableFields.forEach(searchableField -> {
                uri.append(searchableField.get(filter));
                uri.append(";");
            });
        });

        TEIUtil.setTrackedEntityInstances(
                syncRepository.getTrackedEntityInstances(uri.toString()).getBody().getTrackedEntityInstances()
        );
    }
}
