package com.thoughtworks.martdhis2sync.service;

import com.thoughtworks.martdhis2sync.model.DataElement;
import com.thoughtworks.martdhis2sync.model.DataElementResponse;
import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.DataElementsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class DateTimeDataElementService {

    @Autowired
    private SyncRepository syncRepository;

    private List<DataElement> dateTimeDataElements = new LinkedList<>();

    public static final String URI_DATE_TIME_DATA_ELEMENTS = "/api/dataElements?pageSize=1000&filter=valueType:eq:DATETIME";

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_PREFIX = "Data Element Service: ";

    public void getDataElements() {
        String url;
        do {
            ResponseEntity<DataElementResponse> dataElementResponse = syncRepository.getDataElements("");
            if(null == dataElementResponse){
                logger.error(LOG_PREFIX + "Received empty response.");
                return;
            }
            dateTimeDataElements.addAll(dataElementResponse.getBody().getDataElements());
            url = dataElementResponse.getBody().getPager().getNextPage();
        }while (null != url);

        DataElementsUtil.setDateTimeElements(dateTimeDataElements);

        logger.info(LOG_PREFIX + "Saved " + dateTimeDataElements.size() + " Data Elements");
    }
}
