package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import com.thoughtworks.martdhis2sync.util.EventUtil;
import com.thoughtworks.martdhis2sync.util.MarkerUtil;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.thoughtworks.martdhis2sync.util.BatchUtil.DATEFORMAT_WITH_24HR_TIME;
import static com.thoughtworks.martdhis2sync.util.BatchUtil.getStringFromDate;
import static com.thoughtworks.martdhis2sync.util.MarkerUtil.CATEGORY_EVENT;

@Component
@StepScope
public class EventWriter implements ItemWriter {

    private static final String URI = "/api/events?strategy=CREATE_AND_UPDATE";

    @Value("#{jobParameters['service']}")
    private String programName;

    @Autowired
    private SyncRepository syncRepository;

    @Autowired
    private MarkerUtil markerUtil;

    @Override
    public void write(List list) {
        StringBuilder eventApi = new StringBuilder("{\"events\":[");
        list.forEach(item -> eventApi.append(item).append(","));
        int length = eventApi.length();
        eventApi.replace(length - 1, length, "]}");

        syncRepository.sendData(URI, eventApi.toString());
        updateMarker();
    }

    private void updateMarker() {
        markerUtil.updateMarkerEntry(programName, CATEGORY_EVENT,
                getStringFromDate(EventUtil.date, DATEFORMAT_WITH_24HR_TIME));
    }
}
