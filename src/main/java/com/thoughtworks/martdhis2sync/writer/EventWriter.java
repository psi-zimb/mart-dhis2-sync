package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EventWriter implements ItemWriter {

    @Value("${uri.event}")
    private String eventUri;

    @Autowired
    private SyncRepository syncRepository;

    @Override
    public void write(List list) {
        StringBuilder eventApi = new StringBuilder("{\"events\":[");
        list.forEach(item -> eventApi.append(item).append(","));
        int length = eventApi.length();
        eventApi.replace(length - 1, length, "]}");

       syncRepository.sendData(eventUri, eventApi.toString());
    }
}
