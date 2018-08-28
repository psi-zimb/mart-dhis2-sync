package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TrackedEntityInstanceWriter implements ItemWriter {

    @Value("${tei.uri}")
    private String teiUri;

    @Autowired
    private SyncRepository syncRepository;

    @Override
    public void write(List list) {
        StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");
        list.forEach(item -> instanceApiFormat.append(item).append(","));
        instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");

        syncRepository.sendData(teiUri, instanceApiFormat.toString());
    }
}
