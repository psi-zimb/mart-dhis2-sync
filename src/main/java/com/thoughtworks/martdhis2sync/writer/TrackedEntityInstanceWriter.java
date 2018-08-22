package com.thoughtworks.martdhis2sync.writer;

import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TrackedEntityInstanceWriter implements ItemWriter {

    @Override
    public void write(List list) throws Exception {
        StringBuilder instanceApiFormat = new StringBuilder("{\"trackedEntityInstances\":[");

        list.stream().forEach(item -> instanceApiFormat.append(item + ","));

        instanceApiFormat.replace(instanceApiFormat.length() - 1, instanceApiFormat.length(), "]}");
    }
}



