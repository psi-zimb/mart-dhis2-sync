package com.thoughtworks.martdhis2sync.writer;

import com.thoughtworks.martdhis2sync.repository.SyncRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProgramEnrollmentWriter implements ItemWriter {

    @Autowired
    private SyncRepository syncRepository;

    @Value("${uri.program.enrollments}")
    private String programEnrollUri;


    @Override
    public void write(List items) {
        StringBuilder enrollmentApiFormat = new StringBuilder("{\"enrollments\":[");
        items.forEach(item -> enrollmentApiFormat.append(item).append(","));
        enrollmentApiFormat.replace(enrollmentApiFormat.length() - 1, enrollmentApiFormat.length(), "]}");

        syncRepository.sendData(programEnrollUri, enrollmentApiFormat.toString());
    }
}
