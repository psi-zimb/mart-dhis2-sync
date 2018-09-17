package com.thoughtworks.martdhis2sync.step;

import com.thoughtworks.martdhis2sync.processor.ProgramEnrollmentProcessor;
import com.thoughtworks.martdhis2sync.reader.MappingReader;
import com.thoughtworks.martdhis2sync.util.EnrollmentUtil;
import com.thoughtworks.martdhis2sync.writer.ProgramEnrollmentWriter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProgramEnrollmentStep {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private MappingReader mappingReader;

    @Autowired
    private ProgramEnrollmentProcessor processor;

    @Autowired
    private ProgramEnrollmentWriter writer;

    public Step get(String lookupTable) {
        EnrollmentUtil.resetEnrollmentsList();
        return stepBuilderFactory.get("ProgramEnrollmentStep")
                .chunk(500)
                .reader(mappingReader.getEnrollmentReader(lookupTable))
                .processor(processor)
                .writer(writer)
                .build();
    }
}
